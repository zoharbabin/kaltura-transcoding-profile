#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check-transcode.py

Inspect a Kaltura entry’s conversion profile and rendition (“flavor”) ladder.

Features
- Clear report with both simple (explanatory) and expert details
- Correct enum label↔code mapping (per values provided)
- Distinguishes Uploaded Source (paramId=0) vs Transcoded Flavors
- Per-flavor reasons inline (ERROR/NOT_APPLICABLE), with error descriptions
- Conversion Profile analysis: enabled params, missing/not-seen, above-source targets, near-duplicates
- Visual bitrate ladder + ABR switching guidance
- Robust duration/datetime formatting
- Safe, defensive code paths and graceful fallbacks

Usage
  python check-transcode.py \
    --partner-id <PID> \
    --admin-secret '***' \
    --entry-id '<ENTRY_ID>' \
    --admin-user-id '<ADMIN_USER>' \
    --service-url 'https://www.kaltura.com/' \
    [--include-urls]
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
import sys

# ──────────────────────────────────────────────────────────────────────────────
# Kaltura SDK
# ──────────────────────────────────────────────────────────────────────────────

try:
    from KalturaClient import KalturaClient, KalturaConfiguration
    from KalturaClient.Plugins.Core import (  # type: ignore
        KalturaSessionType,
        KalturaBaseEntry,
        KalturaFlavorAsset,
        KalturaFlavorAssetFilter,
        KalturaFilterPager,
        KalturaConversionProfile,
        KalturaFlavorParams,
        KalturaConversionProfileAssetParamsFilter,
    )
except Exception:
    print("❌ KalturaClient not found. Install with: pip install KalturaApiClient", file=sys.stderr)
    raise

# ──────────────────────────────────────────────────────────────────────────────
# Verified enum maps (verbatim from values provided)
# ──────────────────────────────────────────────────────────────────────────────

ENTRY_STATUS_MAP: Dict[Any, str] = {
    -2: "ERROR_IMPORTING",
    -1: "ERROR_CONVERTING",
    "virusScan.ScanFailure": "SCAN_FAILURE",
    0: "IMPORT",
    "virusScan.Infected": "INFECTED",
    1: "PRECONVERT",
    2: "READY",
    3: "DELETED",
    4: "PENDING",
    5: "MODERATE",
    6: "BLOCKED",
    7: "NO_CONTENT",
}
ENTRY_TYPE_MAP: Dict[Any, str] = {
    -1: "AUTOMATIC",
    "conference.CONFERENCE_ENTRY_SERVER": "CONFERENCE_ENTRY_SERVER",
    "externalMedia.externalMedia": "EXTERNAL_MEDIA",
    "sip.SIP_ENTRY_SERVER": "SIP_ENTRY_SERVER",
    1: "MEDIA_CLIP",
    2: "MIX",
    5: "PLAYLIST",
    6: "DATA",
    7: "LIVE_STREAM",
    8: "LIVE_CHANNEL",
    10: "DOCUMENT",
}
SOURCE_TYPE_MAP: Dict[Any, str] = {
    "limeLight.LIVE_STREAM": "LIMELIGHT_LIVE",
    "velocix.VELOCIX_LIVE": "VELOCIX_LIVE",
    1: "FILE",
    2: "WEBCAM",
    5: "URL",
    6: "SEARCH_PROVIDER",
    29: "AKAMAI_LIVE",
    30: "MANUAL_LIVE_STREAM",
    31: "AKAMAI_UNIVERSAL_LIVE",
    32: "LIVE_STREAM",
    33: "LIVE_CHANNEL",
    34: "RECORDED_LIVE",
    35: "CLIP",
    36: "KALTURA_RECORDED_LIVE",
    37: "LECTURE_CAPTURE",
    42: "LIVE_STREAM_ONTEXTDATA_CAPTIONS",
}
CP_TYPE_MAP: Dict[Any, str] = {1: "MEDIA", 2: "LIVE_STREAM"}
CP_STATUS_MAP: Dict[Any, str] = {1: "DISABLED", 2: "ENABLED", 3: "DELETED"}
FLAVOR_STATUS_MAP: Dict[Any, str] = {
    -1: "ERROR",
    0: "QUEUED",
    1: "CONVERTING",
    2: "READY",
    3: "DELETED",
    4: "NOT_APPLICABLE",
    5: "TEMP",
    6: "WAIT_FOR_CONVERT",
    7: "IMPORTING",
    8: "VALIDATING",
    9: "EXPORTING",
}

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────

def get_attr_any(obj: Any, *names: str) -> Any:
    """
    Get the first non-empty attribute from an object.
    
    Args:
        obj: Object to get attribute from
        *names: Attribute names to try, in order
        
    Returns:
        First non-empty attribute value, or None if all are empty
    """
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v not in (None, "", [], {}):
                return v
    return None

def _unwrap_enum_value(v: Any) -> Any:
    """
    Extract the underlying value from a Kaltura enum object.
    
    Args:
        v: Value to unwrap (may be int, str, or enum object)
        
    Returns:
        Unwrapped value (int or str)
    """
    if isinstance(v, (int, str)):
        return v
    for attr in ("value", "val", "code"):
        if hasattr(v, attr):
            raw = getattr(v, attr)
            if isinstance(raw, (int, str)):
                return raw
    return v

def enum_label_code(value: Any, kind: str) -> Tuple[str, Any]:
    """
    Map enum value to human-readable label and code.
    
    Args:
        value: Enum value to map
        kind: Type of enum (e.g., "entry_status", "flavor_status")
        
    Returns:
        Tuple of (label, code), or ("UNKNOWN", raw_value) if not found
    """
    tables = {
        "entry_status": ENTRY_STATUS_MAP,
        "entry_type": ENTRY_TYPE_MAP,
        "source_type": SOURCE_TYPE_MAP,
        "cp_type": CP_TYPE_MAP,
        "cp_status": CP_STATUS_MAP,
        "flavor_status": FLAVOR_STATUS_MAP,
    }
    table = tables.get(kind, {})
    raw = _unwrap_enum_value(value)
    if raw in table:
        return table[raw], raw
    # Try int cast, then string
    try:
        as_int = int(raw)
        if as_int in table:
            return table[as_int], as_int
    except Exception:
        pass
    as_str = str(raw)
    if as_str in table:
        return table[as_str], as_str
    return "UNKNOWN", raw

def safe_int(x: Any, default: Optional[int] = 0) -> Optional[int]:
    """
    Safely convert value to int with a default.
    
    Args:
        x: Value to convert
        default: Default value if conversion fails (can be None)
        
    Returns:
        Converted int or default value
    """
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default

def safe_float(x: Any, default: Optional[float] = 0.0) -> Optional[float]:
    """
    Safely convert value to float with a default.
    
    Args:
        x: Value to convert
        default: Default value if conversion fails (can be None)
        
    Returns:
        Converted float or default value
    """
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def fmt_dt(ts: Any) -> str:
    """Format Unix timestamp as human-readable datetime string."""
    try:
        t = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return t.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(ts)

def fmt_duration_ms(ms: Optional[int]) -> str:
    """Format duration in milliseconds as human-readable string (e.g., '1h 23m 45.678s')."""
    if ms is None:
        return "unknown"
    total_s = ms / 1000.0
    m, s = divmod(total_s, 60)
    h, m = divmod(m, 60)
    if h >= 1:
        return f"{int(h)}h {int(m)}m {s:.3f}s"
    return f"{int(m)}m {s:.3f}s"

# ──────────────────────────────────────────────────────────────────────────────
# Models
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FlavorClassified:
    asset_id: str
    flavor_params_id: Optional[int]
    status_label: str
    status_code: Any
    kind: str  # SOURCE | TRANSCODED | SKIPPED | ERROR | PENDING | DELETED
    reason: Optional[str]
    is_original: bool
    width: Optional[int]
    height: Optional[int]
    fps: Optional[float]
    bitrate_kbps: Optional[int]
    vcodec: Optional[str]
    acodec: Optional[str]
    tags: Optional[str]
    download_url: Optional[str]

# ──────────────────────────────────────────────────────────────────────────────
# Analysis helpers
# ──────────────────────────────────────────────────────────────────────────────

def derive_vcodec_from_tags(tags: Optional[str]) -> Optional[str]:
    """
    Derive video codec from flavor asset tags.
    
    Args:
        tags: Comma-separated tags string from flavor asset
        
    Returns:
        Codec identifier (e.g., 'hvc1', 'avc1', 'av01') or None
    """
    if not tags:
        return None
    t = tags.lower()
    # Accept common spellings/aliases
    if any(k in t for k in ("h265", "hevc", "hvc1", "hev1")):
        return "hvc1"
    if any(k in t for k in ("h264", "avc1", "avc")):
        return "avc1"
    if any(k in t for k in ("av01", "av1")):
        return "av01"
    if any(k in t for k in ("vp09", "vp9")):
        return "vp09"
    if "vp8" in t:
        return "vp8"
    return None

def codec_baseline_label(vcodec: Optional[str]) -> str:
    """Get human-readable codec baseline label."""
    vc = (vcodec or "").lower()
    if "hvc" in vc or "hev" in vc or "265" in vc:
        return "HEVC baseline"
    if "av01" in vc or "av1" in vc:
        return "AV1 baseline"
    if "vp09" in vc or "vp9" in vc:
        return "VP9 baseline"
    if "vp8" in vc:
        return "VP8 baseline"
    return "H.264 baseline"  # default / unknown

def expected_kbps(vcodec: Optional[str], width: int, height: int) -> int:
    """Simple heuristic by resolution & codec."""
    h = height or 0
    vc = (vcodec or "").lower()
    # HEVC
    if "hvc" in vc or "hev" in vc or "265" in vc:
        if h <= 360: return 700
        if h <= 480: return 1200
        if h <= 560: return 1500
        if h <= 800: return 2100
        if h <= 1100: return 3200
        return 4200
    # AV1
    if "av01" in vc or "av1" in vc:
        if h <= 360: return 600
        if h <= 480: return 1000
        if h <= 560: return 1200
        if h <= 800: return 1800
        if h <= 1100: return 2800
        return 3800
    # AVC / VP8 / unknown (conservative)
    if h <= 360: return 1000
    if h <= 480: return 1800
    if h <= 560: return 1800
    if h <= 800: return 2500
    if h <= 1100: return 4000
    return 5000

def bpp_per_second(
    kbps: Optional[int],
    width: Optional[int],
    height: Optional[int],
    fps: Optional[float]
) -> Optional[float]:
    """Calculate bits per pixel per second."""
    if not (kbps and width and height and fps) or width <= 0 or height <= 0 or fps <= 0:
        return None
    return (kbps * 1000) / (width * height * fps)

def low_high_for_res(
    vcodec: Optional[str],
    width: Optional[int],
    height: Optional[int],
    fps: Optional[float],
    kbps: Optional[int]
) -> Tuple[bool, bool]:
    """
    Check if bitrate is likely too low or too high for resolution and codec.
    
    Returns:
        Tuple of (likely_low, likely_high) booleans
    """
    bpp = bpp_per_second(kbps, width, height, fps)
    if bpp is None:
        return False, False
    vc = (vcodec or "").lower()
    if "hvc" in vc or "hev" in vc or "265" in vc:
        low_thr, high_thr = 0.055, 0.32
    elif "av01" in vc or "av1" in vc:
        low_thr, high_thr = 0.045, 0.28
    elif "vp8" in vc:
        low_thr, high_thr = 0.085, 0.45
    else:  # AVC/VP9/unknown (conservative AVC-ish)
        low_thr, high_thr = 0.075, 0.40
    return bpp < low_thr, bpp > high_thr

def efficiency_flags(width: Optional[int], height: Optional[int]) -> List[str]:
    """
    Check for encoding efficiency issues in dimensions.
    
    Returns:
        List of flag strings (e.g., ['non_mod16', 'odd_aspect'])
    """
    flags: List[str] = []
    if width and height:
        if width % 16 != 0 or height % 16 != 0:
            flags.append("non_mod16")
        # Aspect sanity
        ar = width / float(height)
        targets = [(1, 1), (4, 3), (16, 9), (21, 9)]
        closest = min(targets, key=lambda r: abs(ar - (r[0] / r[1])))
        if abs(ar - (closest[0] / closest[1])) > 0.05:
            flags.append("odd_aspect")
    return flags

# ──────────────────────────────────────────────────────────────────────────────
# Fetchers
# ──────────────────────────────────────────────────────────────────────────────

def fetch_entry(client: KalturaClient, entry_id: str) -> KalturaBaseEntry:
    """
    Fetch a Kaltura entry by ID.
    
    Args:
        client: Authenticated Kaltura client
        entry_id: Entry ID to fetch
        
    Returns:
        KalturaBaseEntry object
        
    Raises:
        Exception: If entry cannot be fetched (e.g., not found, permission denied)
    """
    return client.baseEntry.get(entry_id)  # type: ignore

def fetch_flavor_assets(client: KalturaClient, entry_id: str) -> List[KalturaFlavorAsset]:
    """
    Fetch all flavor assets for an entry with full pagination support.
    
    Args:
        client: Authenticated Kaltura client
        entry_id: Entry ID to fetch flavors for
        
    Returns:
        List of all KalturaFlavorAsset objects for the entry
    """
    f = KalturaFlavorAssetFilter()
    f.entryIdEqual = entry_id
    page_size = 500
    page_index = 1
    all_assets: List[KalturaFlavorAsset] = []
    
    while True:
        pager = KalturaFilterPager(pageSize=page_size, pageIndex=page_index)
        res = client.flavorAsset.list(f, pager)  # type: ignore
        objects = list(res.objects or [])
        all_assets.extend(objects)
        
        # Break if we received fewer results than the page size
        if len(objects) < page_size:
            break
        page_index += 1
    
    return all_assets

def fetch_conversion_profile(client: KalturaClient, cp_id: int) -> Optional[KalturaConversionProfile]:
    """
    Fetch a conversion profile by ID.
    
    Args:
        client: Authenticated Kaltura client
        cp_id: Conversion profile ID
        
    Returns:
        KalturaConversionProfile object or None if not found or ID is invalid
    """
    if cp_id <= 0:
        return None
    try:
        return client.conversionProfile.get(cp_id)  # type: ignore
    except Exception:
        return None

def _parse_csv_ints(csv_str: str) -> List[int]:
    """Parse comma-separated string of integers."""
    out: List[int] = []
    for tok in str(csv_str).split(","):
        tok = tok.strip()
        if tok:
            try:
                out.append(int(tok))
            except Exception:
                pass
    return out

def fetch_enabled_flavor_param_ids(client: KalturaClient, cp: Optional[KalturaConversionProfile]) -> List[int]:
    """
    Return Flavor Params IDs enabled on the Conversion Profile.
    Strategy:
      1) Use cp.flavorParamsIds (common) or cp.profileParamsIds (older) if present (CSV).
      2) Fallback to conversionProfileAssetParams.list with full pagination (collect flavorParamsId).
    """
    ids: List[int] = []
    if cp:
        raw = get_attr_any(cp, "flavorParamsIds", "profileParamsIds", "flavor_params_ids", "profile_params_ids")
        if raw:
            ids = _parse_csv_ints(str(raw))
            if ids:
                return sorted(set(ids))

        # Fallback: list ConversionProfileAssetParams with full pagination
        try:
            flt = KalturaConversionProfileAssetParamsFilter()
            flt.conversionProfileIdEqual = int(getattr(cp, "id", 0))
            page_size = 500
            page_index = 1
            
            while True:
                pager = KalturaFilterPager(pageSize=page_size, pageIndex=page_index)
                res = client.conversionProfileAssetParams.list(flt, pager)  # type: ignore
                objects = list(getattr(res, "objects", []) or [])
                
                for obj in objects:
                    pid = getattr(obj, "flavorParamsId", None)
                    if isinstance(pid, int):
                        ids.append(pid)
                
                # Break if we received fewer results than the page size
                if len(objects) < page_size:
                    break
                page_index += 1
        except Exception:
            pass
    return sorted(set(ids))

def fetch_flavor_params_by_ids(client: KalturaClient, ids: Iterable[int]) -> Dict[int, KalturaFlavorParams]:
    """
    Fetch flavor parameters by IDs.
    
    Note: This uses a loop because the Kaltura client lacks a batch-get method
    for flavor params. Each ID requires a separate API call.
    
    Args:
        client: Authenticated Kaltura client
        ids: Iterable of flavor param IDs to fetch
        
    Returns:
        Dictionary mapping flavor param ID to KalturaFlavorParams object
    """
    out: Dict[int, KalturaFlavorParams] = {}
    for pid in sorted(set(int(i) for i in ids if i != 0)):
        try:
            out[pid] = client.flavorParams.get(pid)  # type: ignore
        except Exception:
            pass
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Classification
# ──────────────────────────────────────────────────────────────────────────────

def classify_flavor(client: KalturaClient, fa: KalturaFlavorAsset, include_urls: bool) -> FlavorClassified:
    """
    Classify a flavor asset with normalized type handling.
    
    Args:
        client: Authenticated Kaltura client
        fa: Flavor asset to classify
        include_urls: Whether to include download URLs
        
    Returns:
        FlavorClassified object with normalized data
    """
    status_label, status_code = enum_label_code(getattr(fa, "status", None), "flavor_status")

    is_original = bool(getattr(fa, "isOriginal", False))
    # Normalize flavorParamsId to int using safe_int
    flavor_params_id = safe_int(getattr(fa, "flavorParamsId", None), None)
    reason = (getattr(fa, "description", None) or "").strip() or None

    # Kind
    if is_original or flavor_params_id == 0:
        kind = "SOURCE"
    elif status_label == "READY":
        kind = "TRANSCODED"
    elif status_label == "NOT_APPLICABLE":
        kind = "SKIPPED"
    elif status_label == "ERROR":
        kind = "ERROR"
    elif status_label in {"QUEUED", "CONVERTING", "WAIT_FOR_CONVERT", "IMPORTING", "VALIDATING", "EXPORTING"}:
        kind = "PENDING"
    elif status_label == "DELETED":
        kind = "DELETED"
    else:
        kind = "PENDING"

    # Codecs (explicit > tags > unknown)
    vcodec = get_attr_any(fa, "videoCodec", "video_codec", "codec", "videoCodecId")
    if not vcodec:
        vcodec = derive_vcodec_from_tags(getattr(fa, "tags", None))
    acodec = get_attr_any(fa, "audioCodec", "audio_codec", "audioCodecId")

    # URL
    url: Optional[str] = None
    if include_urls:
        try:
            url = client.flavorAsset.getDownloadUrl(fa.id)  # type: ignore
        except Exception:
            try:
                url = client.flavorAsset.getUrl(fa.id)  # type: ignore
            except Exception:
                url = None

    # Normalize all numeric fields
    width = safe_int(getattr(fa, "width", None), None)
    height = safe_int(getattr(fa, "height", None), None)
    fps = safe_float(getattr(fa, "frameRate", None) or getattr(fa, "frame_rate", None), None)
    bitrate_kbps = safe_int(
        getattr(fa, "bitrate", None)
        or getattr(fa, "bitrateKbps", None)
        or getattr(fa, "bitrateInKbps", None),
        None
    )

    return FlavorClassified(
        asset_id=getattr(fa, "id", ""),
        flavor_params_id=flavor_params_id,
        status_label=status_label,
        status_code=status_code,
        kind=kind,
        reason=reason,
        is_original=is_original,
        width=width,
        height=height,
        fps=fps,
        bitrate_kbps=bitrate_kbps,
        vcodec=vcodec,
        acodec=acodec,
        tags=getattr(fa, "tags", None),
        download_url=url,
    )

def extract_duration_ms(entry: Any, assets: List[KalturaFlavorAsset]) -> Optional[int]:
    """
    Extract duration in milliseconds from entry.
    
    Kaltura duration may be seconds or ms depending on context.
    Heuristics:
      - If >= 10000 -> ms.
      - Else assume seconds when media tracks exist.
    """
    raw = getattr(entry, "duration", None)
    if raw is None:
        return None
    try:
        val = int(raw)
    except Exception:
        return None

    if val >= 10000:
        return val  # ms
    # If there are real media flavors, treat as seconds:
    has_media = any(safe_float(getattr(fa, "frameRate", 0.0), 0.0) > 0 for fa in assets)
    return val * 1000 if has_media else val

# ──────────────────────────────────────────────────────────────────────────────
# Printing helpers
# ──────────────────────────────────────────────────────────────────────────────

def indent(lines: Iterable[str], n: int = 2) -> str:
    """Indent each line by n spaces."""
    pad = " " * n
    return "\n".join(pad + line if line else "" for line in lines)

def render_bitrate_bars(rungs: List[Tuple[str, int]], width: int = 54) -> List[str]:
    """Render visual bitrate bars for ladder rungs."""
    if not rungs:
        return []
    max_br = max((k for _, k in rungs), default=1)
    lines: List[str] = []
    prev: Optional[int] = None
    for label, kbps in rungs:
        bar_len = max(1, int((kbps / max_br) * width))
        ratio = f"   (x{kbps / prev:.2f})" if (prev and prev > 0) else ""
        lines.append(
            f"  {'█' * bar_len:<{width}}  {label:>12}  {kbps:>5} kbps{ratio}"
        )
        prev = kbps
    return lines

def switching_notes(sorted_bitrates: List[int]) -> List[str]:
    """Generate ABR switching guidance notes for bitrate ladder."""
    notes: List[str] = []
    for a, b in zip(sorted_bitrates, sorted_bitrates[1:]):
        if a <= 0 or b <= 0:
            continue
        r = b / a
        if r < 1.2:
            notes.append(
                f"• Tiny step: {a}→{b} (x{r:.2f}) — "
                "frequent switching, little quality gain."
            )
        elif r > 2.5:
            notes.append(
                f"• Large step: {a}→{b} (x{r:.2f}) — "
                "insert a mid rung to avoid a quality cliff."
            )
    return notes

# ──────────────────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────────────────

def print_overview(entry: Any, duration_ms: Optional[int]) -> None:
    """Print entry overview section."""
    etype, etype_code = enum_label_code(getattr(entry, "type", None), "entry_type")
    estatus, estatus_code = enum_label_code(getattr(entry, "status", None), "entry_status")
    stype, stype_code = enum_label_code(getattr(entry, "sourceType", None), "source_type")
    name = getattr(entry, "name", "") or getattr(entry, "title", "")
    # Build a reliable source-download URL (paramId=0)
    partner_id = getattr(entry, "partnerId", "") or getattr(entry, "partner_id", "")
    source_url = (
        f"https://cdnapisec.kaltura.com/p/{partner_id}/sp/{partner_id}00/"
        f"playManifest/entryId/{entry.id}/format/download/protocol/https/"
        f"flavorParamIds/0"
    )
    print("📌 Overview")
    print(
        indent(
            [
                f"Entry:   {entry.id} — {name}",
                f"Type:    {etype} ({etype_code})    Status: {estatus} ({estatus_code})",
                f"User:    {getattr(entry, 'userId', '')}  Duration: {fmt_duration_ms(duration_ms)}",
                f"Source:  {stype} ({stype_code})  Download: {source_url}",
                f"Created: {fmt_dt(getattr(entry, 'createdAt', None))}  "
                f"Updated: {fmt_dt(getattr(entry, 'updatedAt', None))}",
            ]
        )
    )
    print()

def print_conversion_profile(
    cp: Optional[KalturaConversionProfile],
    enabled_ids: List[int],
    classified: List[FlavorClassified],
    source_dims: Tuple[Optional[int], Optional[int]],
    params_by_id: Dict[int, KalturaFlavorParams],
) -> None:
    """Print conversion profile analysis section."""
    print("🧩 Conversion Profile")
    if not cp:
        print(indent(["(No conversion profile associated with this entry)"]))
        print()
        return

    ctype, ctype_code = enum_label_code(getattr(cp, "type", None), "cp_type")
    cstatus, cstatus_code = enum_label_code(getattr(cp, "status", None), "cp_status")

    enabled_txt = ",".join(str(i) for i in enabled_ids) if enabled_ids else "(not available)"
    print(
        indent(
            [
                f"Name: {getattr(cp, 'name', '')}   ID: {getattr(cp, 'id', '')}",
                f"Type: {ctype} ({ctype_code})   Status: {cstatus} ({cstatus_code})",
                f"Profile Enabled Flavor Params: {enabled_txt}",
                "",
            ]
        )
    )

    # Totals
    transcoded = sum(1 for c in classified if c.kind == "TRANSCODED")
    skipped = sum(1 for c in classified if c.kind == "SKIPPED")
    errors = sum(1 for c in classified if c.kind == "ERROR")
    pending = sum(1 for c in classified if c.kind == "PENDING")
    deleted = sum(1 for c in classified if c.kind == "DELETED")

    # Missing (configured on profile but not observed on this entry at all)
    observed_param_ids = {c.flavor_params_id for c in classified if c.flavor_params_id is not None}
    missing_params = [pid for pid in enabled_ids if pid != 0 and pid not in observed_param_ids]

    # Counts line
    enabled_count = len([i for i in enabled_ids if i != 0]) if enabled_ids else "unknown"

    print(
        indent(
            [
                "Simple:",
                f" • Profile Enabled Flavors: {enabled_count}",
                f" • Transcoded Flavors:      {transcoded}",
                f" • Skipped Flavors:         {skipped}   (NOT_APPLICABLE — Optimization skipped where source < target/policy)",
                f" • Flavors with Errors:     {errors}",
                f" • Pending:                 {pending}",
                f" • Deleted:                 {deleted}",
                (
                    f" • Missing Params:          {len(missing_params)}  {missing_params}"
                    if enabled_ids
                    else " • Missing Params:          unknown"
                ),
            ]
        )
    )

    # Expert analysis
    src_w, src_h = source_dims

    def _above_source_ids() -> List[int]:
        ids: List[int] = []
        for pid in enabled_ids:
            if pid == 0:
                continue
            fp = params_by_id.get(pid)
            if not fp:
                continue
            tw, th = safe_int(get_attr_any(fp, "width"), 0), safe_int(get_attr_any(fp, "height"), 0)
            if src_w and src_h and tw and th and (tw > src_w or th > src_h):
                ids.append(pid)
        return ids

    def _near_dupes() -> List[Tuple[int, int]]:
        ids = [i for i in enabled_ids if i != 0]
        pairs: List[Tuple[int, int]] = []
        for i, a in enumerate(ids):
            pa = params_by_id.get(a)
            aw, ah = safe_int(get_attr_any(pa, "width"), 0), safe_int(get_attr_any(pa, "height"), 0)
            if not (aw and ah):
                continue
            for b in ids[i + 1 :]:
                pb = params_by_id.get(b)
                bw, bh = safe_int(get_attr_any(pb, "width"), 0), safe_int(get_attr_any(pb, "height"), 0)
                if not (bw and bh):
                    continue
                if abs(aw - bw) / max(1, aw) <= 0.03 and abs(ah - bh) / max(1, ah) <= 0.03:
                    pairs.append((a, b))
        return pairs

    above_ids = _above_source_ids()
    near_dupes = _near_dupes()
    near_dupe_txt = ", ".join(f"{a}-{b}" for a, b in near_dupes) if near_dupes else "None"

    print(
        indent(
            [
                "",
                "Expert:",
                f" • Above-source param IDs (height/width > source): [{', '.join(map(str, above_ids))}]",
                "   These are commonly skipped as NOT_APPLICABLE when Optimization is enabled; no action is required unless you want to force upscaling.",
                f" • Near-duplicate targets: {near_dupe_txt}",
                " • “Uploaded Source” (paramId=0) is the uploaded file; it is not transcoded.",
            ]
        )
    )
    print()

def print_summary(classified: List[FlavorClassified]) -> None:
    """Print summary statistics section."""
    shown = [
        c for c in classified
        if c.kind in {"TRANSCODED", "SOURCE"} and (c.bitrate_kbps or 0) > 0
    ]
    print("📊 Summary")
    if not shown:
        print(indent(["No playable rungs found."]))
        print()
        return

    min_br = min(int(c.bitrate_kbps or 0) for c in shown)
    max_br = max(int(c.bitrate_kbps or 0) for c in shown)
    min_w = min(int(c.width or 0) for c in shown)
    min_h = min(int(c.height or 0) for c in shown)
    max_w = max(int(c.width or 0) for c in shown)
    max_h = max(int(c.height or 0) for c in shown)

    # Codec counts (normalized)
    codec_counts: Dict[str, int] = {}
    for c in shown:
        key = (c.vcodec or "unknown").lower()
        codec_counts[key] = codec_counts.get(key, 0) + 1

    types = {
        "Transcoded Flavor": sum(1 for c in shown if c.kind == "TRANSCODED"),
        "Uploaded Source": sum(1 for c in shown if c.kind == "SOURCE"),
    }

    likely_low = sum(1 for c in shown if low_high_for_res(c.vcodec, c.width, c.height, c.fps, c.bitrate_kbps)[0])
    likely_high = sum(1 for c in shown if low_high_for_res(c.vcodec, c.width, c.height, c.fps, c.bitrate_kbps)[1])

    print(
        indent(
            [
                f"Flavors: {len(shown)}   Bitrate: {min_br} → {max_br} kbps",
                f"Res:     {min_w}x{min_h} → {max_w}x{max_h}",
                f"Types:   Transcoded Flavor:{types['Transcoded Flavor']}, Uploaded Source:{types['Uploaded Source']}",
                "Codecs:  " + (", ".join(f"{k}:{v}" for k, v in codec_counts.items()) if codec_counts else "(unknown)"),
                f"Bitrate possibly low for resolution: {likely_low}   Possibly higher than needed: {likely_high}",
            ]
        )
    )
    print()

def print_visual_ladder(classified: List[FlavorClassified]) -> None:
    """Print visual bitrate ladder with bars."""
    rungs = sorted(
        [
            (c.asset_id, int(c.bitrate_kbps))
            for c in classified
            if c.kind in {"TRANSCODED", "SOURCE"} and (c.bitrate_kbps or 0) > 0
        ],
        key=lambda x: x[1],
    )
    if not rungs:
        return
    print("📈 Visual Ladder (bitrate bars)")
    for line in render_bitrate_bars(rungs):
        print(line)
    notes = switching_notes([kb for _, kb in rungs])
    if notes:
        print(
            "  Guidance: Aim for ~1.3–1.6× bitrate increase between "
            "adjacent rungs to stabilize ABR switching."
        )
        print("  Switching Notes:")
        for n in notes:
            print("   " + n)
    print()

def print_skipped(classified: List[FlavorClassified]) -> None:
    """Print skipped flavors section, grouped by reason."""
    skipped = [c for c in classified if c.kind == "SKIPPED"]
    if not skipped:
        return
    
    # Group skipped flavors by reason
    grouped: Dict[str, List[str]] = defaultdict(list)
    default_reason = (
        "The source asset has a lower quality than the requested output; "
        "transcoding was not performed."
    )
    
    for c in skipped:
        reason = c.reason or default_reason
        grouped[reason].append(c.asset_id)
    
    print("⏭️  Skipped by Optimization (NOT_APPLICABLE)")
    for reason, asset_ids in grouped.items():
        count = len(asset_ids)
        if count == 1:
            print(indent([f"• {asset_ids[0]}"]))
        else:
            print(indent([f"• {count} flavors: {', '.join(asset_ids)}"]))
        print(indent([f"  Reason: {reason}"], n=4))
    print()

def print_issues(classified: List[FlavorClassified]) -> None:
    """Print issues and warnings section."""
    to_show: List[str] = []
    for c in classified:
        if c.kind == "ERROR":
            detail = (c.reason or "").replace("\r", "").strip()
            to_show.append(
                f"- {c.asset_id} Status: ERROR (-1) — "
                f"{detail if detail else 'no details'}"
            )
        else:
            low, high = low_high_for_res(
                c.vcodec, c.width, c.height, c.fps, c.bitrate_kbps
            )
            label = codec_baseline_label(c.vcodec)
            if low:
                to_show.append(
                    f"- {c.asset_id} Bitrate may be low for resolution ({label})"
                )
            if high:
                to_show.append(
                    f"- {c.asset_id} Bitrate may be higher than needed "
                    f"for resolution ({label})"
                )
    if to_show:
        print("❗ Issues")
        print(indent(to_show))
        print()

def print_ladder_table(
    classified: List[FlavorClassified],
    include_urls: bool,
    src_h: Optional[int]
) -> None:
    """Print detailed ladder table."""
    print("📶 Ladder")
    print(
        "id           type                 kbps         WxH   fps   "
        "vcodec    exp      Δ%  flags"
    )
    for c in sorted(classified, key=lambda x: (x.bitrate_kbps or 0, x.asset_id)):
        w, h = safe_int(c.width, 0), safe_int(c.height, 0)
        fps = safe_float(c.fps, 0.0)
        kb = safe_int(c.bitrate_kbps, 0)
        vcodec = (c.vcodec or "")

        exp = expected_kbps(vcodec, w, h) if (w and h) else 0
        delta_pct = f"{((kb - exp) / exp * 100):+0.0f}%" if exp > 0 else "   -"

        flags = efficiency_flags(w, h)
        low, high = low_high_for_res(vcodec, w, h, fps, kb)
        if low:
            flags.append("too_low_bitrate_for_res")
        if high:
            flags.append("too_high_bitrate_for_res")
        if src_h and h and h > src_h:
            flags.append("above_source_height")
        flags_str = ";".join(flags) if flags else "-"

        kind_label = (
            "Uploaded Source" if c.kind == "SOURCE"
            else "Transcoded Flavor" if c.kind == "TRANSCODED"
            else c.kind.title()
        )
        wh = f"{w}x{h}" if (w and h) else "0x0"
        fps_txt = f"{fps:.1f}" if fps else "0.0"
        vcodec_txt = (vcodec or "").lower() or "-"

        print(
            f"{c.asset_id:<12} {kind_label:<20} {kb:>6} {wh:>10} {fps_txt:>5}  "
            f"{vcodec_txt:<8} {exp:>6} {delta_pct:>7}  {flags_str}"
        )

        # Inline, human-friendly explanations:
        extra: List[str] = []
        if "non_mod16" in flags:
            extra.append(
                "Dimensions not aligned to 16-pixel macroblocks/CTUs "
                "(slightly less codec/hardware friendly)"
            )
        if "odd_aspect" in flags:
            extra.append("Unusual aspect ratio; verify intended display geometry")
        if "above_source_height" in flags:
            extra.append(
                "Target height exceeds source; Optimization may mark as NOT_APPLICABLE"
            )
        if c.kind == "ERROR" and c.reason:
            extra.append(f"Error details: {c.reason.replace(chr(13), '').strip()}")
        # For skipped flavors, don't repeat the reason here (it's shown in the dedicated skipped section)
        if c.kind == "PENDING":
            extra.append(f"In pipeline: {c.status_label} ({c.status_code})")
        if c.kind == "SOURCE":
            extra.append("Uploaded Source (flavorParamsId=0; not transcoded)")
        if include_urls and c.download_url:
            extra.append(c.download_url)

        if extra:
            print(indent([f"• {line}" for line in extra]))
    print()

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    """
    Main entry point for the script.
    
    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    ap = argparse.ArgumentParser(
        description="Inspect a Kaltura entry's conversion profile and transcode ladder."
    )
    ap.add_argument("--partner-id", type=int, required=True, help="Kaltura Partner ID")
    ap.add_argument(
        "--admin-secret",
        default=os.environ.get("KALTURA_ADMIN_SECRET"),
        help="Kaltura admin secret (or set KALTURA_ADMIN_SECRET environment variable)"
    )
    ap.add_argument("--admin-user-id", required=True, help="Kaltura admin user ID")
    ap.add_argument("--entry-id", required=True, help="Entry ID to analyze")
    ap.add_argument(
        "--service-url",
        default="https://www.kaltura.com/",
        help="Kaltura service URL (default: https://www.kaltura.com/)"
    )
    ap.add_argument(
        "--include-urls",
        action="store_true",
        help="Include per-flavor download URLs (when available)"
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format for machine-readable consumption"
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    args = ap.parse_args()
    
    # Validate admin-secret
    if not args.admin_secret:
        print(
            "❌ Error: --admin-secret is required "
            "(or set KALTURA_ADMIN_SECRET environment variable)", 
            file=sys.stderr
        )
        return 1
    
    # Set up logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Client initialization
        logger.debug(f"Initializing Kaltura client for partner {args.partner_id}")
        cfg = KalturaConfiguration(args.partner_id)
        cfg.serviceUrl = args.service_url
        client = KalturaClient(cfg)
        
        # Authentication
        logger.debug(f"Starting session for user {args.admin_user_id}")
        try:
            ks = client.session.start(
                args.admin_secret,
                args.admin_user_id,
                KalturaSessionType.ADMIN,
                args.partner_id
            )
            if not ks or not isinstance(ks, str) or len(ks) == 0:
                print(
                    "❌ Authentication Failed: Invalid session key returned",
                    file=sys.stderr
                )
                return 2
            client.setKs(ks)
            logger.debug("Session started successfully")
        except Exception as e:
            print(f"❌ Authentication Failed: {e}", file=sys.stderr)
            return 2

        # Fetch entry
        logger.debug(f"Fetching entry {args.entry_id}")
        try:
            entry = fetch_entry(client, args.entry_id)
        except Exception as e:
            print(f"❌ Failed to fetch entry '{args.entry_id}': {e}", file=sys.stderr)
            logger.debug("Entry fetch error details", exc_info=True)
            return 3
        
        # Fetch flavor assets
        logger.debug("Fetching flavor assets")
        try:
            assets = fetch_flavor_assets(client, args.entry_id)
            logger.debug(f"Found {len(assets)} flavor assets")
        except Exception as e:
            print(f"❌ Failed to fetch flavor assets: {e}", file=sys.stderr)
            logger.debug("Flavor assets fetch error details", exc_info=True)
            return 4

        # Robust duration
        duration_ms = extract_duration_ms(entry, assets)

        # Classify flavors
        logger.debug("Classifying flavors")
        classified: List[FlavorClassified] = [
            classify_flavor(client, fa, args.include_urls) for fa in assets
        ]

        # Source dimensions (prefer the uploaded source, fall back to entry dims)
        src_w = safe_int(get_attr_any(entry, "width"), 0)
        src_h = safe_int(get_attr_any(entry, "height"), 0)
        for c in classified:
            if c.kind == "SOURCE":
                src_w = c.width or src_w
                src_h = c.height or src_h
                break

        # Conversion profile
        logger.debug("Fetching conversion profile")
        cp_id = safe_int(
            get_attr_any(entry, "conversionProfileId", "conversion_profile_id"), 0
        )
        try:
            cp = fetch_conversion_profile(client, cp_id)
            enabled_param_ids = fetch_enabled_flavor_param_ids(client, cp)
            params_by_id = (
                fetch_flavor_params_by_ids(client, enabled_param_ids)
                if enabled_param_ids
                else {}
            )
            logger.debug(
                f"Conversion profile: {cp_id}, enabled params: {enabled_param_ids}"
            )
        except Exception as e:
            logger.warning(f"Failed to fetch conversion profile details: {e}")
            cp = None
            enabled_param_ids = []
            params_by_id = {}

        # Output
        if args.json:
            # JSON output
            output_data = {
                "entry": {
                    "id": getattr(entry, "id", ""),
                    "name": getattr(entry, "name", "") or getattr(entry, "title", ""),
                    "type": enum_label_code(
                        getattr(entry, "type", None), "entry_type"
                    )[0],
                    "status": enum_label_code(
                        getattr(entry, "status", None), "entry_status"
                    )[0],
                    "duration_ms": duration_ms,
                    "width": src_w,
                    "height": src_h,
                    "created_at": getattr(entry, "createdAt", None),
                    "updated_at": getattr(entry, "updatedAt", None),
                },
                "conversion_profile": {
                    "id": cp_id,
                    "name": getattr(cp, "name", "") if cp else None,
                    "type": enum_label_code(
                        getattr(cp, "type", None), "cp_type"
                    )[0] if cp else None,
                    "status": enum_label_code(
                        getattr(cp, "status", None), "cp_status"
                    )[0] if cp else None,
                    "enabled_param_ids": enabled_param_ids,
                } if cp else None,
                "flavors": [
                    {
                        "asset_id": c.asset_id,
                        "flavor_params_id": c.flavor_params_id,
                        "status": c.status_label,
                        "kind": c.kind,
                        "reason": c.reason,
                        "is_original": c.is_original,
                        "width": c.width,
                        "height": c.height,
                        "fps": c.fps,
                        "bitrate_kbps": c.bitrate_kbps,
                        "vcodec": c.vcodec,
                        "acodec": c.acodec,
                        "tags": c.tags,
                        "download_url": c.download_url,
                    }
                    for c in classified
                ],
            }
            print(json.dumps(output_data, indent=2))
        else:
            # Human-readable output
            print_overview(entry, duration_ms)
            print_conversion_profile(
                cp, enabled_param_ids, classified, (src_w, src_h), params_by_id
            )
            print_summary(classified)
            print_visual_ladder(classified)
            print_skipped(classified)
            print_issues(classified)
            print_ladder_table(
                classified, include_urls=args.include_urls, src_h=src_h or None
            )

        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        logger.debug("Unexpected error details", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
