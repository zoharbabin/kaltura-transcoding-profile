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
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v not in (None, "", [], {}):
                return v
    return None

def _unwrap_enum_value(v: Any) -> Any:
    if isinstance(v, (int, str)):
        return v
    for attr in ("value", "val", "code"):
        if hasattr(v, attr):
            raw = getattr(v, attr)
            if isinstance(raw, (int, str)):
                return raw
    return v

def enum_label_code(value: Any, kind: str) -> Tuple[str, Any]:
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

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def fmt_dt(ts: Any) -> str:
    try:
        t = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return t.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(ts)

def fmt_duration_ms(ms: Optional[int]) -> str:
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

def bpp_per_second(kbps: Optional[int], width: Optional[int], height: Optional[int], fps: Optional[float]) -> Optional[float]:
    if not (kbps and width and height and fps) or width <= 0 or height <= 0 or fps <= 0:
        return None
    return (kbps * 1000) / (width * height * fps)

def low_high_for_res(vcodec: Optional[str], width: Optional[int], height: Optional[int],
                     fps: Optional[float], kbps: Optional[int]) -> Tuple[bool, bool]:
    """Return (likely_low, likely_high) for resolution@fps and codec baseline."""
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
    return client.baseEntry.get(entry_id)  # type: ignore

def fetch_flavor_assets(client: KalturaClient, entry_id: str) -> List[KalturaFlavorAsset]:
    f = KalturaFlavorAssetFilter()
    f.entryIdEqual = entry_id
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    res = client.flavorAsset.list(f, pager)  # type: ignore
    return list(res.objects or [])

def fetch_conversion_profile(client: KalturaClient, cp_id: int) -> Optional[KalturaConversionProfile]:
    if cp_id <= 0:
        return None
    try:
        return client.conversionProfile.get(cp_id)  # type: ignore
    except Exception:
        return None

def _parse_csv_ints(csv_str: str) -> List[int]:
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
      2) Fallback to conversionProfileAssetParams.list (collect flavorParamsId).
    """
    ids: List[int] = []
    if cp:
        raw = get_attr_any(cp, "flavorParamsIds", "profileParamsIds", "flavor_params_ids", "profile_params_ids")
        if raw:
            ids = _parse_csv_ints(str(raw))
            if ids:
                return sorted(set(ids))

        # Fallback: list ConversionProfileAssetParams
        try:
            flt = KalturaConversionProfileAssetParamsFilter()
            flt.conversionProfileIdEqual = int(getattr(cp, "id", 0))
            pager = KalturaFilterPager(pageSize=500, pageIndex=1)
            res = client.conversionProfileAssetParams.list(flt, pager)  # type: ignore
            for obj in list(getattr(res, "objects", []) or []):
                pid = getattr(obj, "flavorParamsId", None)
                if isinstance(pid, int):
                    ids.append(pid)
        except Exception:
            pass
    return sorted(set(ids))

def fetch_flavor_params_by_ids(client: KalturaClient, ids: Iterable[int]) -> Dict[int, KalturaFlavorParams]:
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
    status_label, status_code = enum_label_code(getattr(fa, "status", None), "flavor_status")

    is_original = bool(getattr(fa, "isOriginal", False))
    flavor_params_id = getattr(fa, "flavorParamsId", None)
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

    return FlavorClassified(
        asset_id=getattr(fa, "id", ""),
        flavor_params_id=int(flavor_params_id) if isinstance(flavor_params_id, int) else None,
        status_label=status_label,
        status_code=status_code,
        kind=kind,
        reason=reason,
        is_original=is_original,
        width=getattr(fa, "width", None),
        height=getattr(fa, "height", None),
        fps=getattr(fa, "frameRate", None) or getattr(fa, "frame_rate", None),
        bitrate_kbps=getattr(fa, "bitrate", None)
                    or getattr(fa, "bitrateKbps", None)
                    or getattr(fa, "bitrateInKbps", None),
        vcodec=vcodec,
        acodec=acodec,
        tags=getattr(fa, "tags", None),
        download_url=url,
    )

def extract_duration_ms(entry: Any, assets: List[KalturaFlavorAsset]) -> Optional[int]:
    """
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
    pad = " " * n
    return "\n".join(pad + line if line else "" for line in lines)

def render_bitrate_bars(rungs: List[Tuple[str, int]], width: int = 54) -> List[str]:
    if not rungs:
        return []
    max_br = max((k for _, k in rungs), default=1)
    lines: List[str] = []
    prev: Optional[int] = None
    for label, kbps in rungs:
        bar_len = max(1, int((kbps / max_br) * width))
        ratio = f"   (x{kbps / prev:.2f})" if (prev and prev > 0) else ""
        lines.append(f"  {'█' * bar_len:<{width}}  {label:>12}  {kbps:>5} kbps{ratio}")
        prev = kbps
    return lines

def switching_notes(sorted_bitrates: List[int]) -> List[str]:
    notes: List[str] = []
    for a, b in zip(sorted_bitrates, sorted_bitrates[1:]):
        if a <= 0 or b <= 0:
            continue
        r = b / a
        if r < 1.2:
            notes.append(f"• Tiny step: {a}→{b} (x{r:.2f}) — frequent switching, little quality gain.")
        elif r > 2.5:
            notes.append(f"• Large step: {a}→{b} (x{r:.2f}) — insert a mid rung to avoid a quality cliff.")
    return notes

# ──────────────────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────────────────

def print_overview(entry: Any, duration_ms: Optional[int]) -> None:
    etype, etype_code = enum_label_code(getattr(entry, "type", None), "entry_type")
    estatus, estatus_code = enum_label_code(getattr(entry, "status", None), "entry_status")
    stype, stype_code = enum_label_code(getattr(entry, "sourceType", None), "source_type")
    name = getattr(entry, "name", "") or getattr(entry, "title", "")
    # Build a reliable source-download URL (paramId=0)
    partner_id = getattr(entry, "partnerId", "") or getattr(entry, "partner_id", "")
    source_url = f"https://cdnapisec.kaltura.com/p/{partner_id}/sp/{partner_id}00/playManifest/entryId/{entry.id}/format/download/protocol/https/flavorParamIds/0"
    print("📌 Overview")
    print(
        indent(
            [
                f"Entry:   {entry.id} — {name}",
                f"Type:    {etype} ({etype_code})    Status: {estatus} ({estatus_code})",
                f"User:    {getattr(entry, 'userId', '')}  Duration: {fmt_duration_ms(duration_ms)}",
                f"Source:  {stype} ({stype_code})  Download: {source_url}",
                f"Created: {fmt_dt(getattr(entry, 'createdAt', None))}  Updated: {fmt_dt(getattr(entry, 'updatedAt', None))}",
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
    shown = [c for c in classified if c.kind in {"TRANSCODED", "SOURCE"} and (c.bitrate_kbps or 0) > 0]
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
    rungs = sorted(
        [(c.asset_id, int(c.bitrate_kbps)) for c in classified if c.kind in {"TRANSCODED", "SOURCE"} and (c.bitrate_kbps or 0) > 0],
        key=lambda x: x[1],
    )
    if not rungs:
        return
    print("📈 Visual Ladder (bitrate bars)")
    for line in render_bitrate_bars(rungs):
        print(line)
    notes = switching_notes([kb for _, kb in rungs])
    if notes:
        print("  Guidance: Aim for ~1.3–1.6× bitrate increase between adjacent rungs to stabilize ABR switching.")
        print("  Switching Notes:")
        for n in notes:
            print("   " + n)
    print()

def print_skipped(classified: List[FlavorClassified]) -> None:
    skipped = [c for c in classified if c.kind == "SKIPPED"]
    if not skipped:
        return
    print("⏭️  Skipped by Optimization (NOT_APPLICABLE)")
    for c in skipped:
        reason = c.reason or "The source asset has a lower quality than the requested output; transcoding was not performed."
        print(indent([f"- {c.asset_id} reason: {reason}"]))
    print()

def print_issues(classified: List[FlavorClassified]) -> None:
    to_show: List[str] = []
    for c in classified:
        if c.kind == "ERROR":
            detail = (c.reason or "").replace("\r", "").strip()
            to_show.append(f"- {c.asset_id} Status: ERROR (-1) — {detail if detail else 'no details'}")
        else:
            low, high = low_high_for_res(c.vcodec, c.width, c.height, c.fps, c.bitrate_kbps)
            label = codec_baseline_label(c.vcodec)
            if low:
                to_show.append(f"- {c.asset_id} Bitrate may be low for resolution ({label})")
            if high:
                to_show.append(f"- {c.asset_id} Bitrate may be higher than needed for resolution ({label})")
    if to_show:
        print("❗ Issues")
        print(indent(to_show))
        print()

def print_ladder_table(classified: List[FlavorClassified], include_urls: bool, src_h: Optional[int]) -> None:
    print("📶 Ladder")
    print("id           type                 kbps         WxH   fps   vcodec    exp      Δ%  flags")
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

        print(f"{c.asset_id:<12} {kind_label:<20} {kb:>6} {wh:>10} {fps_txt:>5}  {vcodec_txt:<8} {exp:>6} {delta_pct:>7}  {flags_str}")

        # Inline, human-friendly explanations:
        extra: List[str] = []
        if "non_mod16" in flags:
            extra.append("Dimensions not aligned to 16-pixel macroblocks/CTUs (slightly less codec/hardware friendly)")
        if "odd_aspect" in flags:
            extra.append("Unusual aspect ratio; verify intended display geometry")
        if "above_source_height" in flags:
            extra.append("Target height exceeds source; Optimization may mark as NOT_APPLICABLE")
        if c.kind == "ERROR" and c.reason:
            extra.append(f"Error details: {c.reason.replace(chr(13), '').strip()}")
        if c.kind == "SKIPPED":
            extra.append(f"Skipped (NOT_APPLICABLE [4]): {c.reason or 'Optimization skipped: source < target/policy'}")
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
    ap = argparse.ArgumentParser(description="Inspect a Kaltura entry’s conversion profile and transcode ladder.")
    ap.add_argument("--partner-id", type=int, required=True)
    ap.add_argument("--admin-secret", required=True)
    ap.add_argument("--admin-user-id", required=True)
    ap.add_argument("--entry-id", required=True)
    ap.add_argument("--service-url", default="https://www.kaltura.com/")
    ap.add_argument("--include-urls", action="store_true", help="Include per-flavor download URLs (when available).")
    args = ap.parse_args()

    # Client
    cfg = KalturaConfiguration(args.partner_id)
    cfg.serviceUrl = args.service_url
    client = KalturaClient(cfg)
    ks = client.session.start(args.admin_secret, args.admin_user_id, KalturaSessionType.ADMIN, args.partner_id)
    client.setKs(ks)

    # Fetch entry & flavors
    entry = fetch_entry(client, args.entry_id)
    assets = fetch_flavor_assets(client, args.entry_id)

    # Robust duration
    duration_ms = extract_duration_ms(entry, assets)

    # Classify flavors
    classified: List[FlavorClassified] = [classify_flavor(client, fa, args.include_urls) for fa in assets]

    # Source dimensions (prefer the uploaded source, fall back to entry dims)
    src_w = safe_int(get_attr_any(entry, "width"), 0)
    src_h = safe_int(get_attr_any(entry, "height"), 0)
    for c in classified:
        if c.kind == "SOURCE":
            src_w = c.width or src_w
            src_h = c.height or src_h
            break

    # Conversion profile
    cp_id = safe_int(get_attr_any(entry, "conversionProfileId", "conversion_profile_id"), 0)
    cp = fetch_conversion_profile(client, cp_id)
    enabled_param_ids = fetch_enabled_flavor_param_ids(client, cp)
    params_by_id = fetch_flavor_params_by_ids(client, enabled_param_ids) if enabled_param_ids else {}

    # Output
    print_overview(entry, duration_ms)
    print_conversion_profile(cp, enabled_param_ids, classified, (src_w, src_h), params_by_id)
    print_summary(classified)
    print_visual_ladder(classified)
    print_skipped(classified)
    print_issues(classified)
    print_ladder_table(classified, include_urls=args.include_urls, src_h=src_h or None)

    return 0

if __name__ == "__main__":
    sys.exit(main())
