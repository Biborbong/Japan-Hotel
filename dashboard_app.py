#!/usr/bin/env python3
"""
Hotel REIT & JNTO Tourism Dashboard — Dual Y-Axis Architecture
Interactive visualization for Japanese Hotel REITs and inbound tourism data.

Chart layout:
  Chart 1 (ADR):           Left=JHR ADR, INV ADR (JPY)      Right=JHR ADR YoY, INV ADR YoY (%)
  Chart 2 (RevPAR+Guide):  Left=JHR RevPAR, INV RevPAR (JPY) Right=JHR RevPAR YoY, INV RevPAR YoY, INV Guide (%)
  Chart 3 (Occupancy):     Left=JHR Occ, INV Occ (0-100%)
  Chart 4 (Revenue):       Left=JHR Rev, INV Rev (M JPY)     Right=JHR Rev YoY, INV Rev YoY (%)
"""

import os
import sys
import math
import re as _re
import subprocess
from datetime import datetime, date

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JHR_CSV = os.path.join(BASE_DIR, "output", "JHRTH_Extracted_Data.csv")
INV_CSV = os.path.join(BASE_DIR, "output", "Invincible_Extracted_Data.csv")
JNTO_CSV = os.path.join(BASE_DIR, "output", "JNTO_Extracted_Data.csv")
EXTRACTOR_SCRIPT = os.path.join(BASE_DIR, "hotel_reit_extractor.py")
JNTO_SCRIPT = os.path.join(BASE_DIR, "jnto_scraper.py")

DEFAULT_START = date(2019, 1, 1)
EARLIEST_DATE = date(2003, 1, 1)

# ── Color palette ───────────────────────────────────────────────────────────────
C_JHR_ABS    = "#1f77b4"   # blue  – JHR absolute lines
C_JHR_YOY    = "#aec7e8"   # light blue – JHR YoY lines
C_INV_ABS    = "#ff7f0e"   # orange – INV absolute lines
C_INV_YOY    = "#ffbb78"   # light orange – INV YoY lines
C_INV_GUIDE  = "#d62728"   # red dot – INV guidance

_JNTO_BASE_PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5",
    "#393b79", "#637939", "#8c6d31", "#843c39", "#7b4173",
    "#3182bd", "#e6550d", "#31a354", "#756bb1", "#636363",
]


def _jnto_color(i: int) -> str:
    """Return a distinct color for the i-th country trace. Cycles over the full palette."""
    return _JNTO_BASE_PALETTE[i % len(_JNTO_BASE_PALETTE)]

JNTO_DEFAULTS = ["Total", "South Korea", "China", "Taiwan", "Hong Kong", "USA"]


# ── Data loading ────────────────────────────────────────────────────────────────

def _file_mtime(path: str) -> float:
    """Return file modification time, or 0 if file doesn't exist."""
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


@st.cache_data(show_spinner=False)
def load_reit_data(_mtime_jhr: float, _mtime_inv: float):
    """Load REIT CSVs. Cache key includes file mtimes — auto-invalidates on file change."""
    jhr_df = inv_df = None
    if os.path.exists(JHR_CSV):
        jhr_df = pd.read_csv(JHR_CSV)
        jhr_df["Date"] = pd.to_datetime(jhr_df["Date"], format="%Y/%m")
        jhr_df = jhr_df.sort_values("Date").reset_index(drop=True)
    if os.path.exists(INV_CSV):
        inv_df = pd.read_csv(INV_CSV)
        inv_df["Date"] = pd.to_datetime(inv_df["Date"], format="%Y/%m")
        inv_df = inv_df.sort_values("Date").reset_index(drop=True)
    return jhr_df, inv_df


@st.cache_data(show_spinner=False)
def load_jnto_data(_mtime: float):
    """Load JNTO CSV. Cache key includes file mtime — auto-invalidates on file change."""
    if not os.path.exists(JNTO_CSV):
        return None
    df = pd.read_csv(JNTO_CSV)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y/%m")
    df = df.sort_values("Date").reset_index(drop=True)
    return df


def latest_date_label(df) -> str:
    if df is not None and len(df) > 0:
        return df["Date"].max().strftime("%b %Y")
    return "N/A"


# ── Refresh ─────────────────────────────────────────────────────────────────────

def refresh_all_data() -> bool:
    success = True
    with st.spinner("Running REIT extraction pipeline…"):
        try:
            r = subprocess.run(
                [sys.executable, EXTRACTOR_SCRIPT],
                capture_output=True, text=True, timeout=600, cwd=BASE_DIR,
            )
            if r.returncode != 0:
                st.error(f"REIT extractor error:\n{r.stderr[-2000:]}")
                success = False
            else:
                st.success("REIT data refreshed.")
        except subprocess.TimeoutExpired:
            st.error("REIT extraction timed out.")
            success = False
        except Exception as e:
            st.error(f"REIT extraction failed: {e}")
            success = False

    with st.spinner("Running JNTO download & parse…"):
        try:
            r = subprocess.run(
                [sys.executable, JNTO_SCRIPT],
                capture_output=True, text=True, timeout=120, cwd=BASE_DIR,
            )
            if r.returncode != 0:
                st.warning(f"JNTO scraper warning:\n{r.stderr[-1000:]}")
            else:
                st.success("JNTO data refreshed.")
        except subprocess.TimeoutExpired:
            st.warning("JNTO scraper timed out — using cached data.")
        except Exception as e:
            st.warning(f"JNTO scraper: {e}")

    st.cache_data.clear()
    return success


# ── Y-axis helpers ──────────────────────────────────────────────────────────────

def _visible_range(series: pd.Series, start_ts: pd.Timestamp, dates: pd.Series,
                   pad_pct: float = 0.08):
    """Compute [min, max] for a series filtered to the visible date window."""
    mask = dates >= start_ts
    visible = series[mask].dropna()
    if visible.empty:
        return None
    lo, hi = visible.min(), visible.max()
    span = hi - lo
    if span == 0:
        span = abs(hi) * 0.1 or 1.0
    pad = span * pad_pct
    return [lo - pad, hi + pad]


def _occ_range(series: pd.Series, start_ts: pd.Timestamp, dates: pd.Series):
    """Occupancy: always 0–105, but shrink lower bound if all data > 20."""
    return [0, 105]


# ── Dual-Y chart builder ────────────────────────────────────────────────────────

COMMON_LAYOUT = dict(
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=10, r=10, t=60, b=20),
    height=440,
)


def _add_line(fig, x, y, name, color, dash="solid", width=2, marker_size=5,
              secondary_y=False, hover_fmt="%{y:,.1f}", visible=True):
    """Add a scatter line trace to a secondary-y subplot."""
    fig.add_trace(
        go.Scatter(
            x=x, y=y,
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=width, dash=dash),
            marker=dict(size=marker_size),
            hovertemplate=f"<b>{name}</b><br>%{{x|%b %Y}}: {hover_fmt}<extra></extra>",
            visible=True if visible else "legendonly",
        ),
        secondary_y=secondary_y,
    )


def _add_dot(fig, x, y, name, color, secondary_y=False, visible=True):
    """Add marker-only (dot) trace for guidance values."""
    fig.add_trace(
        go.Scatter(
            x=x, y=y,
            mode="markers",
            name=name,
            marker=dict(color=color, size=9, symbol="circle"),
            hovertemplate=f"<b>{name}</b><br>%{{x|%b %Y}}: %{{y:+.1f}}%<extra></extra>",
            visible=True if visible else "legendonly",
        ),
        secondary_y=secondary_y,
    )


def _filter(df, col, start_ts):
    """Return (dates, values) arrays filtered to start_ts, dropping NaN."""
    if df is None or col not in df.columns:
        return pd.Series(dtype="float64"), pd.Series(dtype="float64")
    mask = (df["Date"] >= start_ts) & df[col].notna()
    return df.loc[mask, "Date"], df.loc[mask, col]


def _make_dual_fig(title: str):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_layout(title=title, **COMMON_LAYOUT)
    return fig


# ── Chart 1: ADR ───────────────────────────────────────────────────────────────

def chart_adr(jhr_df, inv_df, start_ts: pd.Timestamp, toggles: dict) -> go.Figure:
    fig = _make_dual_fig("Average Daily Rate (ADR)")

    # Left axis — absolute ADR
    x, y = _filter(jhr_df, "ADR_JPY", start_ts)
    if toggles.get("jhr_adr"):
        _add_line(fig, x, y, "JHR ADR (JPY)", C_JHR_ABS, hover_fmt="¥%{y:,.0f}")
    x2, y2 = _filter(inv_df, "ADR_JPY", start_ts)
    if toggles.get("inv_adr"):
        _add_line(fig, x2, y2, "INV ADR (JPY)", C_INV_ABS, hover_fmt="¥%{y:,.0f}")

    # Right axis — YoY
    xj, yj = _filter(jhr_df, "ADR_YoY_Pct", start_ts)
    if toggles.get("jhr_adr_yoy"):
        _add_line(fig, xj, yj, "JHR ADR YoY (%)", C_JHR_YOY, dash="dot",
                  secondary_y=True, hover_fmt="%{y:+.1f}%")
    xi, yi = _filter(inv_df, "ADR_Diff_Pct", start_ts)
    if toggles.get("inv_adr_yoy"):
        _add_line(fig, xi, yi, "INV ADR YoY (%)", C_INV_YOY, dash="dot",
                  secondary_y=True, hover_fmt="%{y:+.1f}%")

    # Dynamic Y ranges
    left_r = _visible_range(
        pd.concat([y, y2]), start_ts,
        pd.concat([x, x2]) if not x.empty or not x2.empty else pd.Series(dtype="datetime64[ns]"),
    )
    right_r = _visible_range(
        pd.concat([yj, yi]), start_ts,
        pd.concat([xj, xi]) if not xj.empty or not xi.empty else pd.Series(dtype="datetime64[ns]"),
    )
    fig.update_xaxes(type="date")
    fig.update_yaxes(title_text="ADR (JPY)", secondary_y=False,
                     **({"range": left_r} if left_r else {}))
    fig.update_yaxes(title_text="YoY Change (%)", secondary_y=True,
                     **({"range": right_r} if right_r else {}))
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.3, secondary_y=True)
    return fig


# ── Chart 2: RevPAR + Guidance ─────────────────────────────────────────────────

def _parse_guidance(df) -> tuple[pd.Series, pd.Series]:
    """Extract INV next-month RevPAR guidance as (dates, pct_values)."""
    if df is None or "Next_Month_RevPAR_Forecast" not in df.columns:
        return pd.Series(dtype="datetime64[ns]"), pd.Series(dtype="float64")
    rows = df[df["Next_Month_RevPAR_Forecast"].notna()].copy()
    if rows.empty:
        return pd.Series(dtype="datetime64[ns]"), pd.Series(dtype="float64")
    # Shift date by 1 month (guidance is for next month)
    dates = rows["Date"] + pd.DateOffset(months=1)
    vals = rows["Next_Month_RevPAR_Forecast"].str.extract(r'([+-]?\d+\.?\d*)%')[0].astype(float)
    mask = vals.notna()
    return dates[mask].reset_index(drop=True), vals[mask].reset_index(drop=True)


def chart_revpar(jhr_df, inv_df, start_ts: pd.Timestamp, toggles: dict) -> go.Figure:
    fig = _make_dual_fig("RevPAR & Guidance")

    # Left axis — absolute RevPAR
    x, y = _filter(jhr_df, "RevPAR_JPY", start_ts)
    if toggles.get("jhr_revpar"):
        _add_line(fig, x, y, "JHR RevPAR (JPY)", C_JHR_ABS, hover_fmt="¥%{y:,.0f}")
    x2, y2 = _filter(inv_df, "RevPAR_JPY", start_ts)
    if toggles.get("inv_revpar"):
        _add_line(fig, x2, y2, "INV RevPAR (JPY)", C_INV_ABS, hover_fmt="¥%{y:,.0f}")

    # Right axis — YoY
    xj, yj = _filter(jhr_df, "RevPAR_YoY_Pct", start_ts)
    if toggles.get("jhr_revpar_yoy"):
        _add_line(fig, xj, yj, "JHR RevPAR YoY (%)", C_JHR_YOY, dash="dot",
                  secondary_y=True, hover_fmt="%{y:+.1f}%")
    xi, yi = _filter(inv_df, "RevPAR_Diff_Pct", start_ts)
    if toggles.get("inv_revpar_yoy"):
        _add_line(fig, xi, yi, "INV RevPAR YoY (%)", C_INV_YOY, dash="dot",
                  secondary_y=True, hover_fmt="%{y:+.1f}%")

    # Right axis — Guidance dots
    gx, gy = _parse_guidance(inv_df)
    gx_filt = gx[gx >= start_ts] if not gx.empty else gx
    gy_filt = gy[gx >= start_ts] if not gx.empty else gy
    if toggles.get("inv_guide") and not gx_filt.empty:
        _add_dot(fig, gx_filt, gy_filt, "INV Guidance (%)", C_INV_GUIDE, secondary_y=True)

    # Dynamic Y ranges
    left_r = _visible_range(pd.concat([y, y2]), start_ts, pd.concat([x, x2]))
    all_yoy = pd.concat([yj, yi] + ([gy_filt] if not gy_filt.empty else []))
    right_r = _visible_range(all_yoy, start_ts, pd.concat(
        [xj, xi] + ([gx_filt] if not gx_filt.empty else [])
    ))
    fig.update_xaxes(type="date")
    fig.update_yaxes(title_text="RevPAR (JPY)", secondary_y=False,
                     **({"range": left_r} if left_r else {}))
    fig.update_yaxes(title_text="YoY Change (%)", secondary_y=True,
                     **({"range": right_r} if right_r else {}))
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.3, secondary_y=True)
    return fig


# ── Chart 3: Occupancy ──────────────────────────────────────────────────────────

def chart_occupancy(jhr_df, inv_df, start_ts: pd.Timestamp, toggles: dict) -> go.Figure:
    fig = _make_dual_fig("Occupancy Rate")

    x, y = _filter(jhr_df, "Occupancy_Rate_Pct", start_ts)
    if toggles.get("jhr_occ"):
        _add_line(fig, x, y, "JHR Occupancy (%)", C_JHR_ABS, hover_fmt="%{y:.1f}%")
    x2, y2 = _filter(inv_df, "Occupancy_Rate_Pct", start_ts)
    if toggles.get("inv_occ"):
        _add_line(fig, x2, y2, "INV Occupancy (%)", C_INV_ABS, hover_fmt="%{y:.1f}%")

    fig.update_xaxes(type="date")
    fig.update_yaxes(title_text="Occupancy (%)", range=[0, 105], secondary_y=False)
    # Hide unused right axis
    fig.update_yaxes(showticklabels=False, secondary_y=True)
    return fig


# ── Chart 4: Revenue ────────────────────────────────────────────────────────────

def chart_revenue(jhr_df, inv_df, start_ts: pd.Timestamp, toggles: dict) -> go.Figure:
    fig = _make_dual_fig("Revenue")

    x, y = _filter(jhr_df, "Revenue_JPY_Millions", start_ts)
    if toggles.get("jhr_rev"):
        _add_line(fig, x, y, "JHR Revenue (M JPY)", C_JHR_ABS, hover_fmt="¥%{y:,.0f}M")
    x2, y2 = _filter(inv_df, "Revenue_JPY_Millions", start_ts)
    if toggles.get("inv_rev"):
        _add_line(fig, x2, y2, "INV Revenue (M JPY)", C_INV_ABS, hover_fmt="¥%{y:,.0f}M")

    xj, yj = _filter(jhr_df, "Revenue_YoY_Pct", start_ts)
    if toggles.get("jhr_rev_yoy"):
        _add_line(fig, xj, yj, "JHR Revenue YoY (%)", C_JHR_YOY, dash="dot",
                  secondary_y=True, hover_fmt="%{y:+.1f}%")
    xi, yi = _filter(inv_df, "Revenue_Diff_Pct", start_ts)
    if toggles.get("inv_rev_yoy"):
        _add_line(fig, xi, yi, "INV Revenue YoY (%)", C_INV_YOY, dash="dot",
                  secondary_y=True, hover_fmt="%{y:+.1f}%")

    left_r = _visible_range(pd.concat([y, y2]), start_ts, pd.concat([x, x2]))
    right_r = _visible_range(pd.concat([yj, yi]), start_ts, pd.concat([xj, xi]))
    fig.update_xaxes(type="date")
    fig.update_yaxes(title_text="Revenue (M JPY)", secondary_y=False,
                     **({"range": left_r} if left_r else {}))
    fig.update_yaxes(title_text="YoY Change (%)", secondary_y=True,
                     **({"range": right_r} if right_r else {}))
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.3, secondary_y=True)
    return fig


# ── JNTO chart ──────────────────────────────────────────────────────────────────

def chart_jnto(jnto_df: pd.DataFrame, countries: list, show_absolute: bool,
               start_ts: pd.Timestamp) -> go.Figure:
    col = "Visitors" if show_absolute else "YoY_Change"
    yaxis_title = "Foreign Visitors" if show_absolute else "YoY Change (%)"
    title_str = (
        "Inbound Visitors to Japan by Nationality — Absolute"
        if show_absolute else
        "Inbound Visitors to Japan — Year-over-Year % Change"
    )
    fig = go.Figure()
    for i, country in enumerate(countries):
        sub = jnto_df[(jnto_df["Country"] == country) & (jnto_df["Date"] >= start_ts)][
            ["Date", col]
        ].dropna().sort_values("Date")
        if sub.empty:
            continue
        color = _jnto_color(i)
        hover = (
            f"<b>{country}</b><br>%{{x|%b %Y}}: "
            + ("%{y:,.0f}" if show_absolute else "%{y:+.1f}%")
            + "<extra></extra>"
        )
        fig.add_trace(go.Scatter(
            x=sub["Date"], y=sub[col],
            mode="lines+markers", name=country,
            line=dict(color=color, width=2), marker=dict(size=5),
            hovertemplate=hover,
        ))
    if not show_absolute:
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
    fig.update_layout(
        title=title_str,
        xaxis_type="date",
        yaxis_title=yaxis_title,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=60, b=20),
        height=460,
    )
    if show_absolute:
        fig.update_yaxes(tickformat=",")
    return fig


# ── Toggle row helper ───────────────────────────────────────────────────────────

def _toggle_row(labels_keys: list[tuple[str, str]], ns: str) -> dict:
    """Render a row of checkboxes and return {key: bool}."""
    cols = st.columns(len(labels_keys))
    toggles = {}
    for col_obj, (label, key) in zip(cols, labels_keys):
        with col_obj:
            toggles[key] = st.checkbox(label, value=True, key=f"{ns}_{key}")
    return toggles


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Japanese Hotel REIT & Tourism Dashboard",
        page_icon="🏨",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🏨 Japanese Hotel REIT & Tourism Dashboard")
    st.markdown(
        "Compare **JHR** (Japan Hotel REIT) and **Invincible Investment** "
        "performance metrics, plus JNTO inbound tourism data.  \n"
        "Left axis = absolute values · Right axis = YoY % change"
    )

    # ── Load data ───────────────────────────────────────────────────────────────
    jhr_df, inv_df = load_reit_data(_mtime_jhr=_file_mtime(JHR_CSV),
                                    _mtime_inv=_file_mtime(INV_CSV))
    jnto_df = load_jnto_data(_mtime=_file_mtime(JNTO_CSV))

    # ── Sidebar ─────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Controls")

        if st.button("🔄 Refresh All Data", type="primary", use_container_width=True):
            if refresh_all_data():
                st.rerun()

        st.divider()

        st.markdown("### 📅 Chart Start Date")
        st.caption(
            "Default: Jan 2019. Slide back to include historical data from 2003."
        )
        chart_start = st.date_input(
            "Start date",
            value=DEFAULT_START,
            min_value=EARLIEST_DATE,
            max_value=date.today(),
            help="All charts and Y-axes auto-scale to the visible date window.",
        )
        if not isinstance(chart_start, date):
            chart_start = DEFAULT_START

        st.divider()

        st.markdown("### 📅 Latest Data")
        st.markdown(f"**JHR:** {latest_date_label(jhr_df)}")
        st.markdown(f"**Invincible:** {latest_date_label(inv_df)}")
        if jnto_df is not None:
            st.markdown(f"**JNTO:** {latest_date_label(jnto_df)}")

        st.divider()

        st.markdown("### 📊 Record Counts")
        if jhr_df is not None:
            st.markdown(f"**JHR:** {len(jhr_df)} months")
        if inv_df is not None:
            st.markdown(f"**Invincible:** {len(inv_df)} months")
        if jnto_df is not None:
            n_countries = jnto_df["Country"].nunique()
            n_months = jnto_df["Date"].nunique()
            st.markdown(f"**JNTO:** {n_countries} nationalities · {n_months} months")

    if jhr_df is None and inv_df is None:
        st.error("No REIT data found. Click **Refresh All Data** or run the extractor.")
        st.code("python3 hotel_reit_extractor.py")
        return

    start_ts = pd.Timestamp(chart_start)

    # ═══════════════════════════════════════════════════════════════════════════
    # Chart 1 — ADR
    # ═══════════════════════════════════════════════════════════════════════════
    st.subheader("📈 Chart 1 — Average Daily Rate (ADR)")
    st.caption("Left axis: ADR in JPY · Right axis: YoY % change")

    adr_toggles = _toggle_row([
        ("JHR ADR",      "jhr_adr"),
        ("INV ADR",      "inv_adr"),
        ("JHR ADR YoY",  "jhr_adr_yoy"),
        ("INV ADR YoY",  "inv_adr_yoy"),
    ], ns="adr")

    st.plotly_chart(
        chart_adr(jhr_df, inv_df, start_ts, adr_toggles),
        use_container_width=True,
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # Chart 2 — RevPAR + Guidance
    # ═══════════════════════════════════════════════════════════════════════════
    st.divider()
    st.subheader("📈 Chart 2 — RevPAR & INV Guidance")
    st.caption(
        "Left axis: RevPAR in JPY · Right axis: YoY % change + INV next-month guidance (dots)"
    )

    revpar_toggles = _toggle_row([
        ("JHR RevPAR",      "jhr_revpar"),
        ("INV RevPAR",      "inv_revpar"),
        ("JHR RevPAR YoY",  "jhr_revpar_yoy"),
        ("INV RevPAR YoY",  "inv_revpar_yoy"),
        ("INV Guidance",    "inv_guide"),
    ], ns="revpar")

    st.plotly_chart(
        chart_revpar(jhr_df, inv_df, start_ts, revpar_toggles),
        use_container_width=True,
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # Chart 3 — Occupancy
    # ═══════════════════════════════════════════════════════════════════════════
    st.divider()
    st.subheader("📊 Chart 3 — Occupancy Rate")
    st.caption("Scale: 0–100% (strict). No right axis.")

    occ_toggles = _toggle_row([
        ("JHR Occupancy", "jhr_occ"),
        ("INV Occupancy", "inv_occ"),
    ], ns="occ")

    st.plotly_chart(
        chart_occupancy(jhr_df, inv_df, start_ts, occ_toggles),
        use_container_width=True,
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # Chart 4 — Revenue
    # ═══════════════════════════════════════════════════════════════════════════
    st.divider()
    st.subheader("📈 Chart 4 — Revenue")
    st.caption("Left axis: Revenue in JPY Millions · Right axis: YoY % change")

    rev_toggles = _toggle_row([
        ("JHR Revenue",      "jhr_rev"),
        ("INV Revenue",      "inv_rev"),
        ("JHR Revenue YoY",  "jhr_rev_yoy"),
        ("INV Revenue YoY",  "inv_rev_yoy"),
    ], ns="rev")

    st.plotly_chart(
        chart_revenue(jhr_df, inv_df, start_ts, rev_toggles),
        use_container_width=True,
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # JNTO Section
    # ═══════════════════════════════════════════════════════════════════════════
    st.divider()
    st.header("✈️ JNTO Inbound Tourism Data")
    st.markdown(
        "Source: Japan National Tourism Organization — "
        "Number of Foreign Visitors by Nationality/Month (2003–latest)"
    )

    if jnto_df is None:
        st.warning(
            "JNTO data not found. Click **Refresh All Data** to download it, "
            "or run `python3 jnto_scraper.py`."
        )
        return

    # ── Mode selector ───────────────────────────────────────────────────────
    jnto_mode_col, _ = st.columns([1, 3])
    with jnto_mode_col:
        view_mode = st.radio(
            "Display mode",
            ["Absolute Visitors", "YoY % Change"],
            index=0,
            horizontal=True,
        )
    show_absolute = view_mode == "Absolute Visitors"

    # ── Country selection (6 per row, all countries) ─────────────────────────
    st.markdown("**Select countries/regions** (6 per row):")

    all_countries = sorted(
        c for c in jnto_df["Country"].dropna().unique()
        if not _re.search(r'[\u3000-\u9fff\u30a0-\u30ff\u3040-\u309f]', str(c))
    )
    # Priority countries first
    priority = [c for c in JNTO_DEFAULTS if c in all_countries]
    rest = [c for c in all_countries if c not in priority]
    ordered = priority + rest

    # ── Seed checkbox keys on first load (only when key doesn't yet exist) ────
    # st.checkbox respects st.session_state[key] on every run, so we must set
    # each key exactly once (at first load) to avoid overwriting user clicks.
    _defaults_set = set(JNTO_DEFAULTS)
    for _c in ordered:
        _ck = f"jnto_ctry_{_c}"
        if _ck not in st.session_state:
            st.session_state[_ck] = _c in _defaults_set

    # ── Quick-select buttons ─────────────────────────────────────────────────
    # The ONLY correct way to programmatically toggle st.checkbox widgets is to
    # write directly to their session_state keys BEFORE the widgets are rendered.
    qc1, qc2, qc3, _ = st.columns([1, 1, 1, 5])
    with qc1:
        if st.button("Select All"):
            for _c in ordered:
                st.session_state[f"jnto_ctry_{_c}"] = True
    with qc2:
        if st.button("Clear All"):
            for _c in ordered:
                st.session_state[f"jnto_ctry_{_c}"] = False
    with qc3:
        if st.button("Defaults"):
            for _c in ordered:
                st.session_state[f"jnto_ctry_{_c}"] = _c in _defaults_set

    # ── 6-per-row checkbox grid ──────────────────────────────────────────────
    # No line limit: every checked country gets its own trace in the chart.
    COLS_PER_ROW = 6
    n_rows = math.ceil(len(ordered) / COLS_PER_ROW)
    selected_countries = []

    for row_i in range(n_rows):
        row_items = ordered[row_i * COLS_PER_ROW: (row_i + 1) * COLS_PER_ROW]
        cols = st.columns(COLS_PER_ROW)
        for col_i, country in enumerate(row_items):
            with cols[col_i]:
                # Pass no `value=` — state is already in session_state from the
                # seed block above (or from a previous user interaction / button click).
                checked = st.checkbox(country, key=f"jnto_ctry_{country}")
                if checked:
                    selected_countries.append(country)

    # ── JNTO chart — one line per selected country, no cap ─────────────────
    if selected_countries:
        st.caption(f"Plotting {len(selected_countries)} countr{'y' if len(selected_countries)==1 else 'ies'}")
        st.plotly_chart(
            chart_jnto(jnto_df, selected_countries, show_absolute, start_ts),
            use_container_width=True,
        )

        # Summary metric cards — show ALL selected countries, 6 per row
        latest_month = jnto_df["Date"].max()
        st.markdown(f"**Latest available month: {latest_month.strftime('%b %Y')}**")
        METRIC_COLS = 6
        for row_start in range(0, len(selected_countries), METRIC_COLS):
            chunk = selected_countries[row_start: row_start + METRIC_COLS]
            metric_cols = st.columns(len(chunk))
            for col_obj, country in zip(metric_cols, chunk):
                data_row = jnto_df[
                    (jnto_df["Country"] == country) &
                    (jnto_df["Date"] == latest_month)
                ]
                with col_obj:
                    if not data_row.empty and pd.notna(data_row.iloc[0]["Visitors"]):
                        visitors = int(data_row.iloc[0]["Visitors"])
                        yoy = data_row.iloc[0]["YoY_Change"]
                        delta = f"{yoy:+.1f}%" if pd.notna(yoy) else None
                        st.metric(label=country, value=f"{visitors:,}", delta=delta)
                    else:
                        st.metric(label=country, value="N/A")
    else:
        st.info("Select at least one country to display the chart.")

    # ── Footer ─────────────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        "**Data sources:** "
        "Japan Hotel REIT Monthly Disclosures · "
        "Invincible Investment Performance Updates · "
        "JNTO Visitor Statistics (2003–latest)  \n"
        "All Y-axes auto-scale to the visible date window set by the sidebar slider."
    )


if __name__ == "__main__":
    main()
