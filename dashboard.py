import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg
import streamlit as st

# ---- Configuration ----
# Add this in Streamlit Cloud "Secrets" as DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(page_title="Crypto Prices (Neon)", page_icon="📈", layout="wide")
st.title("📈 Crypto Dashboard (Neon Postgres)")

if not DATABASE_URL:
    st.warning("DATABASE_URL env var is not set. Add it in Streamlit Cloud → Secrets.")
    st.stop()

# ---- Data helpers ----
@st.cache_data(ttl=60)
def get_pairs():
    with psycopg.connect(DATABASE_URL) as conn:
        return pd.read_sql("SELECT DISTINCT pair FROM cryptoprices ORDER BY pair", conn)["pair"].tolist()

@st.cache_data(ttl=60)
def load_data(pair: str, days: int):
    with psycopg.connect(DATABASE_URL) as conn:
        # NOTE on MA75: if you created the column with quotes "MA75", use "MA75" in the SQL below.
        # If you created it without quotes, Postgres stores it as lowercase ma75.
        sql = """
        SELECT
            time,
            pair,
            open, high, low, close,
            volume, avg_volume_20,
            rsi14,
            macd, macd_signal,
            bb_upper, bb_lower, bb_basis,
            ma50, ma100, ma200,
            sma10, sma50,
            vwma10, vwma20, vwma50,
            ma75
        FROM cryptoprices
        WHERE pair = %s
          AND time >= NOW() - INTERVAL '%s days'
        ORDER BY time ASC
        """
        # If the column is actually quoted as "MA75", swap the last line in the SELECT to "MA75" AS ma75
        try:
            df = pd.read_sql(sql, conn, params=(pair, days))
        except Exception as e:
            # Fallback if MA75 was created quoted as "MA75":
            sql2 = sql.replace("ma75", '"MA75" AS ma75')
            df = pd.read_sql(sql2, conn, params=(pair, days))
        return df

# ---- Sidebar controls ----
pairs = get_pairs()
default_pair = "BTCUSDT" if "BTCUSDT" in pairs else (pairs[0] if pairs else None)

st.sidebar.header("Filters")
pair = st.sidebar.selectbox("Trading pair", pairs, index=(pairs.index(default_pair) if default_pair in pairs else 0))
days = st.sidebar.slider("Days of history", min_value=7, max_value=365, value=90, step=1)

if not pair:
    st.info("No pairs found in table `cryptoprices`.")
    st.stop()

# ---- Load + show data ----
df = load_data(pair, days)
if df.empty:
    st.info("No rows returned for the selected filters.")
    st.stop()

st.caption(f"Rows: {len(df):,}  •  Range: {df['time'].min()} → {df['time'].max()}")

# Top-level KPIs
latest = df.iloc[-1]
kpi_cols = st.columns(4)
kpi_cols[0].metric("Close", f"{latest['close']:.4f}")
kpi_cols[1].metric("RSI(14)", f"{latest['rsi14']:.2f}" if pd.notna(latest['rsi14']) else "—")
kpi_cols[2].metric("MACD", f"{latest['macd']:.4f}" if pd.notna(latest['macd']) else "—")
kpi_cols[3].metric("Vol (last)", f"{latest['volume']:.0f}" if pd.notna(latest['volume']) else "—")

# Price chart (with MAs/Bands if present)
st.subheader(f"Price: {pair}")
price_cols = st.columns(2)

with price_cols[0]:
    plot_df = df[["time", "close"]].rename(columns={"time":"Time","close":"Close"})
    st.line_chart(plot_df, x="Time", y="Close")

with price_cols[1]:
    overlay = df[["time", "close"]].rename(columns={"time":"Time","close":"Close"})
    # Add columns if they exist
    for col in ["ma50", "ma100", "ma200", "sma50", "bb_upper", "bb_lower", "bb_basis"]:
        if col in df.columns and df[col].notna().any():
            overlay[col.upper()] = df[col]
    st.line_chart(overlay, x="Time")

# Indicators
st.subheader("Indicators")
ind_cols = st.columns(2)

with ind_cols[0]:
    if {"macd", "macd_signal"}.issubset(df.columns):
        macd_df = df[["time","macd","macd_signal"]].rename(columns={"time":"Time","macd":"MACD","macd_signal":"Signal"})
        st.line_chart(macd_df, x="Time")
    else:
        st.info("MACD columns not found.")

with ind_cols[1]:
    if "rsi14" in df.columns:
        rsi_df = df[["time","rsi14"]].rename(columns={"time":"Time","rsi14":"RSI14"})
        st.line_chart(rsi_df, x="Time")
    else:
        st.info("RSI14 column not found.")

# Table view
st.subheader("Raw data (latest 200)")
st.dataframe(df.tail(200), use_container_width=True)
