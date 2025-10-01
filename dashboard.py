import os
import pandas as pd
import streamlit as st

# We import psycopg inside functions so the app can render errors if it's missing
st.set_page_config(page_title="Neon DB Dashboard", page_icon="📊", layout="wide")
st.title("📊 Neon Postgres → Streamlit")

# 1) Check secret exists
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    st.error(
        "DATABASE_URL is not set. In Streamlit Cloud: App → Settings → Advanced → Edit secrets\n"
        'Add a line like:\nDATABASE_URL="postgresql://...neon.tech/neondb?sslmode=require&channel_binding=require"'
    )
    st.stop()

# 2) Helper: connect and run SQL safely
def run_sql(sql: str, params=None):
    import psycopg  # local import so missing package is clearer
    try:
        with psycopg.connect(DB_URL) as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.exception(e)
        return pd.DataFrame()

# 3) Diagnostics: list tables and columns
with st.expander("🔎 Diagnostics (tables & columns)", expanded=False):
    tables = run_sql("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY 1,2
    """)
    st.write("Tables found:", tables)

    cols = run_sql("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cryptoprices'
        ORDER BY ordinal_position
    """)
    st.write("cryptoprices columns:", cols)

# 4) UI controls
pairs_df = run_sql("SELECT DISTINCT pair FROM cryptoprices ORDER BY pair")
if pairs_df.empty:
    st.warning("No rows in cryptoprices (or table doesn’t exist).")
    st.stop()

pairs = pairs_df["pair"].tolist()
pair = st.sidebar.selectbox("Pair", pairs, index=0)

days = st.sidebar.slider("Days of history", 7, 365, 90)

# 5) Load data with a fallback for case-sensitive MA75
@st.cache_data(ttl=60)
def load_prices(selected_pair: str, day_window: int):
    base_sql = """
        SELECT
            time, pair, open, high, low, close, volume,
            avg_volume_20, rsi14, macd, macd_signal,
            bb_upper, bb_lower, bb_basis,
            ma50, ma100, ma200, sma10, sma50,
            vwma10, vwma20, vwma50,
            {ma75_expr}
        FROM cryptoprices
        WHERE pair = %s AND time >= NOW() - INTERVAL '%s days'
        ORDER BY time ASC
    """
    # try unquoted ma75 first; if it raises, we’ll retry with "MA75" AS ma75
    try:
        df = run_sql(base_sql.format(ma75_expr="ma75"), (selected_pair, day_window))
        if "ma75" not in df.columns:
            raise KeyError("ma75 not present, retry with quoted column")
        return df
    except Exception:
        df = run_sql(base_sql.format(ma75_expr='"MA75" AS ma75'), (selected_pair, day_window))
        return df

df = load_prices(pair, days)
if df.empty:
    st.info("Query returned no rows for the selected filters.")
    st.stop()

# 6) KPIs
latest = df.iloc[-1]
k1, k2, k3, k4 = st.columns(4)
k1.metric("Close", f"{latest['close']:.6f}" if pd.notna(latest['close']) else "—")
k2.metric("RSI(14)", f"{latest['rsi14']:.2f}" if pd.notna(latest.get('rsi14')) else "—")
k3.metric("MACD", f"{latest['macd']:.6f}" if pd.notna(latest.get('macd')) else "—")
k4.metric("Volume", f"{latest['volume']:.0f}" if pd.notna(latest.get('volume')) else "—")

st.caption(f"Rows: {len(df):,} • {df['time'].min()} → {df['time'].max()}")

# 7) Charts (Streamlit default styles; no custom colors)
st.subheader(f"Price for {pair}")
st.line_chart(df.rename(columns={"time":"Time"})[["Time","close"]].set_index("Time"))

st.subheader("Moving Averages")
ma_cols = [c for c in ["sma10","sma50","ma50","ma75","ma100","ma200"] if c in df.columns]
if ma_cols:
    plot_ma = df.rename(columns={"time":"Time"})[["Time"] + ma_cols].set_index("Time")
    st.line_chart(plot_ma)
else:
    st.info("No MA columns found to plot.")

st.subheader("MACD / Signal")
if {"macd","macd_signal"}.issubset(df.columns):
    macd_df = df.rename(columns={"time":"Time"})[["Time","macd","macd_signal"]].set_index("Time")
    st.line_chart(macd_df)
else:
    st.info("MACD columns not found.")

st.subheader("Raw data (latest 200)")
st.dataframe(df.tail(200), use_container_width=True)
