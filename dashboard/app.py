import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(
    page_title="Revenue Reconciliation Dashboard",
    layout="wide",
)

DATA = Path("data/processed")

@st.cache_data
def load_data():
    drill = pd.read_parquet(DATA / "variance_drilldown.parquet")
    cfo = pd.read_parquet(DATA / "cfo_summary_by_type.parquet")
    return drill, cfo

drill, cfo = load_data()

# ------------------------
# Header
# ------------------------
st.title("Revenue Reconciliation & Leakage Dashboard")
st.caption("ERP × Billing × Data Warehouse — Finance-grade reconciliation")

# ------------------------
# KPIs
# ------------------------
total_leakage = drill["leakage_usd"].sum()
material_leakage = drill.loc[drill["materiality_flag"] == "Material", "leakage_usd"].sum()
high_priority = drill.loc[drill["priority"] == "High", "leakage_usd"].sum()

col1, col2, col3 = st.columns(3)
col1.metric("Total Identified Leakage (USD)", f"${total_leakage:,.0f}")
col2.metric("Material Leakage (USD)", f"${material_leakage:,.0f}")
col3.metric("High Priority Leakage (USD)", f"${high_priority:,.0f}")

st.divider()

# ------------------------
# Leakage by type
# ------------------------
st.subheader("Leakage by Variance Type")

cfo_sorted = cfo.sort_values("leakage_usd", ascending=False)

st.bar_chart(
    cfo_sorted.set_index("variance_type")["leakage_usd"]
)

# ------------------------
# Filters
# ------------------------
st.subheader("Drilldown Filters")

colf1, colf2, colf3 = st.columns(3)

with colf1:
    # normalize priorities to safe strings
    drill["priority"] = drill["priority"].astype(str).str.strip()

    priority_options = sorted(drill["priority"].dropna().unique().tolist())
    preferred = ["High", "Medium"]

    priority_defaults = [p for p in preferred if p in priority_options]
    if not priority_defaults:
        priority_defaults = priority_options  # fallback: select all

    priority_filter = st.multiselect(
        "Priority",
        options=priority_options,
        default=priority_defaults,
        key="priority_filter",
    )



with colf2:
    drill["recommended_owner"] = drill["recommended_owner"].astype(str).str.strip()
    owner_options = sorted(drill["recommended_owner"].dropna().unique().tolist())

    owner_filter = st.multiselect(
        "Owner",
        options=owner_options,
        default=owner_options,
        key="owner_filter",
    )


with colf3:
    drill["variance_type"] = drill["variance_type"].astype(str).str.strip()
    variance_options = sorted(drill["variance_type"].dropna().unique().tolist())

    variance_filter = st.multiselect(
        "Variance Type",
        options=variance_options,
        default=variance_options,
        key="variance_filter",
    )


filtered = drill[
    (drill["priority"].isin(priority_filter))
    & (drill["recommended_owner"].isin(owner_filter))
    & (drill["variance_type"].isin(variance_filter))
]

# ------------------------
# Drilldown table
# ------------------------
st.subheader("Variance Drilldown (Actionable)")

st.download_button(
    "Download (CSV) — High Priority",
    data=filtered[filtered["priority"] == "High"].to_csv(index=False).encode("utf-8"),
    file_name="high_priority_variances.csv",
    mime="text/csv",
)


st.dataframe(
    filtered[
        [
            "customer_name",
            "contract_id",
            "month",
            "variance_type",
            "leakage_usd",
            "priority",
            "recommended_owner",
            "recommended_action",
        ]
    ].sort_values("leakage_usd", ascending=False),
    use_container_width=True,
)

# ------------------------
# Top customers
# ------------------------
st.subheader("Top Customers by Leakage")

top_customers = (
    filtered.groupby("customer_name", as_index=False)
    .agg(leakage_usd=("leakage_usd", "sum"))
    .sort_values("leakage_usd", ascending=False)
    .head(10)
)

st.bar_chart(
    top_customers.set_index("customer_name")["leakage_usd"]
)

st.divider()
st.caption("This dashboard is powered by a synthetic finance reconciliation engine for portfolio demonstration.")
