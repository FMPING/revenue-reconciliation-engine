import pandas as pd
import numpy as np
from pathlib import Path

IN = Path("data/processed")
OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

# Load base variance table
df = pd.read_parquet(IN / "variance_table.parquet")

# -------------------------
# Basic diagnostics
# -------------------------
df["variance_pct"] = np.where(
    df["billing_usd"] > 0,
    df["var_erp_vs_billing"] / df["billing_usd"],
    np.nan,
)

# Materiality thresholds
df["materiality_flag"] = np.where(
    (df["leakage_usd"] >= 500) | (df["variance_pct"].abs() >= 0.1),
    "Material",
    "Immaterial",
)

# -------------------------
# Root cause inference
# -------------------------
def infer_root_cause(row):
    vt = row["variance_type"]

    if vt == "Missing Invoice (Billing)":
        return "Invoice not generated or lost between Billing and ERP"
    if vt.startswith("DWH Duplicate"):
        return "Duplicate events ingested in analytics layer"
    if vt.startswith("DWH Undercount"):
        return "Missing analytics events or late ingestion"
    if vt.startswith("FX Mismatch"):
        return "Different FX rate sources or timing between systems"
    if vt.startswith("Timing Difference"):
        return "Revenue recognized in different periods (cutoff issue)"
    return "No issue detected"

df["suspected_root_cause"] = df.apply(infer_root_cause, axis=1)

# -------------------------
# Ownership & actions
# -------------------------
def assign_owner(vt):
    if vt == "Missing Invoice (Billing)":
        return "Billing / RevOps"
    if vt.startswith("DWH"):
        return "Data Engineering"
    if vt.startswith("FX"):
        return "Finance / Accounting"
    if vt.startswith("Timing"):
        return "Finance / RevRec"
    return "None"

def recommend_action(vt):
    if vt == "Missing Invoice (Billing)":
        return "Investigate contract configuration and reissue invoice"
    if vt.startswith("DWH Duplicate"):
        return "Add deduplication logic in ingestion pipeline"
    if vt.startswith("DWH Undercount"):
        return "Backfill missing events and add ingestion monitoring"
    if vt.startswith("FX"):
        return "Align FX rate source and lock FX at invoice date"
    if vt.startswith("Timing"):
        return "Review revenue recognition cutoff rules"
    return "No action required"

df["recommended_owner"] = df["variance_type"].apply(assign_owner)
df["recommended_action"] = df["variance_type"].apply(recommend_action)

# -------------------------
# Priority scoring
# -------------------------
def priority(row):
    if row["materiality_flag"] == "Material" and row["leakage_usd"] >= 2000:
        return "High"
    if row["materiality_flag"] == "Material":
        return "Medium"
    return "Low"

df["priority"] = df.apply(priority, axis=1)

# -------------------------
# Final drilldown table
# -------------------------
cols = [
    "contract_id",
    "customer_name",
    "month",
    "erp_usd",
    "billing_usd",
    "dw_usd",
    "var_erp_vs_billing",
    "leakage_usd",
    "variance_pct",
    "variance_type",
    "materiality_flag",
    "priority",
    "suspected_root_cause",
    "recommended_owner",
    "recommended_action",
]

drilldown = df[cols].sort_values(
    by=["priority", "leakage_usd"], ascending=[True, False]
)

# Save outputs
drilldown.to_parquet(OUT / "variance_drilldown.parquet", index=False)
drilldown.to_csv(OUT / "variance_drilldown.csv", index=False)

print("Variance Drilldown generated:")
print(drilldown.head(10).to_string(index=False))
print("\nSaved:")
print("- data/processed/variance_drilldown.parquet")
print("- data/processed/variance_drilldown.csv")
