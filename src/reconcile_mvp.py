import pandas as pd
from pathlib import Path

RAW = Path("data/raw")
OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

customers = pd.read_parquet(RAW / "customers.parquet")
billing = pd.read_parquet(RAW / "billing_invoices.parquet")
erp = pd.read_parquet(RAW / "erp_revenue.parquet")
dw = pd.read_parquet(RAW / "dw_sales.parquet")

# ---- Normalize to contract-month (USD) ----
billing_m = billing.copy()
billing_m["month"] = billing_m["invoice_date"].dt.to_period("M").dt.to_timestamp()
billing_m = billing_m.groupby(["contract_id", "customer_id", "month"], as_index=False)["amount_usd"].sum()
billing_m = billing_m.rename(columns={"amount_usd": "billing_usd"})

erp_m = erp.copy()
erp_m["month"] = erp_m["revenue_date"].dt.to_period("M").dt.to_timestamp()
erp_m = erp_m.groupby(["contract_id", "customer_id", "month"], as_index=False)["revenue_usd"].sum()
erp_m = erp_m.rename(columns={"revenue_usd": "erp_usd"})

dw_m = dw.copy()
dw_m["month"] = dw_m["event_date"].dt.to_period("M").dt.to_timestamp()
dw_m = dw_m.groupby(["contract_id", "customer_id", "month"], as_index=False)["gross_usd"].sum()
dw_m = dw_m.rename(columns={"gross_usd": "dw_usd"})

# Month spine comes from ERP (covers all months with recognized revenue)
recon = (
    erp_m.merge(billing_m, on=["contract_id", "customer_id", "month"], how="left")
         .merge(dw_m, on=["contract_id", "customer_id", "month"], how="left")
)

recon["billing_usd"] = recon["billing_usd"].fillna(0.0)
recon["dw_usd"] = recon["dw_usd"].fillna(0.0)

recon["var_erp_vs_billing"] = recon["erp_usd"] - recon["billing_usd"]
recon["var_billing_vs_dw"] = recon["billing_usd"] - recon["dw_usd"]
recon["abs_erp_vs_billing"] = recon["var_erp_vs_billing"].abs()
recon["abs_billing_vs_dw"] = recon["var_billing_vs_dw"].abs()

# ---- Variance classification (MVP rules) ----
EPS = 1.0  # ignore tiny rounding

def classify(r):
    if r["erp_usd"] > EPS and r["billing_usd"] <= EPS:
        return "Missing Invoice (Billing)"
    if r["billing_usd"] > EPS and r["abs_billing_vs_dw"] > max(EPS, 0.03 * r["billing_usd"]):
        return "DWH Duplicate / Overcount" if r["dw_usd"] > r["billing_usd"] else "DWH Undercount / Missing Events"
    if r["billing_usd"] > EPS and r["abs_erp_vs_billing"] > max(EPS, 0.03 * r["billing_usd"]):
        pct = r["abs_erp_vs_billing"] / max(r["billing_usd"], EPS)
        return "FX Mismatch (ERP vs Billing)" if pct <= 0.05 else "Timing Difference (RevRec)"
    return "OK"

recon["variance_type"] = recon.apply(classify, axis=1)

out = recon.merge(customers[["customer_id", "customer_name", "segment"]], on="customer_id", how="left")
out["leakage_usd"] = out["var_erp_vs_billing"].clip(lower=0)

cfo_by_type = (
    out.groupby("variance_type", as_index=False)
       .agg(rows=("contract_id", "count"), leakage_usd=("leakage_usd", "sum"))
       .sort_values("leakage_usd", ascending=False)
)

top_customers = (
    out.groupby(["customer_id", "customer_name"], as_index=False)
       .agg(leakage_usd=("leakage_usd", "sum"))
       .sort_values("leakage_usd", ascending=False)
       .head(15)
)

# Save outputs
out.to_parquet(OUT / "variance_table.parquet", index=False)
cfo_by_type.to_parquet(OUT / "cfo_summary_by_type.parquet", index=False)
top_customers.to_parquet(OUT / "top_customers_by_leakage.parquet", index=False)

print(cfo_by_type.to_string(index=False))
print("\nSaved outputs to data/processed/")
