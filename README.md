---
author:
  name: "Felipe Monteiro"
  title: "Economist | Senior Data Analyst"
  focus: ["Finance Analytics", "Data Products"]
  location: "Brazil (Remote)"
  github: "https://github.com/FMPING"
---

# Revenue Reconciliation & Leakage Engine

Finance-grade revenue reconciliation engine that simulates **ERP**, **Billing**, and **Data Warehouse** datasets, detects revenue leakage, classifies root causes, generates an actionable drilldown (owner + action), and exposes insights in a **Streamlit dashboard**.

---

## 🎯 Business Problem

Revenue lives across multiple systems:
- **ERP** (revenue recognition)
- **Billing** (invoices)
- **Data Warehouse** (analytics & reporting)

Misalignment causes:
- Missing invoices (direct revenue loss)
- FX mismatches
- Timing differences (cutoff issues)
- DWH duplicates / missing events

This project demonstrates how to **detect, classify, and prioritize leakage** end-to-end.

---

## ✅ What This Project Delivers

- Synthetic data generator with controlled discrepancy injection
- Reconciliation engine (contract-month level) + variance taxonomy
- Decision-ready drilldown (priority, owner, recommended action)
- Streamlit dashboard for executive + operational view

---

## 📦 Repository Structure

- `src/generate_data.py` — generates synthetic source extracts (ERP/Billing/DWH)
- `src/reconcile_mvp.py` — reconciliation + variance classification
- `src/variance_drilldown.py` — enriches variances with root cause/owner/action/priority
- `dashboard/app.py` — Streamlit dashboard

---

## 🛠️ Tech Stack
Python, Pandas, NumPy, Parquet (PyArrow), Streamlit

---



