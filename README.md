# 🏦 Banking Data Engineering Pipeline (Bronze → Silver → Gold)

This project demonstrates an end-to-end data engineering pipeline for a simulated banking analytics use case. Built on **Databricks Community Edition** using **Apache Spark (PySpark)** and **Delta Lake**, it adheres to the Medallion Architecture: **Bronze**, **Silver**, and **Gold** layers.

---

## 📂 Project Architecture

- 🔹 **Bronze Layer**:  
  Raw data ingestion of synthetic banking data (customers, transactions, branches, employee performance, and costs) using PySpark and stored in Delta format.

- 🔸 **Silver Layer**:  
  Cleansing and transformation of raw data — handling nulls, standardizing fields, applying filters, and joining datasets.

- 🏅 **Gold Layer**:  
  Analytical outputs and business KPIs for decision-making dashboards.

---

## 📊 KPIs Implemented in Gold Layer

1. **Customer Retention Rate**  
   Identifies active customers and retention patterns over a rolling window.

2. **Daily Transaction Volume Growth**  
   Measures daily change trends using window functions.

3. **Average Transaction Value**  
   Calculates the mean transaction value across branches.

4. **Branch Performance (Employee KPI-based)**  
   Aggregates employee performance metrics across branches and rates branches.

5. **Branch Profitability**  
   Calculates net profit by subtracting branch-level cost from revenue.

6. **Customer Segments**  
   Groups customers based on activity and transaction behavior.

7. **Customer Feedback Score**  
   Derived from employee ratings, linked back to customer satisfaction indicators.

---

## ⚠️ Data Quality Notes

> The synthetic dataset used in this project may contain **data discrepancies** like skewed distributions or missing values to simulate real-world inconsistencies.  
> Handling and correcting these is part of the transformation logic.

---

## 🛠️ Technologies Used

- **Apache Spark (PySpark)** – Distributed processing of large datasets  
- **Delta Lake** – ACID-compliant storage layer for Bronze/Silver/Gold medallion architecture  
- **Databricks Community Edition** – Development and interactive execution

---

## 🔮 Future Work

- ⏳ **Delta Loads (Incremental)** – Simulate Change Data Capture with timestamp-based filtering  
- 🧬 **SCD Type 2 Implementation** – Especially for customers or branches changing over time  
- 🧪 **Data Validation Framework** – Add Great Expectations or custom rule-based checks  
- 🧱 **Job Orchestration** – Schedule notebooks with Airflow or Databricks Jobs  
- 🔁 **Streaming Ingestion** – Add Structured Streaming for near real-time data updates
