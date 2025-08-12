# AnalyticsVault
A data warehouse built on the Medallion architecture, implemented in SQL Server with a layered Bronze–Silver–Gold design. The solution integrates ETL processes to consolidate and clean data, and delivers interactive analytics dashboards via Power BI for business insights.

## 📑 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Folder Structure](#folder-structure)
- [Setup Instructions](#setup-instructions)
- [ETL Process](#etl-process)
- [Data Models](#data-models)
- [Visualization](#visualization)
- [Contributors](#contributors)

---

## 📌 Overview
This project implements a **modern analytics data warehouse** designed to:
- Consolidate data from multiple sources
- Clean and standardize information using ETL pipelines
- Store data in a structured format for analytics
- Deliver insights via interactive Power BI dashboards

The warehouse follows the **Medallion architecture** to ensure scalability, maintainability, and data quality.

---

## 🏛 Architecture
The Medallion architecture organizes the data pipeline into three layers:

1. **Bronze Layer** – Raw data ingestion from source systems  
2. **Silver Layer** – Cleaned and transformed data  
3. **Gold Layer** – Aggregated and analytics-ready datasets  

![High-Level Architecture](diagrams/high_level_architecture.png)

---

## 🛠 Technologies
- **Database**: SQL Server  
- **ETL**: [Specify ETL tool, e.g., SSIS / Azure Data Factory / Python scripts]  
- **Visualization**: Power BI  
- **Version Control**: Git & GitHub  
