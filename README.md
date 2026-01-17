## 🏨 End-to-End Hotel Analytics Project on Snowflake

### 📌 Project Overview

This project demonstrates a **complete, hands-on Snowflake analytics pipeline** built using real-world hotel booking data. The goal was to design a **scalable data architecture**, perform **data quality checks and transformations**, and deliver **business-ready insights through SQL-based dashboards**.

The project follows the **Medallion Architecture (Bronze → Silver → Gold)** and uses **Snowflake-native features only**, without external ETL tools.

---

### 🏗️ Architecture Overview

![Architecture](architecture_snowflake.png)

The solution is designed using a modern cloud analytics pattern:

* **Data Source**
  Raw hotel booking data available as CSV files

* **Snowflake Platform**
  Acts as the single system for:

  * Storage
  * Data processing
  * Analytics
  * Visualization

* **Medallion Layers**

  * **Bronze (Raw Data)**

    * Data loaded as-is using **Snowflake Stages**
    * Ingestion via `COPY INTO`
    * No transformations applied
  * **Silver (Cleaned Data)**

    * Data quality issues identified and fixed
    * Null handling, type corrections, invalid records filtered
    * Standardized schema applied
  * **Gold (Business-Ready Data)**

    * KPI and analytics-focused tables created
    * Optimized for dashboard consumption

* **Visualization Layer**

  * Built using **Snowflake SQL Dashboards**
  * Real-time analytical views powered directly from Gold tables

---

### 📥 Data Ingestion (Bronze Layer)

* CSV file uploaded via **Snowflake Internal Stage (Browser Upload)**
* Data previewed before loading
* Ingestion performed using:

```sql
COPY INTO bronze_hotel_booking
FROM @stg_hotel_bookings
FILE_FORMAT = (FORMAT_NAME = ff_csv)
ON_ERROR = 'CONTINUE';
```

This ensures:

* Idempotent loading (files loaded only once)
* Fault-tolerant ingestion
* Safe re-runs without duplication

---

### 🧹 Data Cleaning & Transformation (Silver Layer)

Key data issues identified and resolved:

* Missing or invalid booking dates
* Inconsistent room type values
* Incorrect revenue calculations
* Duplicate booking records
* Invalid guest counts

All transformations were implemented using **pure SQL**, ensuring transparency and auditability.

---

### 📊 Analytics Modeling (Gold Layer)

Created **three business-focused tables** to support analytics and reporting:

* KPI summary table
* Monthly performance metrics
* Dimensional aggregations for slicing and dicing

These tables are optimized for:

* Fast dashboard queries
* Clear business semantics
* Minimal downstream logic

---

### 📈 Dashboard & Insights

*(Refer to the attached dashboard image)*

The Snowflake SQL Dashboard provides:

* **Total Revenue**
* **Average Booking Value**
* **Total Guests**
* **Total Bookings**
* Monthly bookings trend
* Monthly revenue trend
* Top-performing cities
* Booking status distribution
* Room type performance

The dashboard updates dynamically based on the underlying Gold tables, enabling **near real-time business insights**.

![dashboard](final_dashboard.png)


---

### 🛠️ Tools & Technologies Used

* Snowflake (Stages, COPY INTO, SQL Worksheets, Dashboards)
* SQL (Transformations, Aggregations, KPIs)
* Medallion Architecture (Bronze / Silver / Gold)

---

### 🎯 Key Takeaways

* Demonstrates full ownership of the data lifecycle
* Uses Snowflake as an end-to-end analytics platform
* Applies industry-standard architecture patterns
* Converts raw data into actionable business insights

This project reflects how Snowflake can be used in **real production-grade analytics workflows** with minimal tooling and maximum efficiency.
