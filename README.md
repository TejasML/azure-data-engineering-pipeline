# 🚖 NYC Yellow Taxi Data Engineering Pipeline

## 📌 Project Overview

This project demonstrates the design and implementation of an **end-to-end Azure Data Engineering Pipeline** using real-world **NYC Yellow Taxi Trip Data**. The solution follows the **Medallion Architecture (Bronze, Silver, Gold)** and automates the complete data ingestion and transformation process using **Azure Data Factory** and **Azure Databricks**.

The pipeline is designed to be **metadata-driven**, where a JSON configuration file controls which monthly datasets are ingested, making the solution scalable and easy to maintain.

---

## 🏗️ Architecture

```
NYC Taxi Website
        │
        ▼
HTTP Source
        │
        ▼
Azure Data Factory
        │
        ▼
Lookup Activity
        │
        ▼
ForEach Activity
        │
        ▼
Copy Activity
        │
        ▼
Azure Data Lake Storage Gen2
        │
        ▼
Azure Databricks
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer (Star Schema)
```

---

## ⚙️ Technologies Used

* Azure Data Factory (ADF)
* Azure Data Lake Storage Gen2 (ADLS Gen2)
* Azure Databricks
* Apache Spark (PySpark)
* Delta Lake
* GitHub
* JSON Configuration
* Medallion Architecture

---

## 📂 Data Source

Dataset: **NYC Yellow Taxi Trip Records**

The pipeline dynamically downloads monthly Parquet files from the NYC Taxi public dataset using a metadata-driven approach.

---

## 🔄 Metadata-Driven Ingestion

Instead of hardcoding file names, the pipeline reads a JSON configuration file.

Example:

```json
[
    {
        "year": "2025",
        "month": "07"
    },
    {
        "year": "2025",
        "month": "08"
    }
]
```

Azure Data Factory dynamically generates the source file names and downloads the required datasets.

This approach allows new data to be ingested by simply updating the configuration file without modifying the pipeline.

---

# 📥 Azure Data Factory Pipeline

The ADF pipeline performs the following tasks:

### 1. Lookup Activity

* Reads the JSON configuration file.

### 2. ForEach Activity

* Iterates through each year and month.

### 3. Copy Activity

* Downloads NYC Taxi Parquet files.
* Stores raw data in ADLS Gen2.

### 4. Databricks Bronze Notebook

* Loads raw data into Bronze Delta tables.

### 5. Databricks Silver Notebook

* Cleans and transforms the data.

### 6. Databricks Gold Notebook

* Creates analytical data models.

The entire process is automated and can be triggered with a single pipeline execution.

---

# 🥉 Bronze Layer

The Bronze layer stores raw ingested data with minimal transformation.

### Bronze Tables

* Yellow Taxi Trip Data
* Taxi Zone Lookup Data

### Operations

* Read raw Parquet files
* Store data as Delta tables
* Preserve original schema
* Maintain raw historical data

---

# 🥈 Silver Layer

The Silver layer focuses on data quality and transformation.

### Data Cleaning

* Removed invalid passenger counts
* Removed invalid trip distances
* Removed invalid fare amounts
* Removed invalid total amounts
* Removed negative trip durations

### Feature Engineering

Created additional analytical features:

* Pickup Year
* Pickup Month
* Pickup Day
* Pickup Hour
* Trip Duration (Minutes)

### Lookup Enrichment

Joined Taxi Zone Lookup table to generate:

* Pickup Borough
* Pickup Zone
* Pickup Service Zone
* Dropoff Borough
* Dropoff Zone
* Dropoff Service Zone

---

# 🥇 Gold Layer

The Gold layer is designed using a **Star Schema** for analytical workloads.

## Dimension Tables

### Dim Date

Contains calendar-related attributes.

### Dim Pickup Zone

Contains pickup location details.

### Dim Dropoff Zone

Contains dropoff location details.

### Dim Payment Type

Maps payment codes to descriptions.

---

## Fact Table

### Fact Trips

Contains:

* Trip Metrics
* Fare Information
* Passenger Count
* Trip Distance
* Date Key
* Pickup Zone Key
* Dropoff Zone Key
* Payment Type Key

---

# ⭐ Star Schema

```
                 Dim Date
                     │
                     │
Dim Pickup ─── Fact Trips ─── Dim Dropoff
                     │
                     │
             Dim Payment Type
```

The Gold layer is optimized for reporting and business intelligence workloads.

---

# 📊 Data Quality Checks

The project includes multiple validation steps:

* Null value analysis
* Schema validation
* Data type verification
* Invalid trip filtering
* Trip duration validation
* Gold layer verification

---

# 🚀 Pipeline Execution

The pipeline is fully automated.

```
Trigger Pipeline
        │
        ▼
Lookup
        │
        ▼
ForEach
        │
        ▼
Copy Activity
        │
        ▼
Bronze Notebook
        │
        ▼
Silver Notebook
        │
        ▼
Gold Notebook
```

---

# 📁 Project Structure

```
azure-data-engineering-pipeline/

├── azure-data-factory/
├── databricks/
│   ├── 01_Bronze_Layer.py
│   ├── 02_Silver_Layer.py
│   └── 03_Gold_Layer.py
│
├── config/
│   └── config.json
│
├── screenshots/
│
└── README.md
```

---

# ✨ Key Features

* End-to-End Azure Data Pipeline
* Metadata-Driven Ingestion
* Dynamic File Processing
* Medallion Architecture
* Delta Lake Storage
* Data Quality Validation
* Feature Engineering
* Star Schema Design
* Automated Orchestration with ADF
* Git Version Control

---

# 🎯 Learning Outcomes

Through this project, I gained hands-on experience with:

* Azure Data Factory
* Azure Databricks
* PySpark
* Delta Lake
* Data Lake Architecture
* Medallion Architecture
* Star Schema Modeling
* ETL/ELT Pipeline Design
* Metadata-Driven Pipelines
* Cloud Data Engineering Best Practices

---

## 📌 Future Enhancements

* Power BI Dashboard Integration
* Incremental Data Loading
* Delta MERGE Operations
* CI/CD Pipeline
* Automated Scheduling and Monitoring
* Production Deployment
