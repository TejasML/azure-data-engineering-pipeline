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

nyc-taxi-lakehouse-azure/
│
├── README.md
│
├── architecture/
│   ├── solution_architecture.png
│   └── star_schema.png
│
├── azure-data-factory/
│   ├── dataset/
│   ├── factory/
│   ├── linkedService/
│   └── pipeline/
│
├── config/
│   └── months.json
│
├── databricks/
│   ├── bronze_notebook.py
│   ├── silver_notebook.py
│   └── gold_notebook.py
│
├── sql/
│   └── nyc_taxi_queries.sql
│
└── project-assets/
  ├── adf/
  ├── databricks/
  └── powerbi/
```
2. Azure Data Factory Orchestration

ADF orchestrates the entire workflow.

Pipeline Flow
Read metadata from months.json
Iterate through each month using ForEach activity
Download NYC Taxi data through HTTP source
Store files in ADLS Gen2 Raw Landing Layer
Execute Databricks Bronze Notebook
Execute Databricks Silver Notebook
Execute Databricks Gold Notebook
Pipeline Screenshot




3. Linked Services

The following Linked Services were configured:

LS_NYC_TAXI_SOURCE

HTTP connection used to access NYC Taxi source data.

LS_ADLS_GEN2

Connection to Azure Data Lake Storage Gen2.

LS_AZURE_DATABRICKS

Connection between Azure Data Factory and Azure Databricks.

Linked Services




4. Azure Data Lake Storage Gen2

The data lake is organized using multiple containers:

raw-landing
bronze
silver
gold
Raw Landing

Stores source files exactly as received.

Bronze Layer

Stores ingested raw data.

Silver Layer

Stores cleansed and transformed datasets.

Gold Layer

Stores business-ready analytical datasets.

Storage Containers




5. Azure Key Vault & Databricks Secret Scope

To ensure secure credential management, Azure Key Vault was integrated with Databricks.

Why Azure Key Vault?
Eliminates hardcoded credentials
Centralized secret management
Improved security and governance
Simplified credential rotation
Databricks Secret Scope

A Databricks Secret Scope was created and linked with Azure Key Vault.

The notebooks securely retrieve credentials using the Secret Scope rather than storing sensitive information in code.

6. Azure Databricks Processing

ADF triggers Databricks notebooks in sequence:

Bronze Notebook
      ↓
Silver Notebook
      ↓
Gold Notebook
Notebook Structure
01_bronze_ingestion
02_silver_transformation
03_gold_transformation
Databricks Workspace




7. Databricks Cluster Configuration

A dedicated cluster was created for data processing.

Cluster Configuration
Property	Value
Cluster Name	dataeng-cluster
Runtime	Databricks Runtime 17.3 LTS
Node Type	Standard_D4ds_v4
Memory	16 GB
Cores	4
Cluster Type	Single Node
Auto Termination	15 Minutes
Unity Catalog	Enabled
Cluster Screenshot




8. Databricks Jobs

Azure Data Factory triggers Databricks jobs for Bronze, Silver, and Gold processing.

Job Monitoring




9. Spark Processing

The Silver Layer transformation uses Apache Spark and Delta Lake.

The Spark execution plan demonstrates:

Parquet scanning
Shuffle operations
WholeStageCodegen optimization
Distributed processing
Delta file writing
Spark Execution Plan




10. Medallion Architecture
Bronze Layer
Raw ingestion
Historical storage
Minimal transformations
Silver Layer
Data cleansing
Data validation
Standardization
Feature engineering
Gold Layer
Analytical modeling
Fact and dimension tables
Business-ready datasets
11. Data Modeling

The Gold Layer follows a Star Schema design.

Fact Table

fact_trips

Dimension Tables
dim_date
dim_pickup_zone
dim_dropoff_zone
dim_payment_type
Measures
Trip Distance
Fare Amount
Total Amount
Passenger Count
Tip Amount
Trip Duration
12. Power BI Integration

Power BI connects directly to Databricks SQL Warehouse to provide analytical reporting.

Reporting Features
Executive Overview
Trip Analysis
Revenue Analysis
Payment Analysis
Pickup & Dropoff Insights
Connection Flow
Gold Layer
      ↓
Databricks SQL Warehouse
      ↓
Power BI
Cost Analysis

The project was developed using Azure for Students credits.

Total Cost

₹1,269.56

Major Cost Contributors
NAT Gateway
Azure Databricks
Virtual Machines
Virtual Network
Cost Optimization Techniques
Single-node cluster configuration
Auto-termination after 15 minutes
Metadata-driven processing
Delta Lake storage optimization
Cost Dashboard




Key Features
End-to-End Azure Data Engineering Pipeline
Metadata-Driven Data Ingestion
Azure Data Factory Orchestration
Azure Key Vault Integration
Databricks Secret Scope Configuration
Medallion Architecture
Delta Lake Implementation
Spark-Based Data Processing
Star Schema Modeling
Databricks SQL Warehouse
Power BI Reporting
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


## Power BI
* Build a interactive dashbord using powerbi to demostrate the 
