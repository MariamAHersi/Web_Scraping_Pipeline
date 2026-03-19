# Azure Data Pipeline & Analytics Project

## Overview
This project demonstrates the design and implementation of an end-to-end data pipeline using Microsoft Azure. The pipeline ingests raw data from a public GitHub source, processes and transforms it, and generates insights through analytics and visualisation tools.

The project showcases core data engineering concepts including data ingestion, transformation, storage, and reporting within a cloud-based architecture.

---

## Tech Stack
- Azure Data Lake Gen2
- Azure Databricks
- Azure Synapse Analytics
- Power BI

---

## Architecture
GitHub Dataset → Azure Data Lake Gen2 → Azure Databricks → Azure Data Lake Gen2 → Azure Synapse Analytics → Power BI

---

## Project Workflow

### 1. Data Ingestion
- Imported multiple raw datasets from a public GitHub repository
- Stored raw data in Azure Data Lake Gen2 for scalable storage

### 2. Data Processing
- Used Azure Databricks to clean and transform data
- Applied data cleaning, filtering, and aggregation techniques
- Structured the dataset for efficient querying and analysis

### 3. Data Storage
- Stored processed data back into Azure Data Lake Gen2
- Ensured data was organised and ready for downstream analytics

### 4. Data Visualisation
- Queried processed data using Azure Synapse Analytics
- Developed dashboards in Power BI to present insights
- Enabled clear communication of trends and key findings

---

## Key Features
- End-to-end ETL pipeline in a cloud environment
- Scalable data storage using Azure Data Lake
- Data transformation using distributed processing (Databricks)
- Interactive dashboards for data-driven decision-making

---

## Key Learnings
- Understanding of cloud-based data architecture
- Experience with ETL pipeline design and implementation
- Data transformation and preparation techniques
- Visualising data to support business insights

---

## Dataset
- Public dataset containing information on US legislators and executives
- Includes structured information such as names, roles, states, party affiliation, and tenure
- Used as raw input to demonstrate ingestion, transformation, and visualisation in a cloud-based pipeline

---

## Project Background
This project was initially developed as part of a university assignment. To deepen my understanding of data engineering workflows, I independently explored and reinforced each stage of the pipeline, gaining a broader end-to-end perspective.

---

## Future Improvements
- Automate pipeline scheduling and orchestration
- Expand dataset size and complexity
- Enhance dashboard interactivity and KPIs
