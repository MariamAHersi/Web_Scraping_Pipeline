# Web Scraping & Data Engineering Pipeline Project

## Overview
This project demonstrates an end-to-end data engineering pipeline that includes web scraping, data processing, database management, and automated data ingestion.

The pipeline extracts data from an e-commerce website, cleans and structures it, and stores it across both relational (MySQL) and NoSQL (MongoDB) databases. It also incorporates workflow automation using Apache NiFi.

This project highlights key data engineering concepts such as data collection, transformation, storage, and pipeline orchestration.

---

## Tech Stack
- Python (BeautifulSoup, Pandas)
- MySQL (Relational Database)
- MongoDB (NoSQL Database)
- Apache NiFi (Data Pipeline Automation)

---
## Project Structure 
├── .venv/

├── data/
│   ├── raw/
│   └── processed/

├── database/
│   ├── queries.py
│   └── schema.py

├── docs/
│   └── report_final.pdf

├── outputs/

├── pipelines/
│   └── extracted_data_*.json

├── scripts/
│   ├── cleaning.py
│   ├── ingestion_*.py
│   └── .ipynb_checkpoints/

├── README.md


---

## Project Workflow

### 1. Web Scraping
- Scraped data engineering book listings from an e-commerce website (Packt Publishing)  
- Extracted key attributes:
  - Title  
  - Author(s)  
  - Publication year  
  - Rating  
  - Price  
- Stored raw data in `data/raw/` as CSV files  

---

### 2. Data Cleaning & Transformation
- Processed raw data using Python (`scripts/cleaning.py`)  
- Performed:
  - Handling missing values  
  - Data formatting and type conversion  
  - Removing duplicates and inconsistencies  
- Saved cleaned datasets in `data/processed/`  

---

### 3. Relational Database (MySQL)
- Designed schema using `database/schema.py`  
- Imported processed data into MySQL tables  
- Ensured:
  - Proper normalization  
  - Efficient querying structure  

---

### 4. Data Analysis
- Wrote SQL queries in `database/queries.py`  
- Performed analysis such as:
  - Pricing trends  
  - Rating distributions  
  - Author and publication insights  
- Stored outputs in `outputs/`  

---

### 5. Data Pipeline Automation (Apache NiFi)
- Built automated workflows to:
  - Ingest data from files  
  - Route data between systems  
- Pipeline outputs stored in `pipelines/`  

---

### 6. MongoDB Integration
- Stored semi-structured data in MongoDB  
- Demonstrated flexibility compared to relational databases  
- Enabled handling of varying data formats  

---

## Key Features
- End-to-end data pipeline from scraping to storage and analysis  
- Modular project structure for scalability and maintainability  
- Web scraping using Python and BeautifulSoup  
- Dual database integration (MySQL + MongoDB)  
- Automated data ingestion using Apache NiFi  
- Clear separation of raw and processed data  

---

## Key Learnings
- Web scraping and real-world data extraction  
- Data cleaning and preprocessing techniques  
- Relational vs NoSQL database design  
- Writing efficient SQL queries for analysis  
- Building automated data pipelines with Apache NiFi  
- Structuring a scalable data engineering project  

---

## Dataset
- Scraped dataset of ~200 data engineering books from Packt Publishing  
- Includes:
  - Book title  
  - Author(s)  
  - Publication year  
  - Star rating  
  - Price  
- Stored in CSV format and used across both MySQL and MongoDB  

---

## Project Background
This project was initially developed as part of a group assignment. To deepen my understanding, I independently extended the project to cover the full data pipeline, including ingestion, transformation, and multi-database integration.

---

## Future Improvements
- Automate the web scraping process (scheduled scraping)  
- Add data validation and error handling  
- Expand dataset size and variety  
- Implement logging and monitoring for pipelines  
- Enhance analytics with advanced SQL queries and dashboards  
