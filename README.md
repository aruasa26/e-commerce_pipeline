# E-Commerce PySpark ETL Pipeline

## Overview

This project implements a PySpark-based ETL pipeline that processes raw e-commerce datasets into a clean, enriched analytical layer.

The pipeline performs:
- Data ingestion with explicit schemas
- Data cleaning and standardisation
- Joins and feature engineering
- Window-based aggregations
- Return and refund analysis
- Data quality checks (DQ gates)
- Final outputs in Parquet and CSV formats

## Project Structure
```
e-commerce_pipeline/
│
├── data/                   # Input CSV files
├── output/                 # Final outputs
├── rejected/               # Rejected records
├── src/
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── enrichment.py
│   ├── aggregations.py
│   ├── returns_analysis.py
│   ├── output.py
│
├── tests/
│   ├── test_cleaning.py
│   ├── test_aggregations.py
│   ├── test_returns_analysis.py
│   ├── test_ingestion.py
│   ├── test_enrichment.py
│   ├── test_output.py
│
├── pipeline.py
├── README.md
├── requirements.txt
```

##  Setup Instructions

### 1. Clone the repository
git clone <repository-url>  
cd e-commerce_pipeline  

### 2. Create virtual environment 
python3 -m venv venv  
source venv/bin/activate  

### 3. Install dependencies
pip install -r requirements.txt  

### 4. Run the pipeline
python3 pipeline.py  

## What Happens During Execution
### 1. Data Ingestion
- Loads all CSV files   
- No schema inference is used  

### 2. Data Cleaning
- Removes exact duplicate rows  
- Normalises date formats to ISO (YYYY-MM-DD)  
- Standardises categorical fields 
- Casts numeric fields to correct types  
- Negative total_amount rows are not dropped, only flagged   

### 3. Data Quality Checks 
- Counts NULL customer_id values  
- Counts negative net_amount rows  
- Writes rejected records to separate output folders 
- Pipeline fails fast using exceptions if critical DQ checks fail

### 4. Joins & Enrichment
- Joins orders- customers- order_items using appropriate join types; LEFT JOIN used to preserve all orders 
- Computes derived metric: net_amount = total_amount * (1 - discount_pct / 100)

### 5. Window-Based Aggregations
- Customer lifetime ranking by country  
- 7-day rolling order count per customer  
- Monthly revenue share by product category  

## 6. Return Analysis
- Joins returns with enriched dataset  
- Computes return rates per category and customer tier  
- Identifies top refunding customers  
- Flags refund anomalies

## 7. Output Generation
- Writes final dataset to Parquet partitioned by year and month
- Writes aggregated results to CSV  
- Writes rejected records separately  
- All outputs use overwrite mode for reproducibility  

## Output Structure
```
output/  
 ├── final_parquet/  
 ├── ranked_customers/  
 ├── rolling_orders/  
 ├── revenue_share/  
 ├── category_return_rate/  
 ├── tier_return_rate/  
 ├── top_refund_customers/  

rejected/  
 ├── orders/  
 ├── order_items/  
 ├── returns/  
```
## Design Decisions

1. Explicit schema enforcement that ensures data consistency and prevents inference issues.

2. The pipeline is modular:
   ingestion.py- loading  
   cleaning.py- cleaning  
   enrichment.py- joins and feature engineering  
   aggregations.py- window functions  
   returns_analysis.py- return/refund logic  
   output.py- writing outputs  

3. Data quality gates are implemented for:
   - NULL customer IDs  
   - Negative net amounts (flagged, not dropped)  
   - Orphaned records  

4. All outputs use overwrite mode for reproducibility.

## Assumptions

- order_date values are consistently parseable  
- currency is uniform across datasets  
- returns reference valid order IDs  
- duplicate rows are exact duplicates  

## Limitations

- No external validation framework 
- Rolling window assumes consistent timestamp granularity  
- No currency conversion logic  
- No distributed fault tolerance or checkpointing  

## Key Features

- Schema enforcement  
- Data cleaning & standardisation  
- Left and anti joins  
- Window-based ranking and rolling metrics  
- Return rate analysis  
- Refund anomaly detection  
- Partitioned Parquet output  
