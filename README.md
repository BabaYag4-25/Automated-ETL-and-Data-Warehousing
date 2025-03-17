# Automated ETL and Data Warehousing with PySpark and Apache Airflow

## Overview
This project automates the **ETL (Extract, Transform, Load)** process using **PySpark** and schedules data pipeline execution with **Apache Airflow**. The processed data is stored in **MongoDB** for further analysis.

## Features
- **Extract:** Reads raw financial data from CSV files.
- **Transform:** Cleans and standardizes data using PySpark.
- **Load:** Stores cleaned data into a MongoDB database.
- **Automation:** Uses Apache Airflow to schedule and automate ETL processes.

## Project Structure
- **`extract_transform.py`**: Extracts raw data and applies cleaning and transformation using PySpark.
- **`load.py`**: Loads the transformed data into MongoDB.
- **`Directed_Acyclic_Graph.py`**: Defines Airflow DAG for scheduling ETL execution.
- **`data_cleaned_gx.csv`**: Processed and cleaned data.
- **`data_raw.csv`**: Raw dataset before processing.

## Installation & Setup
1. Clone this repository:
   ```sh
   git clone <repository-url>
   ```
2. Install required dependencies:
   ```sh
   pip install pyspark apache-airflow pymongo pandas
   ```
3. Start **Airflow Scheduler**:
   ```sh
   airflow scheduler
   ```
4. Start **Airflow Webserver**:
   ```sh
   airflow webserver -p 8080
   ```
5. Trigger the DAG manually from the Airflow UI or let it run on schedule.

## How It Works
1. **Extract:** Reads raw CSV data into a Spark DataFrame.
2. **Transform:** Cleans column names, converts dates, and processes numerical values.
3. **Load:** Saves transformed data into MongoDB.
4. **Scheduling:** Airflow DAG executes ETL tasks every Saturday from 09:10 AM to 09:30 AM in 10-minute intervals.

## Results & Benefits
- Fully automated data pipeline.
- Scalable ETL process using **PySpark**.
- Centralized data storage in **MongoDB** for easy querying and analysis.

## Author
Developed by Ahmad Ghifari as part of a data engineering milestone project using Apache Airflow and PySpark.


