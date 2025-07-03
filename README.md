# airflow-data-orchestration

# **Designing Data Pipeline using Airflow**

## Content
[1. Project Description](#1-project-description)  
[2. Source of Dataset](#2-source-of-dataset)  
[3. Tech Stack](#3-tech-stack)  
[4. Pipeline Design](#4-pipeline-design)  
[5. Project Setup](#5-project-setup)  

---
# 1. Project Description
This project develops a well-structured and automated data pipeline tailored for a flight booking system by leveraging Apache Airflow. The pipeline is designed to orchestrate the end-to-end flow of data — starting from the extraction of records from a PostgreSQL source database, transferring and storing the raw data into a MinIO object storage system, and subsequently loading it into a data warehouse environment. Once ingested, the data undergoes transformation processes to generate structured analytical tables that are optimized for reporting and insights.

The enhanced version of this pipeline introduces several advanced features and architectural improvements. It now supports incremental data loading, allowing only new or updated records to be processed, which significantly improves efficiency and scalability. Additionally, the pipeline utilizes Jinja templating for dynamic DAG configurations, and employs Airflow’s XComs for task-to-task communication and metadata passing. Real-time monitoring is also enabled through Slack alerting, ensuring that stakeholders are promptly informed of pipeline status or failures. The pipeline further includes dynamic task generation to accommodate varying input patterns and modular configuration via command-line interfaces (CLI), which enhances its flexibility, maintainability, and ease of deployment across different environments.

---

# 2. Source of Dataset
The dataset used on this repo comes from airlines data management system that handle flight bookings of customers. you can access the repo on this [link](https://drive.google.com/file/d/1Zu5grD6mKuCcLagggE9R8ILjzvPIbXDQ/view?usp=sharing)

---

# 3. Tech Stack  
- Database: PostgresQL
- Containerization: Docker
- Object Storage: MinIO
- Orchestrator: Airflow
- Notifier: Slack Webhook

---

# 4. Pipeline Design
![image](https://github.com/user-attachments/assets/5325515e-e8d6-4b0d-b723-272c7b5bb7a9)

# 5. Project Setup
Clone this repository
```
https://github.com/ibnufajar1994/airflow-data-orchestration.git
```



create .env file on your project directory
```
# Airflow internal config
AIRFLOW_FERNET_KEY=    # Run fernet.py to generate this (see below)
AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow_metadata/airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

# Warehouse (staging and dimensional)
WAREHOUSE_DB_USER=postgres
WAREHOUSE_DB_PASSWORD=postgres
WAREHOUSE_DB_NAME=warehouse_pacflight

# Source DB (bookings)
PACFLIGHT_DB_USER=postgres
PACFLIGHT_DB_PASSWORD=postgres
PACFLIGHT_DB_NAME=pacflight

# MinIO
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
```

Generate fernet key, then copy it to your .env file
```
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

run the project
```
docker-compose up --build
```

Load Variables and Connection Via CLI of airflow container:

```
docker exec -it airflow_standalone bash
```
---
```
airflow variables import include/variables.json
```
---
```
airflow connections import include/connections.yaml
```
---

# Documentation

## DAG Sample
![DAG Sample](sample/1.dag-sample.png)

## DAG Graph
![DAG Graph](sample/2.dag-graph.png)

## XComs
![XComs](sample/3.Xcoms.png)

## Skip Exception
![Skip Exception](sample/4.skip-exception.png)

## Sample Warehouse Database
![Sample Warehouse Database](sample/5.sample-warehouse-database.png)

## Sample Queries
![Sample Queries](sample/6.sample-queries.png)


  

