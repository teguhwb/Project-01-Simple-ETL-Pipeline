📚 Project 1 — End-to-End ETL Pipeline: Open Library API








An end-to-end automated Data Engineering ETL Pipeline that extracts book data from the Open Library Search API, performs data cleaning and transformation using Python, and loads the structured data into a PostgreSQL database.

The entire workflow is orchestrated using Apache Airflow and fully containerized with Docker Compose.

📌 Table of Contents

Overview

Architecture & Workflow

Tech Stack

Project Structure

ETL Pipeline Process

1. Extract

2. Transform

3. Airflow XCom

4. Database Schema

5. Load

Getting Started & Installation

Prerequisites

Clone Repository

Optional Local Environment

Run Docker Compose

Access Airflow

Configure Airflow Connection

Trigger DAG

Verification & Result

Author

📖 Overview

In modern data platform engineering, automating data ingestion from public REST APIs into structured data stores is a fundamental workflow.

This project demonstrates how to:

Ingest unstructured JSON payloads from a public REST API.

Filter, clean, and map API fields using Python.

Pass transformed data between Airflow tasks using XCom.

Dynamically execute DDL and DML operations on PostgreSQL.

Orchestrate the entire ETL workflow using an Apache Airflow DAG.

Run the complete data pipeline inside Docker containers.

🏗 Architecture & Workflow
+-----------------------+
|   Open Library API    |
| REST API Search       |
+-----------+-----------+
            |
            | JSON Response
            v
+-----------------------------------------------------+
|              Apache Airflow                         |
|                                                     |
|  +---------------------+                            |
|  | Task 1              |                            |
|  | Extract & Transform |                            |
|  +----------+----------+                            |
|             |                                       |
|             | XCom                                  |
|             v                                       |
|  +---------------------+                            |
|  | Task 2              |                            |
|  | Create Table        |                            |
|  +----------+----------+                            |
|             |                                       |
|             v                                       |
|  +---------------------+                            |
|  | Task 3              |                            |
|  | Insert Data         |                            |
|  +----------+----------+                            |
+-------------|---------------------------------------+
              |
              | SQL INSERT
              v
+---------------------------+
|    PostgreSQL Database    |
|                           |
|       Table: books        |
+---------------------------+

Workflow

The pipeline follows this sequence:

Open Library API
       ↓
Extract & Clean Data
       ↓
Airflow XCom
       ↓
Create PostgreSQL Table
       ↓
Insert Transformed Data
       ↓
PostgreSQL

Flow Explanation

API Extraction
Airflow retrieves book information from the Open Library Search API.

Data Transformation
The JSON response is filtered and transformed into a clean list of dictionaries.

XCom Data Transfer
The transformed data is temporarily passed between Airflow tasks using XCom.

Table Initialization
PostgreSQL table books is created using a SQL DDL script.

Data Loading
The transformed records are inserted into PostgreSQL using PostgresHook.

🛠️ Tech Stack
Category	Technology	Usage
Language	Python 3.12	Data extraction, transformation, and API handling
Orchestrator	Apache Airflow 2.9.2	DAG orchestration and task dependency management
Database	PostgreSQL 13	Structured data storage
Containerization	Docker	Application containerization
Orchestration	Docker Compose	Running Airflow and PostgreSQL services
HTTP Client	requests	Calling Open Library API
Airflow Provider	airflow.providers.postgres	PostgreSQL integration
State Management	Airflow XCom	Passing data between tasks
📂 Project Structure
Project-01-Simple-ETL-Pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── dag_project_de_etl_v04.py
│   │       # Main Airflow DAG workflow definition
│   │
│   └── files/
│       └── create_table.sql
│           # PostgreSQL table schema / DDL
│
├── .gitignore
│   # Git ignore rules
│
├── compose.yml
│   # Docker Compose configuration
│
├── extract_and_cleaning_data.py
│   # Standalone Python ETL prototype
│
└── README.md
    # Project documentation

⚙️ ETL Pipeline Process
1. Extract

Data is retrieved from the Open Library Search API:

https://openlibrary.org/search.json?q=Data+Engineering


The Python requests library is used to send an HTTP request and retrieve the JSON response.

Example:

import requests

url = "https://openlibrary.org/search.json?q=Data+Engineering"

response = requests.get(url)
data = response.json()


The pipeline limits the result to the first 10 books.

2. Transform

The raw JSON response is transformed into a clean list of dictionaries.

The pipeline extracts the following fields:

title

author_name

first_publish_year

Example transformed structure:

[
    {
        "title": "Fundamentals of Data Engineering",
        "author_name": "Joe Reis",
        "first_publish_year": 2022
    },
    {
        "title": "Designing Data-Intensive Applications",
        "author_name": "Martin Kleppmann",
        "first_publish_year": 2017
    }
]

Data Mapping
Open Library API	PostgreSQL	Data Type	Description
title	title	TEXT NOT NULL	Book title
author_name[0]	author_name	TEXT	First author
first_publish_year	first_publish_year	TEXT	First publication year
Missing Values

If author_name is missing or empty, the pipeline uses:

Unknown


as the default author value.

3. Airflow XCom

After transformation, the cleaned data is pushed into Airflow XCom:

ti.xcom_push(
    key="book_data",
    value=cleaned_data
)


The next task retrieves the data using:

book_data = ti.xcom_pull(
    task_ids="extract_cleaning_data",
    key="book_data"
)


This allows the transformed data to be passed from the extraction task to the loading task without using an intermediate file.

4. Database Schema

The PostgreSQL table is created using:

airflow/files/create_table.sql


Example schema:

CREATE TABLE IF NOT EXISTS books (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    author_name TEXT,
    first_publish_year TEXT
);


The create_table Airflow task executes this SQL script before the data insertion task runs.

5. Load

The transformed data is loaded into PostgreSQL using PostgresHook.

The Airflow connection used by the DAG is:

books_connection


The general workflow is:

XCom
  ↓
Retrieve transformed data
  ↓
PostgresHook
  ↓
PostgreSQL Connection
  ↓
INSERT INTO books


The DAG task dependency is:

extract_cleaning_data
          ↓
    create_table
          ↓
      insert_data

🚀 Getting Started & Installation
1. Prerequisites

Make sure the following tools are installed:

Docker Desktop

Git

Python 3.12 (optional, for local testing outside Docker)

2. Clone Repository

Clone the repository:

git clone https://github.com/teguhwb/Project-01-Simple-ETL-Pipeline.git


Navigate into the project directory:

cd Project-01-Simple-ETL-Pipeline

3. Optional Local Environment

If you want to test the Python script locally, create a virtual environment:

Windows
python -m venv myvenv


Activate it:

myvenv\Scripts\activate

Linux / macOS
python3 -m venv myvenv


Activate it:

source myvenv/bin/activate

4. Run Docker Compose

Start all services:

docker compose up -d


Check running containers:

docker compose ps


To view logs:

docker compose logs -f

5. Access Airflow

Open your browser and navigate to:

http://localhost:8080


Default Airflow credentials depend on the Docker Compose configuration.

If credentials are not explicitly configured, check the container logs:

docker compose logs airflow

6. Configure Airflow Connection

In the Airflow UI:

Admin → Connections → Add Connection


Configure the PostgreSQL connection as follows:

Field	Value
Connection Id	books_connection
Connection Type	Postgres
Host	postgres
Database	airflow
Login	airflow
Password	airflow
Port	5432

Note: These values should match the PostgreSQL configuration defined in compose.yml.

7. Trigger DAG

In the Airflow UI:

Find the DAG:

dag_project_de_etl_v04


Enable / unpause the DAG.

Click Trigger DAG.

Wait until all tasks show a green Success status.

Expected task flow:

extract_cleaning_data
          ↓
    create_table
          ↓
      insert_data

✅ Verification & Result

After the DAG finishes successfully, verify the data inside PostgreSQL.

First, identify the PostgreSQL container:

docker compose ps


Then access PostgreSQL:

docker exec -it <postgres_container_id> \
psql -U airflow -d airflow


Run:

SELECT * FROM books;


Or execute it directly:

docker exec -it <postgres_container_id> \
psql -U airflow -d airflow \
-c "SELECT * FROM books;"

Expected Result

Example output:

 id | title                                      | author_name       | first_publish_year
----+--------------------------------------------+-------------------+-------------------
  1 | Fundamentals of Data Engineering           | Joe Reis          | 2022
  2 | Designing Data-Intensive Applications      | Martin Kleppmann  | 2017

Airflow Verification

The DAG should show all tasks with:

SUCCESS


Expected pipeline status:

┌─────────────────────────┐
│ extract_cleaning_data   │
│         SUCCESS         │
└────────────┬────────────┘
             ↓
┌─────────────────────────┐
│      create_table       │
│         SUCCESS         │
└────────────┬────────────┘
             ↓
┌─────────────────────────┐
│       insert_data       │
│         SUCCESS         │
└─────────────────────────┘

🎯 Project Goals

This project was built to demonstrate practical implementation of:

REST API data ingestion

JSON data processing

Data cleaning and transformation

ETL pipeline development

Apache Airflow DAG orchestration

Airflow XCom

PostgreSQL data loading

SQL DDL and DML

Docker containerization

Docker Compose

Python-based data engineering workflows

👨‍💻 Author

Teguh Wibowo

GitHub:
github.com/teguhwb

📄 License

This project is intended for educational and portfolio purposes.
