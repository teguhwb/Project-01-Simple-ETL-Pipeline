# 📚 Project 1 — End-to-End ETL Pipeline: Open Library API

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.2-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

An end-to-end Automated Data Engineering Pipeline that extracts book data from the **Open Library Search API**, performs data cleaning & transformation using Python, and loads the structured data into a **PostgreSQL** database. 

The entire workflow is orchestrated using **Apache Airflow (DAG)** and fully containerized with **Docker Compose**.

---

## 📌 Table of Contents
- [Overview](#-overview)
- [Architecture & Workflow](#-architecture--workflow)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
* [ETL Pipeline Process](#%EF%B8%8F-etl-pipeline-process)
  - [1. Extract & Transform](#1-extract--transform)
  - [2. Database Schema](#2-database-schema)
  - [3. Load & Orchestration](#3-load--orchestration)
- [Getting Started & Installation](#-getting-started--installation)
- [Verification & Result](#-verification--result)


---
📌 **Table of Contents**


## 📖 Overview

In modern data platform engineering, automating data ingestion from public REST APIs into structured data stores is a fundamental workflow. This project demonstrates how to:
1. Ingest unstructured JSON payloads from a public REST API.
2. Filter, clean, and map field properties using Python scripts.
3. Pass in-memory transformed state via Airflow XComs.
4. Execute DDL and DML operations dynamically on PostgreSQL using specialized Airflow Operators & Hooks.

---

## 🏗 Architecture & Workflow

<img width="1024" height="119" alt="8ff4c4d4-1e95-4fba-aec6-4223b2cee931" src="https://github.com/user-attachments/assets/c68ccddc-c93d-49be-9568-0cb607b860e3" />

---
## 🛠 Tech Stack

| Category | Technology | Usage Description |
| :--- | :--- | :--- |
| **Language** | Python 3.12 | Extraction script, payload parsing, array slicing |
| **Orchestrator** | Apache Airflow 2.9.2 | DAG scheduling, task dependency management, XCom |
| **Database** | PostgreSQL 13 | Target relational database storage |
| **Containerization** | Docker & Docker Compose | Container orchestration & environment virtualization |
| **Libraries** | `requests`, `airflow.providers.postgres` | HTTP handling and Database Connection Hooks |

---

## 📂 Project Structure

```text
Project-01-Simple-ETL-Pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── dag_project_de_etl_v04.py    # Main Airflow DAG workflow definition
│   └── files/
│       └── create_table.sql             # PostgreSQL table schema DDL script
│
├── .gitignore                           # Git ignore rules for virtualenv & logs
├── compose.yml                          # Docker Compose configuration for Airflow & Postgres
├── extract_and_cleaning_data.py         # Standalone Python ETL prototyping script
└── README.md                            # Comprehensive project documentation
```
---
## ⚙️ ETL Pipeline Process

### 1. Extract & Transform
The script fetches data using `requests.get()`, retrieves top 10 books based on the query *Data Engineering*, and cleans missing or deeply nested fields:

```python
def extract_and_cleaning_data(ti):
    query = "Data Engineering"
    url = f"[https://openlibrary.org/search.json?q=](https://openlibrary.org/search.json?q=){query.replace(' ', '+')}"
    response = requests.get(url)
    data = response.json()

    books = []
    for book in data['docs'][:10]:
        books.append({
            'title': book.get('title'),
            'author_name': book.get('author_name')[0] if book.get('author_name') else 'Unknown',
            'first_publish_year': book.get('first_publish_year')
        })
    ti.xcom_push(key='book_data', value=books)
```

### 2. Database Schema
Target table schema executed by PostgresOperator:

```SQL

CREATE TABLE IF NOT EXISTS books (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    author_name TEXT,
    first_publish_year TEXT
);

```

### 3. Load & Orchestration
Airflow manages task order and passes state between tasks via XCom:

```python
task_1 = PythonOperator(
    task_id='extract_cleaning_data', 
    python_callable=extract_and_cleaning_data, 
    dag=dag
)

task_2 = PostgresOperator(
    task_id='create_table', 
    postgres_conn_id='books_connection', 
    sql="./files/create_table.sql", 
    dag=dag
)

task_3 = PythonOperator(
    task_id='insert_data', 
    python_callable=insert_data, 
    dag=dag
)

## Task Dependencies
task_1 >> task_2 >> task_3

```
---
## 🚀 Getting Started & Installation

### Prerequisites
* **Docker Desktop** installed and running.
* **Git** installed.
* **Python 3.12** (optional, for local testing outside Docker).

---

### Step-by-Step Setup

1. **Clone Repository**
   ```bash
   git clone [https://github.com/teguhwb/Project-01-Simple-ETL-Pipeline.git](https://github.com/teguhwb/Project-01-Simple-ETL-Pipeline.git)
   cd Project-01-Simple-ETL-Pipeline
   ```
2. **Setup Local Virtual Environment (Optional / Local Testing)**
```bash
python -m venv myvenv

# Windows
myvenv\Scripts\activate

# Linux/MacOS
source myvenv/bin/activate

```
3. **Run Services with Docker Compose**
```bash
docker compose up -d
```
4. **Access Airflow UI**
   - **Open browser:** `http://localhost:8080`
   - **Default Username:** `admin`
   - **Default Password:** `check container logs or standalone login file`

5. **Configure Airflow Connection**
   - Go to **Admin -> Connections -> Add connection**:
     - **Conn Id:** `books_connection`
     - **Conn Type:** `Postgres`
     - **Host:** `postgres`
     - **Database:** `airflow`
     - **Login:** `airflow`
     - **Password:** `airflow`
     - **Port:** `5432`

6. **Trigger DAG**
   - Unpause DAG `dag_project_de_etl_v04` and click **Trigger DAG**.
---
## ✅ Verification & Result

After running the Airflow DAG successfully:
* **Airflow DAG Run:** All tasks (`extract_cleaning_data`, `create_table`, `insert_data`) complete with status **Success** (Green).
* **PostgreSQL Data Verification:** Execute inside PostgreSQL container:
```bash
  docker exec -it <postgres_container_id> psql -U airflow -d airflow -c "SELECT * FROM books;"
```
  **Output Sample:**

| id | title | author_name | first_publish_year |
| :--- | :--- | :--- | :--- |
| 1 | Fundamentals of Data Engineering | Joe Reis | 2022 |
| 2 | Designing Data-Intensive Applications | Martin Kleppmann | 2017 |

---

## 👨‍💻 Author
**Teguh Wibowo** — [GitHub Profile](https://github.com/teguhwb)
