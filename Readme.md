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
- [ETL Pipeline Process](#-etl-pipeline-process)
  - [1. Extract & Transform](#1-extract--transform)
  - [2. Database Schema](#2-database-schema)
  - [3. Load & Orchestration](#3-load--orchestration)
- [Getting Started & Installation](#-getting-started--installation)
- [Verification & Result](#-verification--result)
- [Author](#-author)

---

## 📖 Overview

In modern data platform engineering, automating data ingestion from public REST APIs into structured data stores is a fundamental workflow. This project demonstrates how to:
1. Ingest unstructured JSON payloads from a public REST API.
2. Filter, clean, and map field properties using Python scripts.
3. Pass in-memory transformed state via Airflow XComs.
4. Execute DDL and DML operations dynamically on PostgreSQL using specialized Airflow Operators & Hooks.

---

## 🏗 Architecture & Workflow

```text
+-----------------------+      +-----------------------------------------------------+      +------------------------+
|   Open Library API    | ---> |        Apache Airflow (Docker Container)           | ---> |  PostgreSQL Database   |
| (REST API Search Endpoint)   |  [Task 1: Extract/Clean] -> [Task 2: Create Table]  |      |   (Table: `books`)     |
+-----------------------+      |             -> [Task 3: Insert Data]                |      +------------------------+
                               +-----------------------------------------------------+
