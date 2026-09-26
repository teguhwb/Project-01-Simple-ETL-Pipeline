# Project 1 — ETL Pipeline: Open Library API

## Overview

An end-to-end ETL pipeline that extracts book data from
the Open Library API, transforms the data using Python,
and loads the result into PostgreSQL.

The pipeline is orchestrated using Apache Airflow and
runs in a Docker environment.

## Architecture

Open Library API
       ↓
    Python
 Extract + Transform
       ↓
 Apache Airflow
       ↓
   PostgreSQL

All services run using Docker Compose.

## Tech Stack

- Python
- Apache Airflow
- PostgreSQL
- Docker
- Docker Compose
- REST API
- SQL

## ETL Process

### 1. Extract

Data is extracted from the Open Library API using Python
and the requests library.

Search keyword:

Data Engineering

The pipeline retrieves the first 10 books from the API response.

### 2. Transform

The raw API response is transformed into a structured format.

Selected fields:

- title
- author_name
- first_publish_year

The transformation also handles missing fields from the API response.

### 3. Load

The transformed data is inserted into a PostgreSQL table named:

books

## Airflow DAG

The pipeline consists of three tasks:

Extract & Clean
       ↓
Create Table
       ↓
Insert Data

## Docker

The project uses Docker Compose to run:

- Apache Airflow
- PostgreSQL

## Result

The pipeline successfully retrieves book data from the Open Library API,
transforms the response, and stores the result in PostgreSQL.
