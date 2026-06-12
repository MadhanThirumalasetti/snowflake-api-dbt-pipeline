# Snowflake API Ingestion & dbt Transformation Platform

## Overview
A production-grade Python framework that ingests data from 15+ REST API endpoints into Snowflake and transforms it into analytics-ready dimensional models using dbt.

## Problem It Solves
Business teams were manually preparing data for analysis every day — slow, error-prone, and not scalable. This pipeline eliminated 100% of that manual effort by automating ingestion, transformation, and quality checks end to end.

## Architecture
REST APIs → Python Ingestion → AWS S3 → Snowflake Stages → dbt Models → Analytics / ML

## Key Features
- Handles OAuth 2.0, API key, and JWT authentication flows
- Cursor and page-based pagination for large datasets
- Adaptive rate limiting with retry logic and dead-letter handling
- Pulls from 15+ REST endpoints including Salesforce CRM
- Layered dbt project: staging → intermediate → mart
- Kimball dimensional models with Star Schema and SCDs
- Apache Airflow DAGs for scheduling and alerting
- Great Expectations checkpoints enforcing data quality SLAs at every stage

## Tech Stack
| Layer | Tools |
|-------|-------|
| Language | Python, SQL |
| Ingestion | Python REST client (OAuth 2.0, pagination, rate limiting) |
| Storage | AWS S3, Snowflake (Stages, Snowpipe) |
| Transformation | dbt (models, tests, macros, lineage) |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Infrastructure | Docker, GitHub Actions CI/CD |

## Results
- Eliminated 100% of manual data preparation effort
- Achieved 99.9% delivery SLA across all pipeline runs
- Reduced data incidents by 25% through automated quality checks
- Enabled analysts to fully self-serve with standardized, tested business logic

## Project Structure
ingestion/

auth/         # OAuth, API key, JWT handlers

clients/      # Per-endpoint API clients

loaders/      # Snowflake stage uploaders

dbt/

models/

staging/        # Raw source cleaning

intermediate/   # Business logic

mart/           # Final dimensional models

tests/            # dbt data quality tests

dags/               # Airflow DAG definitions

expectations/       # Great Expectations suites
