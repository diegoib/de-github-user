# de-github-user
Repo for final project of DEZC

## Overview
The goal of this project is to build an end-to-end pipeline that extracts data, loads it in a data lake, to be later transformed with a big data tool to finally be consumed by a data visualization layer. For this purspose, the tools to be used are:
- GCP: as a cloud provider
- Terraform: for setting up the infrastructure
- Prefect: for the ELT orchestration
- Pyspark: for transformation
- BigQuery: as a Data Warehouse
- Looker: for data visualization

## Data source
We are going to use the [GitHub Archive](https://www.gharchive.org/) data source. This provides:

*20+ event types, which range from new commits and fork events, to opening new tickets, commenting, and adding members to a project. These events are aggregated into hourly archives*

