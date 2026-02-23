# AEMO Wholesale Electricity and Emissions Data

### What

Built to demonstrate end-to-end pipeline ingesting AEMO wholesale electricity and emissions data into a SQL Server warehouse, with stored procedures surfacing grid emissions intensity by time-of-day for ESG reporting use cases, visualized in ~~Power BI~~ Streamlit.

### Why

Energy sector market size predicted to be up 11% in 2026.

Source:

*https://www.ft.com/content/3cd94803-909c-4617-99b0-a1b5061f93ad*

Australian companies above the reporting threshold are now subject to mandatory climate-related financial disclosures under the Australian Sustainability Reporting Standards (ASRS).
A core data requirement is Scope 2 emissions, which depend on grid emissions intensity at the time of electricity consumption.
This project models that calculation from publicly available AEMO generation and emissions data."

### Process

#### 1. Sourcing Data

AEMO supplies demand and generation data in numerous tables, such as daily averages (dispatch summary), and in 5 minute intervals.

Each generator unit uses different fuels, and it's notable that the AEMO Data Dashboard's Renewable Penetration report illustrates that the maximum recorded over a 30 minute interval was in October 2025 at 78.6%.

By knowing the output of each genunit and their fuel types, we can visualise a more accurate understanding of emmissions numbers.

So data is sourced from 5 minute intervals, not daily averages.

https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/operational-demand-data

Emissions data is calculated as generation from fuel type multiplied by emission per mwh, based on fact sheet.

#### 2. Importing and Normalising

Python used to download and unzip files from NEMWEB, some columns not dropped, appended to a Pandas dataframe, sent to Azure SQL Server.

GitHub actions, through the yaml file is set to run at 6am everyday.

Initially intended to write to CSV, and create report on PowerBI. However publishing a public PowerBI report costs money on Microsoft's Power platform. Instead using Streamlit as a free workaround.

#### 3. Azure SQL Server

The Pandas dataframe write to CSV here, the numbers are checked against the fuel type emissions fact sheet.

Source:

*source goes here*

The graphics are created with more Python.

Can't afford Azure Data Factory/Microsoft Fabric for orchestration.
#### 4. Streamlit Interactive Dashboard

Fun and free interactive online dashboard.

### Limitations and Scope

The project covers 5 NEM regions in the AEMO dispatch framework (QLD, NSW, SA, VIC and TAS).

WA has a separate grid, Wholesale Electricity Market (WEM) which supplies separate data. Gas and coal are the primary fuel types.

NT has three grids for Darwin-Katherine, Tennant Creek and Alice Springs, with some data supplied by Interim Northern Territory Electricity Market (I-NTEM).
Gas is primary fuel type.

A 'national emissions data pipeline' incorporating WEM and NT data sources is outside the current scope of this project and represents a natural extension for future development.

### References 

While reading the process of this project, the UNSW NEMED tool may have come to mind.

NEMED is a Python library that gives researchers a package to pull emissions data programmatically, it's designed for a different audience and purpose. It was not used for this project as data requirements and processing are different.

This project demonstrates an end-to-end data engineering pipeline, from ingestion, warehouse schema, SQL stored procedures to Azure SQL Server reporting, framed around ESG reporting obligations.