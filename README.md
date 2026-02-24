# AEMO Wholesale Electricity and Emissions Data

Find app here:

https://aemo-emissions-data.streamlit.app/

### What
Built to demonstrate end-to-end pipeline ingesting AEMO wholesale electricity and emissions data into a SQL Server warehouse, with stored procedures surfacing grid emissions intensity by time-of-day for ESG reporting use cases, visualized in ~~Power BI~~ Streamlit.

### Why
Energy sector market size predicted to be up 11% in 2026.

Source:

***https://www.ft.com/content/3cd94803-909c-4617-99b0-a1b5061f93ad***

Australian companies above the reporting threshold are now subject to mandatory climate-related financial disclosures under the Australian Sustainability Reporting Standards (ASRS).
A core data requirement is Scope 2 emissions, which depend on grid emissions intensity at the time of electricity consumption.
This project models that calculation from publicly available AEMO generation and emissions data."

### Process
#### 1. Sourcing Data

AEMO supplies demand and generation data in numerous tables, such as daily averages (dispatch summary), and in 5 minute intervals.

Each generator unit uses different fuels, and it's notable that the AEMO Data Dashboard's Renewable Penetration report illustrates that the maximum recorded over a 30 minute interval was in October 2025 at 78.6%.

By knowing the output of each genunit and their fuel types, we can visualise a more accurate understanding of emmissions numbers.

So data is sourced from 5 minute intervals, not daily averages.

AEMO Site

[https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb)

Generation and Load Data:

[https://nemweb.com.au/Reports/Current/Dispatch_SCADA/](https://nemweb.com.au/Reports/Current/Dispatch_SCADA/)

Genunit fuel type is sourced from ***'NEM Generation Info Jan 2026.xlsx'*** file:

https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information

Emissions data is calculated as generation from fuel type multiplied by emission per mwh, based on workbook.

National Greenhouse Accounts (NGA) Factors workbook published annually by DCCEEW from:

https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors

#### 2. Importing and Normalising

Python used to download and unzip files from NEMWEB, some columns not dropped, appended to a Pandas dataframe, sent to Azure SQL Server.

GitHub actions, through the yaml file is set to run at 6am everyday.

Initially intended to write to CSV, and create report on PowerBI. However publishing a public PowerBI report costs money on Microsoft's Power platform. Instead using Streamlit as a free workaround.

#### 3. Streamlit Interactive Dashboard

Fun and free interactive online dashboard.

### Limitations and Scope

The project covers 5 NEM regions in the AEMO dispatch framework (QLD, NSW, SA, VIC and TAS).

WA has a separate grid, Wholesale Electricity Market (WEM) which supplies separate data. Gas and coal are the primary fuel types.

NT has three grids for Darwin-Katherine, Tennant Creek and Alice Springs, with some data supplied by Interim Northern Territory Electricity Market (I-NTEM).
Gas is primary fuel type.

A 'national emissions data pipeline' incorporating WEM and NT data sources is outside the current scope of this project and represents a natural extension for future development.

### References 

While reading the process of this project, the UNSW NEMED tool may have come to mind.

NEMED is a Python library that gives researchers a package to pull emissions data programmatically, it's designed for a different audience and purpose.
It was not used for this project as data requirements and processing are different.

This project is framed around ESG reporting obligations and demonstrates an end-to-end data engineering pipeline:
* Ingestion
* Warehouse schema
* Python stored procedures to ~~Azure SQL Server~~ GitHub Actions as orchestration, writing out daily data file for reporting
#