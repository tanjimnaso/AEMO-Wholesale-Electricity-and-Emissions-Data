# AEMO Wholesale Electricity and Emissions Data

### What

Built to demonstrate end-to-end pipeline ingesting AEMO wholesale electricity and emissions data into a SQL Server warehouse, with stored procedures surfacing grid emissions intensity by time-of-day for ESG reporting use cases, visualized in Power BI.

### Why

Energy sector market size predicted to be up 11% in 2026.

Source:

*https://www.ft.com/content/3cd94803-909c-4617-99b0-a1b5061f93ad*

Australian companies above the reporting threshold are now subject to mandatory climate-related financial disclosures under the Australian Sustainability Reporting Standards (ASRS).
A core data requirement is Scope 2 emissions, which depend on grid emissions intensity at the time of electricity consumption.
This project models that calculation from publicly available AEMO generation and emissions data."

### How

Python HTML parser -> Raw data to SQL Server -> SQL stored procedure creates tables -> PowerBI for reports

In a production environment this pipeline would be orchestrated via Azure Data Factory and Microsoft Fabric.