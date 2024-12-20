# End-To-End-Data-Engineering-Project-With-Snowflake-and-python
This project uses snowflake-snowpark api for an ETL process that handles both initial and delta loads.

## Table of Contents
1. [Overview](#overview)
2. [Features](#features)
3. [Dataset Description](#dataset-description)
4. [Technologies Used](#technologies-used)
5. [Project Architecture](#project-architecture)
8. [Challenges Faced](#challenges-faced)
9. [Future Improvements](#future-improvements)
10. [Contact Information](#contact-information)

## Overview
for this project we collected amazon phone sales data for 3 different countries; US, FR, and IN. these datasets came in 3 different formats. parquet, json, and csv. these datasets are hosted on our local machine, and our goal is to load them into a snowflake internal stage and then make them ready for consumption as one unified dataset. this 

## Features
- Fact and Dimension table design
- Internal stages and file formats
- user creation and permissions
- ETL
  - data ingestion
  - data transformation
  - deduplication
  - data load (initial and delta)

## Dataset Description
The project uses a publicly available e-commerce dataset containing:
- 'Order Date': The date an order was placed
- 'Customer Name': customer name
- 'price per unit': price of product
- 'Quantity': Number of items ordered
- 'order Amount': Total sales amount for each order
- 'Order ID' unique identifier for each order
- 'Payment Status': shows if payment was completed
- 'Shipping Status': shows if it has be shipped
- 'Payment Provider': Shows payment carrier
- 'Payment Method': shows payment method
- 'Mobile Delivery Address': shows delivery address
- 'GST': as tax amount
- 'Mobile Model': model of phone

## Technologies Used
- **Programming Languages**: Python, SQL, SnowSQL
- **Libraries**: Pandas, snowflake.snowpark, os, logging, sys
- **Tools**: vscode, Snowflake

## Project Architecture
The project follows the typical data analytics pipeline:
1. establish snowpark connection
2. create database and schemas
3. create internal stages and file formats
4. load data to internal stage
5. load data from internal stage to source tables
6. load data from source to curated
7. load data from curated to consumption
8. could create dashboards etc

## Challenges Faced
- **Data Cleaning**: Dealt with duplicate values.
- **Robust loading**: Optimized code to ignore already loaded data and load new data without errors.
- **compactibility issues**: my snowpark was not compatitble with my pandas version, so it caused errors. it was an interesting issue to resolve. updating my snowpark to the latest version fixed it.

## Future Improvements
- use snowflake streams for delta loads
- make my code more robust by using error exceptions
- using auto increment ddls instead of sequences
- having a better naming conventions
- an audit table to check and monitor data

## Contact Information
For any inquiries, feel free to contact me:
- Email: chinwutemegbuluka@gmail.com
- LinkedIn: https://www.linkedin.com/in/chinwutem-egbuluka-76a870173/
