# Automated Data Pipeline for NOAA Weather Data

## Team Members -
- Aditi Ashutosh Deodhar  002279575
- Lenin Kumar Gorle  002803806
- Poorvika Girish Babu  002801388

## Project Overview

### Problem Statement

This Quickstart will cover a lot of ground, and by the end you will have built a robust data engineering pipeline using Snowpark Python stored procedures. That pipeline will process data incrementally, be orchestrated with Snowflake tasks, and be deployed via a CI/CD pipeline. You'll also learn how to use Snowflake's new developer CLI tool and Visual Studio Code extension!

## Methodology
Refer to the codelabs document to find a detailed explanation to follow the quickstart

## Technologies Used

- Python
- Snowflake
- AWS

## Architecture Diagram

![image](https://github.com/user-attachments/assets/b3569453-e2f3-477c-8443-aed10fae6917)

## Codelabs Documentation (QuickStart)
https://codelabs-preview.appspot.com/?file_id=1wk6H_M9M_dLhHGOEL4t6OVRPMPILZgCr3UrAkTILmRE#16

## Demo -
https://shorturl.at/ir2Rh

## Prerequisites
- Python 3.9+
- AWS Account
- Snowflake Account

## Set Up the Environment
```
   # clone the environment
     https://github.com/deodhar-ad/DAMG7245-Assignment-01.git
     cd DAMG7245-Assignment-01
   # add all the environmental variables in .env file
   # add all the secrets for snowflake , api token and AWS in the github secrets
   # enable the github actions to the repository
```

## Project Structure
 ``` 
DAMG7245-Assignment-03/
├─ steps/                # steps to execute quickstart steps
├─ tests/                # tests for the UDF's and stored procedures 
└─ requirements.txt      # library requirements
 
```
