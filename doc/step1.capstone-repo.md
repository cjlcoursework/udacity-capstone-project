
## Contents of this repository

| PATH         | SUBDIR              | DESCRIPTION                                                                                                                |
|:-------------|---------------------|----------------------------------------------------------------------------------------------------------------------------|
| airflow/     | dags/               | Airflow DAGS that initialize EMR and process pyspark steps from raw to processed                                           |
|              | job_flow_overrides/ | for the create EMR task                                                                                                    |
|              | emr_steps/          | steps for the pyspark master                                                                                               |
|              | scripts/            | helper functions to pre-format yaml/json inputs from configurations                                                        |
| boneyard/    |                     | experimental  code that is no longer used                                                                                  |
| data/        | raw/                | pre-processed data from source_data that we will use as hypothetical "input" data                                          |
|              | processed/          | This is preprocessed data that we will use as input data from a local run of pyspark - the same code we will upload to EMR |
| deploy/      |                     | CDK scrips that create AWS Airflow, VPC's Buckets, and uploads data to the s3 buckets                                      |
|              | scripts/            | assorted deployment scripts to upload the raw dataset to s3                                                                |
|              | src/                | CDK source code                                                                                                            |
|              | tests/              | Work in progress                                                                                                           |
| doc/         |                     | documentation of the solution                                                                                              |
|              |                     |                                                                                                                            |
| source_data/ |                     | raw csv, and sas data used to create a curated raw dataset                                                                 |
|              | controls/           | raw ancillary data used to create a 'dummy' raw input dataset                                                              |
|              | temperature/        | raw Global temperature data                                                                                                |
|              | udacity/            | raw immigration data from Udacity                                                                                          |
| src/         |                     | pyspark code to prepare, and execute the solution                                                                          |
|              |                     |                                                                                                                            |

## 