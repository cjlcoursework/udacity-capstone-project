# Deployment Steps

This document outlines the steps taken to deploy the solution in AWS.

---

# Steps Taken

## Run CDK from Local IDE
Used CDK to deploy all of the resources to ETL from raw tables to processed table.
The CDK is a pure code solution that:
1. Allows a developer to define the stack using Python, Node, Go, or Java code
2. Synthesizes Cloudformation Stacks
3. Runs the Stacks and monitors for completion

![Run CDK from Local](capstone-screenshots/00-cdk-run.png)

---

## Cloudformation stacks created
CDK creates and runs Cloudformation stacks and runs them, waiting until the CF stacks are fully executed

![Cloudformation stacks created](capstone-screenshots/03-cloudformation.png)

---

## Buckets created by CDK
CDK created the following buckets to support, raw data, log buckets, application and DAG data uploaded to s3

![Buckets created by CDK](capstone-screenshots/01-s3-cdk-buckets.png)

---

## DAGS sub-folder
DAG data was uploaded to s3 by CDK

![DAGS sub-folder](capstone-screenshots/02-dags-folder.png)
---

## Pyspark apps subfolder
pyspark applications were uploaded to s3 by CDK

![pyspark apps subfolder](capstone-screenshots/02-pyspark_application.png)

---

## ParameterStore Parameters created
As we create AWS resources, we add the information to the AWS Systems Manager Parameter Store


![Configuration Parameters created](capstone-screenshots/04-parameter-store.png)

---

## Managed Airflow DAGS
The CDK spins up a Managed Airflow instance (MAA) that uses Dags and Plugins located in S3 are picked up by AWS managed Airflow 

![Airflow DAGS](capstone-screenshots/05-airflow-dags2.png)

---

## Airflow Job Graph
There is a single DAG, but it deploys both immigration and temperature as a part of the "STEPS" list passedin into the 


```python
# EMR Steps are passed into the add_steps task -- both ETLS are run in parallel
EMR_STEPS=[
    {
        "Name": "Immigration ETL",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                "spark.yarn.submit.waitAppCompletion=true",
                "https://udacity-s3-application9137406b-gpvgce5aeiwe.s3.amazonaws.com/pyspark_applications/ingest_immigration.py"
            ]
        }
    },
    {
        "Name": "Temperature ETL",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                "spark.yarn.submit.waitAppCompletion=true",
                "https://udacity-s3-application9137406b-gpvgce5aeiwe.s3.amazonaws.com/pyspark_applications/ingest_temerature.py"
            ]
        }
    },
]

```

---

#### Airflow Tree View

![Airflow Job Graph](capstone-screenshots/06-airflow-job-graph.png)

---

## Airflow Graph View

![Airflow EMR Job](capstone-screenshots/07-airflow-emr-job.png)

---

## Processed Data
Once the job is complete the data from the raw bucket is transformed into the processed bucket
- Raw and Processed data 
* aws s3 ls raw:       [raw data files](raw.dump)
* aws s3 ls processed: [processed data files](processed.dump)

---

## AWS Glue Tabel Definitions
Once the parquet data is available in S3, we can enter the schemas and table names intp Glue.  

#### Immigration Table definition
![AWS Immigration table](capstone-screenshots/09-immigration-table.png)

#### Temperature Table definition
![AWS Immigration table](capstone-screenshots/09-temperatures_table.png)


---

## Athena Query Immigration
At this point the data is available in AWS Athena 

#### immigration table
![Athena Query Immigration](capstone-screenshots/10-athena-immigration.png)

#### temperature table
![Athena Query Temperature](capstone-screenshots/11-athena-temerature.png)

---

## Athena Join Immigrations with Temperatures
At this point we can join the data and provide a curated pivoted table to data analysts and BI tools.

```sql
WITH country_temps as (
	select canon_country_name,
		year,
		month,
		avg(average_temp) as avg_temp
	from temperature
	group by canon_country_name,
		year,
		month
)
select DISTINCT I.year as year,
	I.month as month,
	I.origin_country_name,
	C2.avg_temp as origin_temp,
	I.arrival_country_name,
	C1.avg_temp as arrival_temp,
	case
		when C2.avg_temp > C1.avg_temp then 'to-cooler-temp' else 'to-warmer-temp'
	end as direction
from immigration I
	left join country_temps C1 on (
		I.arrival_country_name = C1.canon_country_name
		and C1.year = I.year
		and C1.month = I.month
	)
	left join country_temps C2 on (
		I.origin_country_name = C2.canon_country_name
		and C2.year = I.year
		and C2.month = I.month
	);

```

#### Run in Athena
![Athena Join](capstone-screenshots/12-athena-curated-set.png)





