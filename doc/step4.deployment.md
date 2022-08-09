## Deployment



## CDK references and starter code
1. `AWS samples`:  https://github.com/aws-samples/cdk-amazon-mwaa-cicd.git
2. `Ricardo Sueiras`: https://github.com/094459/blogpost-cdk-mwaa.git

---

### DAG creation
- Create EMR instance in EMR VPC
- Add steps to ingest and process immigration and temperature raw data into the processed bucket
- Tear down

---

![](deployment.png)

### CDK deployment
- [x] create the EMR VPC


- [x] create data buckets and upload code
  - `raw-bucket` -- holding the curated input data for immigrations and temperatures
  - `processed-bucket` -- destination for final processed data
  - `application-data` -- pyspark application files 
  - upload pyspark_applications to the `application-data` bucket


- [x] create a separate `airflow-bucket` for airflow dags
  - upload dags to the airflow bucket


- [x] create a separate `VPC` for Airflow


- [x] create an `AWS managed Airflow` instance in the airflow VPC

---

### Scripts
- [x] Upload the curated RAW immigration and temperature data to the raw-bucket
- [x] (incude a process to empty buckets before asking CDK to remove them)

