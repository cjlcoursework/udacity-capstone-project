
### Data Pipeline Capstone

* [terms](doc/terms.md)

* [data dictionary](doc/data_dictionary.md)

---


### Step 1: Scope the Project and Gather Data
- Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.
- Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)


* documentation:   [scope of the project](doc/step1.scope.md)

---

### Step 2. Explore and Assess the Data
- Explore the data to identify data quality issues, like missing values, duplicate data, etc.
- Document steps necessary to clean the data


* documentation:   
  * [data exploration](doc/step2.explore.md)
  
  * [immigration cleanup notes](doc/step2.labnotes1.md)

  * [defect cleanup details](doc/step2.labnotes2.md)
---

### Step 3: Define the Data Model
- Map out the conceptual data model and explain why you chose that model
- List the steps necessary to pipeline the data into the chosen data model


* documentation:   [definition of the data](doc/step3.define.md)

---

### Step 4: Run ETL to Model the Data
- Create the data pipelines and the data model
- Include a data dictionary
- Run data quality checks to ensure the pipeline ran as expected
- Integrity constraints on the relational database (e.g., unique key, data type, etc.)
- Unit tests for the scripts to ensure they are doing the right thing
- Source/count checks to ensure completeness


* documentation:
  - [architecture](doc/step4.architecture.md)

  - [the data pipeline](doc/step4.pipeline.md)

  - [base query](doc/step4.proof-of-concept.md)

---


### Step 5: Complete Project Write Up
- What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
- Clearly state the rationale for the choice of tools and technologies for the project.
- Document the steps of the process.
- Propose how often the data should be updated and why.
- Post your write-up and final data model in a GitHub repo.
- Include a description of how you would approach the problem differently under the following scenarios:
  - If the data was increased by 100x.
  - If the pipelines were run on a daily basis by 7am.
  - If the database needed to be accessed by 100+ people.





