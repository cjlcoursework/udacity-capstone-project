

### The Pipeline

Take in raw i94immigration data and global temperatures, and create a dataset that helps analysts find any correlation between migrations and global temperatures.

1. RAW DATA: Use raw data to create de-normalized core dataframes
```
                          |
                          |
                          prepare 
                          |
                          V
```
2. PARTITIONED DATASETS then write them to partitioned datasets by /year/month/?
    - The partitioned datasets are the START of the pipeline
    - I am setting this up as 'pretend' partitioned dataset so I can set up an incremental pipline
```
                          |
                          |
                          pipeline 
                          |
                          V
```
3. BATCH pipeline - create a batch pipeline that:
    1. periodically pulls from a periodic immigration dataset
    2. periodically pulls from a global temperatures dataset
    3. correlates the following two datasets:
        - immigrations into the US with
        - global temperatures in both the origin and arrival locations
    4. Use Airflow for scheduling
    5. EMR/Spark Streaming to pull from the partitioned immigration and temperatures datasets
        - create an extended "gold" dataset for query
        - create a "wide" denormalized dataset - instead of a warehouse
        - validate the data from each job for completeness
    6. Augment and store the denormalized datasets into a GOLD dataset
        - create a "wide" denormalized dataset - instead of a warehouse
```
                          |
                          |
                          query 
                          |
                          V
```
3. Query:

    8. Join the dataset having immigrations AND temperatures together by country
        - only US destinations for starter - can add other destinations later
    9. Chart the data in Tableau, Looker, Quicksight, or Jupyter, or?

---