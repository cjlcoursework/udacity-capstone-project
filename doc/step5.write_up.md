
Step 5: Complete Project Write Up
### Clearly state the rationale for the choice of tools and technologies for the project.

> The i94immigration data is not really a closed data set where the data is controlled.
> 
> It is more approprite to a research or analytical dataset
>
> The data can get large enough that it cannot be handled by a single instance
> 
> This is most appropriate as an analytical dataset, to be used by a BI tool or a notebook
> Honestly, there's a good question whether temperature has any effect at all on immigration, but this sort of dataset might help validate that assumption


---


### Propose how often the data should be updated and why.
> This sort of data could get away with being monthly.
> 
> It is not streaming data and the providers would not update this kind of data daily
> 
> but I am thinking that our Airflow cron would look for new files **DAILY**

<br/>

### Write a description of how you would approach the problem differently under the following scenarios:

* The data was increased by 100x.
> I would not change anything except the cluster size
> 
> This solution is already designed as a "big date" solution

<br/>

* The data populates a dashboard that must be updated on a daily basis by 7am every day.
> I would schedule the job at end of day or early in the morning
> 
> I would size the cluster appropriatly to get done in a reasonable window
> 
> I would add some sort of meta-data were the users can see how current the data is  ( I have not done this)



<br/>

* The database needed to be accessed by 100+ people.
> Depending on the latency required I might move from Athena - where I cannot control the size of the cluster
> 
> to something like Snowflake, Redshift, or even MongoDB where I can control the scale-up
