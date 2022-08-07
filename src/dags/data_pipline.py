"""
This Dag is pretty self-explanatory
 - The object of the Dag is to
    1. COPY a songs dataset from a pre-existing S3 folder into Redshift staging table
    2. COPY an events dataset from a pre-existing S3 folder into Redshift staging table
    3. Insert as select from songs and events staging tables into a data warehouse (DWH) in

    This is a one tier solution - the Dag handles all choices, and the operators are just workers

    The whole dag is in the udacity subfolder - there's a utils/ subdirectory with supporting code

"""
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from operators.cluster_pause import RedshiftClusterDownOperator
from operators.cluster_start import RedshiftClusterUpOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator
from udacity.utils.dhw_transforms import SqlQueries
from udacity.utils.constants import *

FEATURE_TRIAL_ONLY: bool = False

default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False,
    'email': [""],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup': False    # does not work with Airflow 2.x
}

dag = DAG(dag_id="data_pipeline_dag",
          schedule_interval="@hourly",
          description="test dag",
          catchup=False,
          default_args=default_args)

check_for_preconditions = PythonOperator (
    task_id="deploy emr",
    dag=dag
)

deploy_emr = PythonOperator (
    task_id="deploy emr",
    dag=dag
)


# start_operator >> stage_events_to_redshift >> load_song_plays_table
# start_operator >> stage_songs_to_redshift >> load_song_plays_table
# load_song_plays_table >> load_user_dimension_table >> run_quality_checks >> end_operator
# load_song_plays_table >> load_song_dimension_table >> run_quality_checks >> end_operator
# load_song_plays_table >> load_artist_dimension_table >> run_quality_checks >> end_operator
# load_song_plays_table >> load_time_dimension_table >> run_quality_checks >> end_operator
