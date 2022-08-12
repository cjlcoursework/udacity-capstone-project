# todo - review - this is a abstracted version with an external payload - from job-flow-overrides, and emr-steps
import json
import os
import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from datetime import timedelta

# ************** AIRFLOW VARIABLES **************
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

JOB_FLOW_OVERRIDES={
    "Name": "demo-cluster-airflow",
    "ReleaseLabel": "emr-6.2.0",
    "LogUri": "s3n://udacity-s3-applicationlog4bca1895-p0axqcxsknyj",
    "Applications": [
        {
            "Name": "Spark"
        }
    ],
    "Instances": {
        "InstanceFleets": [
            {
                "Name": "MASTER",
                "InstanceFleetType": "MASTER",
                "TargetSpotCapacity": 1,
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "m5.xlarge"
                    }
                ]
            },
            {
                "Name": "CORE",
                "InstanceFleetType": "CORE",
                "TargetSpotCapacity": 1,
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "r5.xlarge"
                    }
                ]
            }
        ],
        "Ec2SubnetId": "subnet-0276870fc50977d00",
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        "Ec2KeyName": "{{ emr_ec2_key_pair }}"
    },
    "Configurations": [
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        }
    ],
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "EbsRootVolumeSize": 32,
    "StepConcurrencyLevel": 5,
    "Tags": [
        {
            "Key": "Environment",
            "Value": "Development"
        },
        {
            "Key": "Name",
            "Value": "Airflow EMR Demo Project"
        },
        {
            "Key": "Owner",
            "Value": "Data Analytics Team"
        }
    ]
}
# ***********************************************

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "udacity",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_object(key, bucket_name):
    """
    Load S3 object as JSON
    """

    hook = S3Hook()
    content_object = hook.read_key(key=key, bucket_name=bucket_name)
    return json.loads(content_object)


with DAG(
    dag_id=DAG_ID,
    description="Run multiple Spark jobs with Amazon EMR",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=pendulum.today('UTC').add(days=1),
    schedule_interval=None,
    tags=["emr demo", "spark", "pyspark"],
) as dag:

    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=EMR_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    begin >> cluster_creator >> step_adder >> step_checker >> end
