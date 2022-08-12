#!/usr/bin/env python3
import aws_cdk as cdk
import datetime

from src.airflow_maa2 import AirflowMAAStack2
from src.airflow_maa_vpc import AirflowMaaVPCStack
from src.airflow_roles import AirflowMAARoles
from src.emr_buckets_stack import EMRBuckets
from src.emr_vpc_stack import EMRVpcStack

env_US = cdk.Environment(region="us-east-1", account="251647727762")
mwaa_props = {
    'airflow-dags': f'udacity-airflow-{datetime.datetime.now().strftime("%Y%m%d%H%M")}',
    'mwaa_env': 'mwaa-stack-env',
    'environment': 'dev',
    'application': 'udacity'
}


app = cdk.App()

emr_roles = AirflowMAARoles(
    scope=app,
    id='udacity-roles',
    env=env_US,
    mwaa_props=mwaa_props
)

emr_vpc = EMRVpcStack(
    scope=app,
    construct_id="udacity-vpc",
    env=env_US,
    mwaa_props=mwaa_props
)

emr_buckets = EMRBuckets(
    scope=app,
    construct_id="udacity-s3",
    env=env_US,
    mwaa_props=mwaa_props
)

mwaa_hybrid_backend = AirflowMaaVPCStack(
    scope=app,
    id="udacity-maa-vpc",
    env=env_US,
    mwaa_props=mwaa_props
)

mwaa_hybrid_env = AirflowMAAStack2(
    scope=app,
    id="udacity-airflow",
    vpc=mwaa_hybrid_backend.vpc,
    env=env_US,
    mwaa_props=mwaa_props
)

app.synth()
