#!/usr/bin/env python3

import aws_cdk as cdk

from deployment.src.airflow_maa import AirflowMAAStack
from deployment.src.airflow_maa_vpc import AirflowMaaVPCStack
from deployment.src.emr_buckets_stack import EMRBuckets
from deployment.src.emr_vpc_stack import EMRVpcStack

env_US=cdk.Environment(region="us-east-1", account="251647727762")

app = cdk.App()

emr_vpc = EMRVpcStack(app, "udacity-vpc")

emr_buckets = EMRBuckets(app, "udacity-s3")

mwaa_props = {'dagss3location': '094459-airflow-hybrid-demo','mwaa_env' : 'mwaa-hybrid-demo'}

mwaa_hybrid_backend = AirflowMaaVPCStack(
    scope=app,
    id="mwaa-hybrid-backend",
    env=env_US,
    mwaa_props=mwaa_props
)

mwaa_hybrid_env = AirflowMAAStack(
    scope=app,
    id="mwaa-hybrid-environment",
    vpc=mwaa_hybrid_backend.vpc,
    env=env_US,
    mwaa_props=mwaa_props
)

app.synth()
