#!/usr/bin/env python3

import aws_cdk as cdk

from src.emr_buckets_stack import EMRBuckets
from src.emr_vpc_stack import EMRVpcStack


app = cdk.App()

emr_vpc = EMRVpcStack(app, "udacity-vpc")

emr_buckets = EMRBuckets(app, "udacity-s3")

# airflow = MWAAirflowStack(app, "MWAAirflowStack")

# airflow = AirflowStack(
#     app,
#     construct_id="MWAAEnvStack",
#     vpc=None,
#     subnet_ids_list= "",
#     env_name="dev",
#     env_tags="",
#     env_class="",
#     max_workers=1,
#     access_mode="PUBLIC",
#     secrets_backend=""
# )

app.synth()
