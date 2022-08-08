from aws_cdk import (
    Duration,
    Stack,
    core,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_ssm as ssm,
    aws_s3_deployment as s3_deploy,
    aws_sns_subscriptions as subs, RemovalPolicy,
)
from constructs import Construct

from cdk.src.emr_vpc_stack import APP


class EMRBuckets(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.create_bucket("raw")
        self.create_bucket("processed")
        self.create_bucket("application")

    def create_bucket(self, bucket_name: str, copy_folder: str = None):
        bucket = s3.Bucket(self, bucket_name,
                           removal_policy=RemovalPolicy.DESTROY)

        ssm.StringParameter(self, f"ssm_{bucket_name}",
                            string_value=bucket.bucket_name,
                            parameter_name=f'/{APP}/dev/bucket/{bucket_name}'
                            )