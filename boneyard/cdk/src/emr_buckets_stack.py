from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_ssm as ssm,
    RemovalPolicy,
)
from constructs import Construct

from boneyard.cdk.src.emr_vpc_stack import APP


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