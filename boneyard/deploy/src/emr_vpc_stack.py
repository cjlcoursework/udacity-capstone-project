from typing import Any

from aws_cdk import (
    Duration,
    Stack,
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

APP = "udacity"


class EMRVpcStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        env_name = self.node.try_get_context("env")

        self.vpc = ec2.Vpc(self, 'emr-vpc',
                           cidr='192.168.50.0/24',
                           max_azs=2,
                           nat_gateways=1,
                           enable_dns_hostnames=True,
                           enable_dns_support=True,
                           subnet_configuration=[
                               ec2.SubnetConfiguration(
                                   name=f'{APP}-emr-public-subnet',
                                   subnet_type=ec2.SubnetType.PUBLIC,
                                   cidr_mask=26
                               )
                           ],

                           )

        ssm.StringParameter(self, APP,
                            string_value=self.vpc.vpc_id,
                            parameter_name=f'/{APP}/dev/emr/vpcId'
                            )

        public_subnets = [subnet.subnet_id for subnet in self.vpc.public_subnets]
        count = 1
        for psub in public_subnets:
            ssm.StringParameter(self, 'public-subnet-' + str(count),
                                string_value=psub,
                                parameter_name=f'/{APP}/emr/public-subnet/{str(count)}'
                                )
            count += 1


