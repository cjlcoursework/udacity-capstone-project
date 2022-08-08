from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
)


class VPCStack(Stack):

    vpc: ec2.Vpc

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(self, 'my-cdk-vpc', {
            'cidr': '10.0.0.0/16',
            'natGateways': 0,
            'maxAzs': 2,
            'subnetConfiguration': [
                {
                    'name': 'public-subnet-1',
                    'subnetType': ec2.SubnetType.PUBLIC,
                    'cidrMask': 24,
                }
            ],
        })
