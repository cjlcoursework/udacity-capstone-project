#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
)
from constructs import Construct

from cdk.src.airflow_stack import AirflowStack
from .nested_stacks.project import AirflowProjectStack
from cdk.src.airflow_vpc import VpcStack
from .nested_stacks.provisioning import AirflowProvisioningStack


class MWAAirflowStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.cidr = None
        self.vpc_id = None

        # Try to get VPC ID
        self.vpc_id = self.node.try_get_context("vpcId")
        if not self.vpc_id:
            self.cidr = self.node.try_get_context("cidr")
            self.vpc = VpcStack(
                self, construct_id="MWAAVpcStack", cidr=self.cidr, **kwargs
            ).vpc
        else:
            self.vpc = ec2.Vpc.from_lookup(self, "MWAAVPC", vpc_id=self.vpc_id)

        # Try to get Stack params
        self.subnet_ids_list =  ""
        self.env_name =  "MwaaEnvironment"
        self.env_tags = {}
        self.env_class =  "mw1.small"
        self.max_workers =  1
        self.access_mode = (
            "PUBLIC_ONLY"
        )
        self.secrets_backend = self.node.try_get_context("secretsBackend")

        mwaa_env = AirflowStack(
            self,
            construct_id="MWAAEnvStack",
            vpc=self.vpc,
            subnet_ids_list=self.subnet_ids_list,
            env_name=self.env_name,
            env_tags=self.env_tags,
            env_class=self.env_class,
            max_workers=self.max_workers,
            access_mode=self.access_mode,
            secrets_backend=self.secrets_backend,
            **kwargs
        )

        project_stack = AirflowProjectStack(
            self, construct_id="MWAAProjectStack", mwaa_bucket=mwaa_env.bucket, **kwargs
        )

        provisioning_stack = AirflowProvisioningStack(
            self,
            construct_id="MWAAProvisioningPipelineStack",
            vpc_id=self.vpc_id,
            cidr=self.cidr,
            mwaa_bucket=mwaa_env.bucket,
            **kwargs
        )

        provisioning_stack.add_dependency(project_stack)
