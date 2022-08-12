# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import (
    aws_iam as iam,
    aws_ssm as ssm,
    Stack, Environment
)
from constructs import Construct


class AirflowMAARoles(Stack):

    def __init__(self, scope: Construct, id: str, env: Environment, mwaa_props: dict, **kwargs) -> None:
        super().__init__(scope, id, env=env, **kwargs)

        self.emr_demo_role = iam.Role(self, "EMR_DemoRole",
                                      role_name="EMR_DemoRole",
                                      assumed_by=
                                      iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
                                      description="EMR_DemoRole role...",
                                      managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                                          'service-role/AmazonElasticMapReduceRole')] or None,
                                      )

        self.emr_ec2_demo_role = iam.Role(self, "EMR_EC2_DemoRole",
                                          role_name="EMR_EC2_DemoRole",
                                          assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
                                          managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                                              'service-role/AmazonElasticMapReduceforEC2Role')] or None,
                                          description="Example role...",
                                          )
        self.emr_ec2_demo_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=['ssm:*'],
            resources=[f'arn:aws:ssm:{self.region}:{self.account}:parameter/udacity*']
        ))

        ssm.StringParameter(self, f"ssm_job_flow_role",
                            string_value="EMR_Role",
                            parameter_name="/udacity/dev/job_flow_role"
                            )
        ssm.StringParameter(self, f"ssm_service_role",
                            string_value="EMR_EC2_Role",
                            parameter_name="/udacity/dev/service_role"
                            )

