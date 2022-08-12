from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_ssm as ssm, Environment,
)
from constructs import Construct

APP = "udacity"


class EMRVpcStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env: Environment, mwaa_props: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

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
                            parameter_name=f'/{APP}/dev/emr_vpcId'
                            )

        public_subnets = [subnet.subnet_id for subnet in self.vpc.public_subnets]
        count = 1
        label = "emr_subnet_id"
        for psub in public_subnets:
            if count > 1:
                label = f"emr_subnet_id/{str(count)}"
            ssm.StringParameter(self, 'public-subnet-' + str(count),
                                string_value=psub,
                                parameter_name=f'/{APP}/dev/{label}'
                                )
            count += 1
