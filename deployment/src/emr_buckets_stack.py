from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_ssm as ssm,
    aws_s3_deployment as s3_deploy,
    RemovalPolicy, Environment,
)
from constructs import Construct


APP = "udacity"
LOCAL_PYSPARK_APPLICATIONS_PATH = "../src/pyspark_applications"  # todo move all paths a dn albels to external module


class EMRBuckets(Stack):

    # todo - move labels to a commons module
    config_labels = {
        "release_label",
        "raw_data_path",
        "airflow_dags_path",
        "airflow_logs_path",
        "airflow_work_path",
        "application_data_path",
        "processed_data_path",
        "bootstrap_path",
        "logs_path",
        "emr_subnet_id",
        "emr_ec2_key_pair",
        "job_flow_role",
        "service_role",
        'immigration_data_path'
    }

    def __init__(self, scope: Construct, construct_id: str, env: Environment, mwaa_props,  **kwargs) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

        raw_bucket = self.create_bucket(
            bucket_name="raw",
            label_name=self.get_ssm_label_name(
                label_name="raw_data_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment'])
        )

        processed_bucket = self.create_bucket(
            bucket_name="processed",
            label_name=self.get_ssm_label_name(
                label_name="processed_data_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment']) or None
        )

        app_bucket = self.create_bucket(
            bucket_name="application",
            label_name=self.get_ssm_label_name(
                label_name="application_data_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment']) or None
        )

        airflow_logs_path = self.create_bucket(
            bucket_name="aiflow-log",
            label_name=self.get_ssm_label_name(
                label_name="airflow_logs_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment']) or None
        )

        bootstrap_path = self.create_bucket(
            bucket_name="emr-bootstrap",
            label_name=self.get_ssm_label_name(
                label_name="bootstrap_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment']) or None
        )

        bootstrap_path = self.create_bucket(
            bucket_name="airflow-work",
            label_name=self.get_ssm_label_name(
                label_name="airflow_work_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment']) or None
        )

        logs_path = self.create_bucket(
            bucket_name="application-log",
            label_name=self.get_ssm_label_name(
                label_name="logs_path",
                application_name=mwaa_props['application'],
                environment=mwaa_props['environment']) or None
        )

        s3_deploy.BucketDeployment(self, "DeployApplications",
                                   sources=[s3_deploy.Source.asset(LOCAL_PYSPARK_APPLICATIONS_PATH)],
                                   destination_bucket=app_bucket,
                                   destination_key_prefix="pyspark_applications",
                                   prune=False,
                                   retain_on_delete=False
                                   )

    def create_bucket(self, bucket_name: str, label_name: str ):
        bucket = s3.Bucket(self, bucket_name,
                           removal_policy=RemovalPolicy.DESTROY,
                           versioned=True,
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL or None,
                           auto_delete_objects=True
                           )

        ssm.StringParameter(self, f"ssm_{bucket_name}",
                            string_value=bucket.bucket_name,
                            parameter_name=label_name
                            )

        return bucket

    def get_ssm_label_name(self, label_name: str, application_name: str, environment: str,  strict: bool = True) -> str:
        if label_name not in self.config_labels:
            if strict:
                raise Exception(f"{label_name} is not a known label name")
        if application_name is None:
            raise Exception(f"application_name cannot be None ")
        if environment is None:
            raise Exception(f"environment cannot be None ")
        if label_name is None:
            raise Exception(f"label_name cannot be None ")

        return f"/{application_name.lower()}/{environment.lower()}/{label_name.lower()}"

