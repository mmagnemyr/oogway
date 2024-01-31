from aws_cdk import (
    aws_iam as iam,
    Duration
)
from constructs import Construct
from typing import List, Union

# This module is required by VGCS and maintained by Delivery Engineering
import importlib
de_pipelines =  importlib.import_module("de-pipelines-constructs")

class GenericSsoRoleStack(de_pipelines.VgcsStack):

    def __init__(self, scope: Construct, construct_id: str, stack_vars: dict, usecase_name: str, aws_account_id: str, aws_region: str, env_name: str, vpc_name, saml_provider_name:str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        role_name=f"{usecase_name}-{stack_vars['SSO_ROLE_NAME_SUFFIX']}"

        self.create_sso_role(
            role_path=stack_vars["SSO_ROLE_PATH"],
            role_name=role_name,
            role_description=stack_vars["STACK_DESCRIPTION"],
            usecase_name=usecase_name,
            aws_account_id=aws_account_id,
            aws_region=aws_region,
            bucket_name=f"{stack_vars['BUCKET_NAME_PREFIX']}-{env_name}",
            vpc_name=vpc_name,
            saml_provider_name=saml_provider_name,
            underlying_s3_access=stack_vars["UNDERLYING_S3_ACCESS"]
        )
    
    def create_sso_role(
        self,
        role_path: str,
        role_name: str,
        role_description: str,
        usecase_name: str,
        aws_account_id: str,
        aws_region: str,
        bucket_name: str,
        vpc_name: Union[str, List],
        saml_provider_name: str,
        underlying_s3_access: bool
    ) -> None:
        
        # Managed Policy
        lakeformation_policy= iam.ManagedPolicy.from_managed_policy_name(
            scope=self,
            id='LakeformationDataAccess',
            managed_policy_name='CustomPolicies/Services/Lakeformation/LakeformationDataAccess' # lakeformation:GetDataAccess action
        )

        # Inline Policies
        athena_list_data_catalog_policy = iam.PolicyStatement(
            actions=[
                'athena:ListDataCatalogs' # Seems to be required based on ODBC connectivity testing
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                '*'
            ],
            conditions={
                'StringEquals': {
                    'aws:sourceVpc': vpc_name
                }
            }
        )

        athena_work_group_policy = iam.PolicyStatement(
            actions=[
                'athena:GetQueryExecution',
                'athena:GetQueryResults',
                'athena:GetQueryResultsStream',
                'athena:StartQueryExecution',
                'athena:StopQueryExecution',
                'athena:CreatePreparedStatement'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:athena:{aws_region}:{aws_account_id}:workgroup/{usecase_name}"
            ],
            conditions={
                'StringEquals': {
                    'aws:sourceVpc': vpc_name
                }
            }
        )

        glue_policy = iam.PolicyStatement(
            actions=[
                'glue:GetDatabase',
                'glue:GetDatabases',
                'glue:GetTables',
                'glue:GetTable',
                'glue:GetPartition',
                'glue:GetPartitions'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                "*"
            ],
            conditions={
                'StringEquals': {
                    'aws:sourceVpc': vpc_name
                }
            }
        )

        glue_via_athena_policy = iam.PolicyStatement(
            actions=[
                'glue:GetDatabase',
                'glue:GetDatabases',
                'glue:GetTables',
                'glue:GetTable',
                'glue:GetPartition',
                'glue:GetPartitions'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                "*"
            ],
            conditions={
                'ForAnyValue:StringEquals':{
                    'aws:CalledVia':'athena.amazonaws.com'
                }
            }
        )

        s3_via_athena_policy = iam.PolicyStatement(
            actions=[
                's3:PutObject',
                's3:GetObject',
                's3:ListBucket',
                's3:GetBucketLocation',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ],
            conditions={
                'ForAnyValue:StringEquals':{
                    'aws:CalledVia':'athena.amazonaws.com'
                }
            }
        )

        policy_doc = iam.PolicyDocument(
            statements=[
                athena_list_data_catalog_policy,
                athena_work_group_policy,
                glue_policy,
                glue_via_athena_policy,
                s3_via_athena_policy
            ]
        )

        s3_get_object_policy = iam.PolicyStatement(
            actions=[
                's3:GetObject'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:s3:::{bucket_name}/db/*"    
            ],
            conditions={
                'StringEquals':{
                    'aws:sourceVpc': vpc_name
                }
            }
        )

        if underlying_s3_access:
            policy_doc.add_statements(s3_get_object_policy)

        iam.Role(
            scope=self,
            id=role_name,
            description=role_description,
            role_name=role_name,
            assumed_by=iam.SamlPrincipal(
                saml_provider=iam.SamlProvider.from_saml_provider_arn(
                    scope=self,
                    id="from_saml_provider",
                    saml_provider_arn=f"arn:aws:iam::{aws_account_id}:saml-provider/{saml_provider_name}"
                ),
                conditions={
                    "StringEquals": {
                        "SAML:aud": "http://localhost/athena"
                    }
                }
            ),
            managed_policies=[lakeformation_policy],
            inline_policies={
                f"{usecase_name}-cdk-sso-role-inline-policy": policy_doc
            },
            path=f"/{role_path}/",
            max_session_duration=Duration.hours(1)
        )