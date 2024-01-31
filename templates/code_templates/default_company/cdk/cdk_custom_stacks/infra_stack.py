from typing import cast
import importlib
from constructs import Construct

# This module is required by VGCS and maintained by Delivery Engineering
from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    Duration,
    aws_lakeformation as lf,
    aws_glue as glue,
    aws_athena as athena,
    RemovalPolicy,
    aws_sqs as sqs
)
from cdk_custom_stacks.env_variables import EnvVariables

DE_PIPELINES = importlib.import_module("de-pipelines-constructs")

class InfraStack(DE_PIPELINES.VgcsStack):
    """
      Infra cdk stack
    """
    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            stack_settings: dict,
            **kwargs
        ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create bucket for workgroup queries and use case
        query_results_prefix = 'athena-query-results'
        bucket = self.create_s3_bucket(
            bucket_name=stack_settings["TARGET_BUCKET"],
            query_results_prefix=query_results_prefix,
            queryresult_days_retention=None,
            db_days_retention=None
        )
        cfn_bucket = cast(s3.CfnBucket, bucket.node.default_child)

        workgroup_bucket_name=stack_settings["TARGET_BUCKET"]
        cfn_work_group = self.create_athena_workgroup(
            name=stack_settings["WORKGROUP_NAME"],
            bucket_name=workgroup_bucket_name,
            query_results_prefix=query_results_prefix)
        cfn_work_group.add_dependency(cfn_bucket)

        # database
        cfn_db = self.create_glue_db(
            stack_settings["AWS_ACCOUNT"],
            stack_settings["DB_NAME"],
            stack_settings["DB_COMMENT"],
            stack_settings["TARGET_BUCKET"])
        # lake formation Data locations
        lakeformation_resource = self.create_lakeformation_resource(
            bucket=bucket,
            bucket_name=stack_settings["TARGET_BUCKET"]
        )
        lakeformation_resource.add_dependency(cfn_bucket)

        # Steps role
        dead_letter_queue = self.create_dead_letter_queue(
            f"{stack_settings['SQS_QUEUE_NAME']}-id",
            stack_settings["SQS_QUEUE_NAME"])
        cfn_queue = cast(sqs.Queue, dead_letter_queue.node.default_child)

        self.iam_step_role = self.create_step_role(
            step_role_name=stack_settings["STEPS_ROLE_NAME"],
            aws_region=stack_settings["AWS_REGION"],
            aws_account_id=stack_settings["AWS_ACCOUNT"],
            bucket_name=stack_settings["TARGET_BUCKET"],
            workgroup_name=stack_settings["WORKGROUP_NAME"],
            workgroup_bucket_name=workgroup_bucket_name,
            db_name=stack_settings["DB_NAME"],
            steps_role_prefix=stack_settings["STEPS_ROLE_PREFIX"],
            glue_job_name_prefix=stack_settings["NAME"]
            )
        cfn_step_role = cast(iam.CfnRole, self.iam_step_role.node.default_child)
        cfn_step_role.add_dependency(cfn_queue)
        cfn_step_role.add_dependency(cfn_db)

        db_names = stack_settings["STEPS_ROLE_DB_DEPENDENCIES"]
        for db_name in db_names:
            # lake formation Database permission
            p=self.grant_role_lakeformation_permission_to_db(
                role=cfn_step_role,
                aws_account_id=stack_settings["AWS_ACCOUNT"],
                db_name_to_grant_permission_to=db_name,
                dependent_on_db=cfn_db
            )
            p.add_dependency(cfn_db)
            # lake formation table permission
            # adding also db permission dependency although the step role is already dependent on the db so it should not be necessary, even so we get a dependency issue that the table can be created before the db.
            p=self.grant_role_lakeformation_permission_to_table(
                role=cfn_step_role,
                aws_account_id=stack_settings["AWS_ACCOUNT"],
                db_name_to_grant_permission_to=db_name,
                dependent_on_db=cfn_db
            )
            p.add_dependency(cfn_db)

    def create_athena_workgroup(self, name: str, bucket_name: str, query_results_prefix: str):
        workgroup_description = 'Workgroup for use case: ' + name

        encryption_configuration_prop = athena.CfnWorkGroup.EncryptionConfigurationProperty(
            encryption_option = 'SSE_S3'
        )

        result_configuration_prop = athena.CfnWorkGroup.ResultConfigurationProperty(
            output_location = f"s3://{bucket_name}/{query_results_prefix}/",
            encryption_configuration = encryption_configuration_prop,
        )
        workgroup_config_prop = athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            enforce_work_group_configuration = True,
            engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                effective_engine_version="Athena engine version 3",
                selected_engine_version="Athena engine version 3"
            ),
            publish_cloud_watch_metrics_enabled = True,
            requester_pays_enabled = False,
            result_configuration = result_configuration_prop,
        )

        cfn_work_group = athena.CfnWorkGroup(self, name + '-workgroup',
            name = name,
            description = workgroup_description,
            recursive_delete_option = False,
            state = 'ENABLED',
            work_group_configuration = workgroup_config_prop
        )
        return cfn_work_group


    def create_glue_db(
            self,
            aws_account_id: str,
            database_name: str,
            comment: str,
            bucket_name: str
    ) -> glue.CfnDatabase:
        glue_db = glue.CfnDatabase(
            scope=self,
            id=f"{database_name.lower()}-id",
            catalog_id=aws_account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description=comment,
                location_uri=f"s3://{bucket_name}/db/",
            )
        )
        return glue_db

    def grant_role_lakeformation_permission_to_db(
            self,
            role: iam.CfnRole,
            aws_account_id: str,
            db_name_to_grant_permission_to: str,
            dependent_on_db: glue.CfnDatabase
    ) -> lf.CfnPermissions:
        # Database
        print(f"DB name:{db_name_to_grant_permission_to}")
        db_lf_permissions = lf.CfnPermissions(
            scope=self,
            id=f"db_lf_permissions_{db_name_to_grant_permission_to}",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role.attr_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=aws_account_id,
                    name=db_name_to_grant_permission_to
                )
            ),
            permissions=["ALL"]
        )
        db_lf_permissions.add_dependency(role)
        db_lf_permissions.add_dependency(dependent_on_db)
        return db_lf_permissions

    def grant_role_lakeformation_permission_to_table(
            self,
            role: iam.CfnRole,
            aws_account_id: str,
            db_name_to_grant_permission_to: str,
            dependent_on_db: glue.CfnDatabase
    ) -> lf.CfnPermissions:
        print(f"DB name:{db_name_to_grant_permission_to}")
        table_lf_permissions = lf.CfnPermissions(
            scope=self,
            id=f"table_lf_permissions{db_name_to_grant_permission_to}",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role.attr_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    catalog_id=aws_account_id,
                    database_name=db_name_to_grant_permission_to,
                    table_wildcard=lf.CfnPermissions.TableWildcardProperty()
                )
            ),
            permissions=["ALTER", "INSERT", "DROP", "DELETE", "SELECT", "DESCRIBE"]
        )
        table_lf_permissions.add_dependency(role)
        table_lf_permissions.add_dependency(dependent_on_db)
        return table_lf_permissions

    def create_s3_bucket(
            self,
            bucket_name: str,
            query_results_prefix: str,
            queryresult_days_retention: int = None,
            db_days_retention: int = None,
    ) -> s3.Bucket:
        bucket = s3.Bucket(
            self,
            bucket_name,
            versioned=False,
            bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN
        )
        if not queryresult_days_retention is None:
            bucket.add_lifecycle_rule(
                expiration=Duration.days(queryresult_days_retention),
                prefix=f"{query_results_prefix}/"
            )
        if not db_days_retention is None:
            bucket.add_lifecycle_rule(
                expiration=Duration.days(db_days_retention),
                prefix="db/"
            )
        return bucket

    def create_lakeformation_resource(self, bucket: s3.Bucket, bucket_name: str) -> lf.CfnResource:
        lakeformation_resource = lf.CfnResource(
            self,
            id=f"{bucket_name}-DATALOCATIONPERMISSION",
            resource_arn=bucket.bucket_arn,
            use_service_linked_role=True
        )
        return lakeformation_resource

    '''
    step_role_name: name of role to create that will run the step jobs
    bucket_name: bucket that role will write to
    db_name: glue database name that role will write to
    workgroup_name: specific use case workgroup, traditionally 'etljobs'
    workgroup_bucket_name: bucket that role needs access to for using the right workgroup
    steps_role_prefix: normally 'usecase'
    glue_job_name_prefix: name that all glue job begins with that will be run by the role created. f.ex 'uc0039'
    '''
    def create_step_role(
            self,
            step_role_name: str,
            aws_region: str,
            aws_account_id: str,
            bucket_name: str,
            db_name: str,
            workgroup_name: str,
            workgroup_bucket_name: str,
            steps_role_prefix: str,
            glue_job_name_prefix: str,
    ) -> iam.Role:
        # Managed Policy
        lakeformation_policy = iam.ManagedPolicy.from_managed_policy_name(
            scope=self,
            id='LakeformationDataAccess',
            managed_policy_name='CustomPolicies/Services/Lakeformation/LakeformationDataAccess'
        )

        basic_execution_role = iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                "*"
            ],
        )

        lambda_policy = iam.PolicyStatement(
            actions=[
                'lambda:InvokeAsync',
                'lambda:InvokeFunction',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:lambda:{aws_region}:{aws_account_id}:function:*"
            ],
        )
        dynamodb_policy = iam.PolicyStatement(
            actions=[

                'dynamodb:CreateTable',
                'dynamodb:PartiQLSelect',
                'dynamodb:PartiQLUpdate',
                'dynamodb:PartiQLInsert',
                'dynamodb:BatchWriteItem',
                'dynamodb:Query',
                'dynamodb:PutItem',
                'dynamodb:PartiQLDelete',
                'dynamodb:DescribeTable',
                'dynamodb:Scan',
                'dynamodb:GetItem',
                'dynamodb:UpdateItem',
                'dynamodb:DeleteItem',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:dynamodb:{aws_region}:{aws_account_id}:*",
            ],
        )

        glue_policy = iam.PolicyStatement(
            actions=[
				'glue:StartJobRun',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:glue:{aws_region}:{aws_account_id}:job/{glue_job_name_prefix}*",
            ],
        )

        steps_policy = iam.PolicyStatement(
            actions=[
                'states:StartSyncExecution',
                'states:StartExecution',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:*"
            ],
        )

        '''
        taken from CustomServiceRoleForGlueETLJobs to get grant working in steps
        '''
        lakeformation_grant_permissions = iam.PolicyStatement(
            actions= [
                "events:PutTargets",
                "events:DescribeRule",
                "lakeformation:BatchGrantPermissions",
                "cloudwatch:PutMetricData",
                "lakeformation:GrantPermissions",
                "lambda:InvokeFunction",
                "lakeformation:GetDataAccess",
                "events:PutRule",
                "lakeformation:PutDataLakeSettings",
                "lakeformation:RevokePermissions",
                "glue:*",
                "lakeformation:BatchRevokePermissions"
                ],
            effect=iam.Effect.ALLOW,
            resources=["*"]
        )

        glue_get_table_policy = iam.PolicyStatement(
            actions=[
                'glue:GetTable',
                'glue:GetTables',
                'glue:GetDatabase',
                'glue:UpdateTable',
                'glue:BatchDeletePartition',
                'glue:BatchCreatePartition',
                'glue:CreatePartition',
                'glue:GetPartitions',
                'glue:GetPartition',
                'glue:DeletePartition',
                'glue:GetPartitionIndexes',
                'glue:UpdatePartition',
                'glue:DeletePartition',
                'glue:DeletePartitionIndex',
                'glue:CreatePartitionIndex',
                'glue:CreateTable',
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:glue:{aws_region}:{aws_account_id}:catalog",
                f"arn:aws:glue:{aws_region}:{aws_account_id}:database/{db_name}",
                f"arn:aws:glue:{aws_region}:{aws_account_id}:table/{db_name}/*"
            ],
        )

        athena_policy = iam.PolicyStatement(
            actions=[
                'athena:*'
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:athena:{aws_region}:{aws_account_id}:workgroup/{workgroup_name}",
                f"arn:aws:s3:::{workgroup_bucket_name}",
                f"arn:aws:s3:::{workgroup_bucket_name}/*"
            ]
        )

        step_function_execution_rule_policy = iam.PolicyStatement(
            actions=[
                'events:PutTargets',
                'events:PutRule',
                'events:DescribeRule'
            ],
            effect=iam.Effect.ALLOW,
            resources=[f"arn:aws:events:{aws_region}:{aws_account_id}:rule/*"]
        )

        s3_via_athena_policy = iam.PolicyStatement(
            actions=[
				"s3:GetBucketLocation",
				"s3:GetObject",
				"s3:CreateBucket",
				"s3:ListBucket",
				"s3:PutObject"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*",
                f"arn:aws:s3:::{workgroup_bucket_name}",
                f"arn:aws:s3:::{workgroup_bucket_name}/*"
            ],
            conditions={
                'ForAnyValue:StringEquals':{
                    'aws:CalledVia':'athena.amazonaws.com'
                }
            }
        )

        policy_doc = iam.PolicyDocument(
            statements=[
                basic_execution_role,
                lambda_policy,
                glue_policy,
                dynamodb_policy,
                steps_policy,
                glue_get_table_policy,
                athena_policy,
                step_function_execution_rule_policy,
                s3_via_athena_policy,
                lakeformation_grant_permissions
            ]
        )

        step_role = iam.Role(
            scope=self,
            id=f'{step_role_name}.json',
            role_name=f'{step_role_name}',
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("states.eu-west-1.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
                ),
            path=f"{steps_role_prefix}",
            managed_policies=[lakeformation_policy],
            inline_policies={
                f"{step_role_name}-inline-policy": policy_doc
            },

            #path=f"/odl_ds_ra/"
        )
        return step_role



    def create_dead_letter_queue(
            self,
            dlq_id,
            queue_name
    ) -> sqs.Queue:
        queue = sqs.Queue(
            scope=self,
            id=dlq_id,
            queue_name=queue_name
            )
        return queue
