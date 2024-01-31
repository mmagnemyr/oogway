#!/usr/bin/env python3
import aws_cdk as cdk
from cdk_custom_stacks.steps_stack import StepsStack
from cdk_custom_stacks.infra_stack import InfraStack
from cdk_custom_stacks.env_variables import EnvVariables
from cdk_custom_stacks.generic_stack import GenericSsoRoleStack


APP = cdk.App()
ENV_VARS = EnvVariables(APP)


stack_settings= {
    **ENV_VARS.get_env_variable("STACK_INFRA"), 
    **ENV_VARS.get_env_variable("COMMON")
    }
INFRA=InfraStack (
    scope=APP,
    construct_id=f"{stack_settings['NAME']}-infra-stack-{stack_settings['AWS_ENVIRONMENT']}",
    stack_settings=stack_settings,
    description=f"Infrastructure {stack_settings['NAME']} {stack_settings['AWS_ENVIRONMENT']}."
)

stack_settings= {
    **ENV_VARS.get_env_variable("STACK_STEPS"),
    **ENV_VARS.get_env_variable("COMMON")
    }
StepsStack(
    scope=APP,
    construct_id=f"{stack_settings['NAME']}-steps-stack-{stack_settings['AWS_ENVIRONMENT']}",
    stack_settings=stack_settings,
    iam_step_role=INFRA.iam_step_role,
    description=f"{stack_settings['NAME']} steps copy job.{stack_settings['AWS_ENVIRONMENT']}."
)

stack_settings={
    **ENV_VARS.get_env_variable("SSO_ROLE"),
    **ENV_VARS.get_env_variable("COMMON")}
vpc_name = stack_settings[f"VPC_NAME_{stack_settings['AWS_ENVIRONMENT'].upper()}"]
sso_role = GenericSsoRoleStack(
    scope=APP,
    construct_id=f"{stack_settings['STACK_NAME']}-{stack_settings['AWS_ENVIRONMENT']}",
    stack_vars=stack_settings,
    usecase_name=stack_settings["USECASE_NAME"],
    aws_account_id=stack_settings["AWS_ACCOUNT"],
    aws_region=stack_settings["AWS_REGION"],
    env_name=stack_settings["AWS_ENVIRONMENT"],
    vpc_name=vpc_name,
    saml_provider_name=stack_settings["SAML_PROVIDER_NAME"],
    description=stack_settings["STACK_DESCRIPTION"]
)

APP.synth()
