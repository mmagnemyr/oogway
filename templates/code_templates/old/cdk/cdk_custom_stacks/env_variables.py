import os
import json
from typing import Any
from aws_cdk import App

class EnvVariables():
    ''' 
    handles environment vars and settings that are set by pipelines and cdk.json
    '''

    def __init__(self, app: App):
        self.app = app
        # Get enviroment variables from Pipelines
        self.aws_account_id = self.get_env_variable('DE_DEPLOY_ACCOUNT')
        self.aws_region = self.get_env_variable('DE_DEPLOY_REGION')
        self.env_name = self.get_env_variable('DE_DEPLOY_ENVIRONMENT')
        self.env_name_upper = self.get_env_variable('DE_DEPLOY_ENVIRONMENT').upper()
        self.env_name_lower = self.get_env_variable('DE_DEPLOY_ENVIRONMENT').lower()

        self.app.node.set_context('DE_DEPLOY_ACCOUNT', self.aws_account_id)
        self.app.node.set_context('DE_DEPLOY_REGION', self.aws_region)
        self.app.node.set_context('DE_DEPLOY_ENVIRONMENT', self.env_name)
        self.app.node.set_context('DE_DEPLOY_ENVIRONMENT_UPPER', self.env_name_upper)

    def get_env_variable(self, var: str) -> Any:
        '''
        get var primarily from the os, else from cdk.json file.
        '''
        return os.environ.get(var, self.__get_context_variable(var))

    def __env_replace(self, var):
        return var.replace(
            "${AWS_ACCOUNT}", 
            os.environ.get("DE_DEPLOY_ACCOUNT", self.app.node.try_get_context("DE_DEPLOY_ACCOUNT"))) \
        .replace(
            "${AWS_REGION}", 
            os.environ.get("DE_DEPLOY_REGION", self.app.node.try_get_context("DE_DEPLOY_REGION"))) \
        .replace(
            "${AWS_ENVIRONMENT}", 
            os.environ.get("DE_DEPLOY_ENVIRONMENT", self.app.node.try_get_context("DE_DEPLOY_ENVIRONMENT"))) \
        .replace(
            "${AWS_ENVIRONMENT_LOWER}", 
            os.environ.get("DE_DEPLOY_ENVIRONMENT", self.app.node.try_get_context("DE_DEPLOY_ENVIRONMENT")).lower()) \
        .replace(
            "${AWS_ENVIRONMENT_UPPER}", 
            os.environ.get("DE_DEPLOY_ENVIRONMENT", self.app.node.try_get_context("DE_DEPLOY_ENVIRONMENT")).upper())

    def __get_context_variable(self, var: str) -> Any:
        '''
        private class function to get var from cdk.json file.
        '''
        context_var = self.app.node.try_get_context(var.upper())

        if context_var == "" :
            context_var = "Unknown"
        elif isinstance(context_var, dict):
            var_str = self.__env_replace(json.dumps(context_var))
            context_var = json.loads(var_str)
        elif isinstance(context_var, list):
            context_var = [self.__env_replace(s) for s in context_var]
        else:
            context_var = self.__env_replace(context_var)
        return context_var
