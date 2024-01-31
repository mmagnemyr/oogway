class stringtemplate:
   
    
    awssteps_jobqueue_state_machine_name="[:REPLACE_SOLUTION_NAME]_jobqueue_steps"
    awssteps_main_load_state_machine_name="[:REPLACE_SOLUTION_NAME]_main_[:REPLACE_TABLE_NAME]_steps"
    awssteps_jq_array_state_machine_name="[:REPLACE_SOLUTION_NAME]_jobqueue_array_steps"
    awssteps_main_load_state_machine_name="[:REPLACE_SOLUTION_NAME]_main_load_steps"

   

    cdk_create_step_function_from_json = "def create_step_function_from_json( \
        self, name:str, role_arn:str, stack_settings dict)-> \
        sfn.CfnStateMachine: definition_string = self.get_steps_string('[:REPLACE_STEPS_NAME].json') \
        return sfn.CfnStateMachine ( scope=self,id=id=f'{name}-state-machine',state_machine_name=name, \
        role_arn=role_arn,definition_string = definition_string, \
        definition_substitutions={**{k: v for (k, v) in stack_settings.items() if (isinstance(v,str))},  'AWS_ENVIRONMENT_UPPERCASE': stack_settings['AWS_ENVIRONMENT'].upper()})"

    cdk_create_step_function_invokation = " state_machine = self.create_step_function_from_json( '[:REPLACE_STEP_FUNCTION_NAME]', \
          role_iam.role_arn, stack_settings)"

class replacetags:
    replace_table_name = "[:REPLACE_TABLE_NAME]"
    table_name = "[:REPLACE_TABLE_NAME]"
    replace_solution_name = "[:REPLACE_SOLUTION_NAME]"

    #AWS
    aws_region="[:REPLACE_AWS_REGION]"
    aws_account = "[:REPLACE_AWS_ACCOUNT]"
    aws_state_machine_name="[:REPLACE_STATE_MACHINE_NAME]"
    aws_environment_uppercase="[:REPLACE_AWS_ENVIRONMENT_UPPERCASE]"
    aws_environment_lowercase="[:REPLACE_AWS_ENVIRONMENT_LOWERCASE]"

    view_name = "[:REPLACE_VIEW_NAME]"
    replace_list_of_column_names_incl_partition ="[:REPLACE_LIST_COLUMN_NAME_INCL_PARTITION]"
    sql="[:REPLACE_SQL]"
    replace_cols="[:REPLACE_COLS]"
    replace_concat_cols = "[:REPLACE_CONCAT_COL_LIST]"
    
    #hub key col name (2)
    hub_key_col_name = "[:REPLACE_HUB_KEY_COL_NAME]"
    replace_hk_col_name = "[:REPLACE_HK_COL_NAME]"
    
    replace_col_list_with_datatypes="[:REPLACE_LIST_COLUMN_NAME_WITH_DATATYPES]"
    replace_view_name = "[:REPLACE_VIEW_NAME]"
    replace_list_partition_by_column_list="[:REPLACE_LIST_PARTITION_BY_COLUMN_LIST]"
    replace_s3_path="[:REPLACE_S3_PATH]"
    replace_sql="[:REPLACE_SQL]"

    

