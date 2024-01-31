import os
import importlib
from constructs import Construct

# This module is required by VGCS and maintained by Delivery Engineering
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_iam as iam,
    aws_events_targets as targets,
    aws_sqs as sqs,
    Duration
)
from aws_cdk.aws_events import Rule, Schedule, RuleTargetInput, EventField

DE_PIPELINES = importlib.import_module("de-pipelines-constructs")

class StepsStack(DE_PIPELINES.VgcsStack):
    """
      stack for steps 
    """
    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            stack_settings: dict,
            iam_step_role: iam.Role,
            **kwargs
        ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        step_job_names = []
        steps_path = os.path.join(os.getcwd(),'deploy','aws_steps')
        for file_name in os.listdir(steps_path):
            if os.path.isfile(os.path.join(steps_path, file_name)):
                name, extension = os.path.splitext(file_name)
                if 'json' in extension:
                    step_job_names.append(name)
                else:
                    print(f"Warning: file {file_name} not a steps job, will not be deployed.")

        print(f"to deploy: {step_job_names}")
        for function_name in step_job_names:
            # Step Function main
            state_machine = self.create_step_function_from_json(
                function_name,
                iam_step_role.role_arn,
                stack_settings)
            if '_main' in function_name:
                # Event Rule
                rule = self.create_event_bridge_rule(state_machine.state_machine_name)
                rule.node.add_dependency(state_machine)

    def create_event_bridge_rule(
            self,
            state_machine_name: str
        ):
        state_machine = sfn.StateMachine.from_state_machine_name(
            self,
            id=f"{state_machine_name}-eventbridge",
            state_machine_name=state_machine_name
        )
        rule = Rule(
            scope=self,
            id=f'{state_machine_name}-eventbridge-rule-id-cdk',
            enabled=True,
            rule_name=f'{state_machine_name}-eventbridge-rule-cdk',
            schedule=Schedule.cron(hour='9', day='*', minute='45', month='*', year='*'),
        )
        rule.add_target(
            targets.SfnStateMachine(
                machine=state_machine,
                input=RuleTargetInput.from_object({
                    "force_rerun":"False",
                    "time":EventField.from_path('$.time'),
                    "process_date":EventField.from_path('$.time'),
                    "id":EventField.from_path('$.id'),
                    "parent_task_id":"EVENTBRIDGE",
                    "tag":"-"
                    }),
                dead_letter_queue=sqs.Queue(self, id=f'{state_machine_name}-sqs', retention_period=Duration.days(14))
                )
            )
        return rule


    def get_steps_string(self, file_name: str, path_to_file: str = os.path.join(os.getcwd(),'deploy','aws_steps')) -> str:
        '''
        Loads the specified sql-file into a variable and removes new-lines and unnecessary
        whitelines.
        Args:
            file_name (str): name of the file including .json suffix. E.g. uc0041_main_steps.json.
            path_to_file (str): full path up until the file without the trailing "/".
            Default is current directory + "/steps".
        Returns:
            steps_string (str): steps-string with no trailing new-lines or unnecessary whitelines
        '''
        file_with_path = f"{path_to_file}/{file_name}"
        ret = None
        with open(file_with_path) as file:
            ret = " ".join(file.read().split())
        return ret

    def create_step_function_from_json(
            self,
            name: str,
            role_arn: str,
            stack_settings: dict
        ) -> sfn.CfnStateMachine:
        """
            steps job from json file as if taken from aws console
            converted to what can be used with cdk CfnStateMachine.
        """
        definition_string = self.get_steps_string(f'{name}.json')
        return sfn.CfnStateMachine(
            scope=self,
            id=f'{name}-state-machine',
            state_machine_name=name,
            role_arn=role_arn,
            definition_string=definition_string,
            definition_substitutions={
                **{k: v for (k, v) in stack_settings.items() if isinstance(v, str)},
                "AWS_ENVIRONMENT_UPPER": stack_settings["AWS_ENVIRONMENT"].upper(),
                "AWS_ENVIRONMENT_LOWER": stack_settings["AWS_ENVIRONMENT"].lower()
            }
        )
