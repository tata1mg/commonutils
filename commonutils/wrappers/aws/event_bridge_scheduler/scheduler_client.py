import asyncio
import copy
import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from aiohttp import ContentTypeError
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.session import get_session

logger = logging.getLogger()

from commonutils.base_api_request import BaseApiRequest
from commonutils.constants import (EVENT_SCHEDULER_CREATE_DEFINITION, Constant,
                                   EventBridgeSchedulerType)
from commonutils.handlers import SQSHandler
from commonutils.utils import Singleton
from commonutils.wrappers.aws.lambdaa import BaseLambdaWrapper
from commonutils.wrappers.aws.sqs import BaseSQSWrapper


class SchedulerClientWrapper(metaclass=Singleton):
    def __init__(self, config: dict):
        self.config = config
        self.event_scheduler_config = self.config.get("EVENT_SCHEDULER", {})
        self.group_name = self.event_scheduler_config.get(
            "SCHEDULER_GROUP_NAME", "default"
        )
        self.queue_name = self.event_scheduler_config.get("SQS_QUEUE_NAME")
        self.sqs_arn_dict = {}
        self.base_sqs_wrapper = None
        self.lambda_wrapper = None
        self.aws_region = self.event_scheduler_config.get(
            "EVENT_SCHEDULER_REGION", "ap-south-1"
        )
        self.aws_service = "scheduler"
        self.path = "/schedules/{}"
        self.aws_access_key = self.event_scheduler_config.get("AWS_ACCESS_KEY_ID", None)
        self.aws_secret_key = self.event_scheduler_config.get(
            "AWS_SECRET_ACCESS_KEY", None
        )

        if self.aws_access_key is None or self.aws_secret_key is None:
            # load secret key or access key from Environment Variables or AWS Config file
            session = get_session()
            self.aws_credentials = session.get_credentials()
        else:
            self.aws_credentials = Credentials(self.aws_access_key, self.aws_secret_key)
        self.aws_sigv4 = SigV4Auth(
            self.aws_credentials, self.aws_service, self.aws_region
        )

    async def initialize_event_scheduler(self, event_handler: SQSHandler = None):
        """
        Initializes schedule client, creates scheduled group and
        subscribes to queue to process SQS messages, if SQS_QUEUE_NAME
        present in EVENT_SCHEDULER config
        """
        await self._create_schedule_group()
        if len(self.queue_name) > 0:
            await self._initialise_sqs_client(self.config)
            asyncio.ensure_future(
                asyncio.shield(self.base_sqs_wrapper.subscribe_all(event_handler))
            )
        if self.config.get("LAMBDA"):
            await self._initialise_lambda_client(self.config)
        return None

    async def _initialise_sqs_client(self, config=None):
        self.base_sqs_wrapper = BaseSQSWrapper(config)
        await self.base_sqs_wrapper.get_sqs_client(self.queue_name)
        return None

    async def _initialise_lambda_client(self, config=None):
        self.lambda_wrapper = BaseLambdaWrapper(config)
        await self.lambda_wrapper.get_client()
        return None

    async def _create_schedule_group(self):
        schedule_group_definition = {"ClientToken": str(uuid.uuid1())}
        full_path = f"/schedule-groups/{self.group_name}"
        await self._call_aws_api(full_path, "POST", schedule_group_definition)

    async def create_sqs_event_schedule(
        self,
        schedule_name,
        schedule_expression,
        msg="",
        schedule_description="",
        queue_name=None,
    ):
        """
        Creates an Event Bridge Schedule
        :param schedule_name: Name of the schedule
        :param schedule_expression: Rate at which schedule should run.
        Example:
        1. One time job: at(2023-06-02T13:30:00)
        2. Job running at a regular interval: rate(1 minutes)
        3. Job running at a regular interval using CRON: cron(fields)
        A cron expression consists of six fields separated by white spaces:
        (minutes hours day_of_month month day_of_week year)
        :param msg: Job Context, json that would be posted to SQS queue
        :param schedule_description: Description of this schedule job
        :param queue_name: Target queue name, if not provided defaulted to queue mentioned in the EVENT_SCHEDULER config

        """

        schedule_definition = SchedulerDefinition().event_scheduler_definition

        if not queue_name:
            queue_name = self.queue_name

        await self._create_schedule_definition(
            msg,
            schedule_definition,
            schedule_description,
            schedule_expression,
            schedule_name,
            queue_name,
            EventBridgeSchedulerType.SQS.value,
        )

        await self._create_schedule(schedule_definition, schedule_name)

    async def _create_schedule(self, schedule_definition, schedule_name):
        schedule_definition["ClientToken"] = str(uuid.uuid1())
        full_path = self.path.format(schedule_name)
        await self._call_aws_api(full_path, "POST", schedule_definition)

    async def _call_aws_api(self, full_path, method, schedule_definition):
        url = f"https://{self.aws_service}.{self.aws_region}.{Constant.AWS_DOMAIN}{full_path}"
        headers = await self._get_aws_auth_headers(
            method, url, payload=schedule_definition
        )

        aws_response, status = None, None
        aiohttp_session = await BaseApiRequest.get_session()

        async with aiohttp_session.request(
            method, str(url), headers=headers, json=schedule_definition, timeout=60
        ) as response:
            status = response.status
            if status != 200:
                aws_response = await response.text()
                _msg = "Error is AWS Schedule API with status code : {}, message: {}".format(
                    status, aws_response
                )
                logger.error(_msg)
                raise Exception(_msg)
            else:
                try:
                    aws_response = await response.json()
                except ContentTypeError as e:
                    # in some api like delete schedule aws sends empty response with 200 status code
                    pass

        return status, aws_response

    async def create_lambda_event_schedule(
        self,
        schedule_name,
        target_resource_name,
        schedule_expression,
        msg="",
        schedule_description="",
    ):
        """
        Creates an Event Bridge Schedule
        :param schedule_name: Name of the schedule
        :param target_resource_name: Name of the target Lambda that will be triggered
        :param schedule_expression: Rate at which schedule should run.
        Example:
        1. One time job: at(2023-06-02T13:30:00)
        2. Job running at a regular interval: rate(1 minutes)
        3. Job running at a regular interval using CRON: cron(fields)
        A cron expression consists of six fields separated by white spaces:
        (minutes hours day_of_month month day_of_week year)
        :param msg: Job Context, context that would be supplied to Lambda
        :param schedule_description: Description of this schedule job
        """

        schedule_definition = SchedulerDefinition().event_scheduler_definition

        await self._create_schedule_definition(
            msg,
            schedule_definition,
            schedule_description,
            schedule_expression,
            schedule_name,
            target_resource_name,
            EventBridgeSchedulerType.LAMBDA.value,
        )

        await self._create_schedule(schedule_definition, schedule_name)

    async def get_event_schedule(self, schedule_name):
        """
        Fetches an Event Bridge Schedule
        :param schedule_name: Name of the schedule to be retrieved
        """

        full_path = self.path.format(schedule_name + "?groupName=" + self.group_name)
        status, aws_response = await self._call_aws_api(full_path, "GET", None)
        return aws_response

    async def delete_event_schedule(self, schedule_name):
        """
        Deletes an Event Bridge Schedule
        :param schedule_name: Name of the schedule to be deleted
        """
        client_token = str(uuid.uuid1())
        full_path = self.path.format(
            schedule_name
            + "?clientToken="
            + client_token
            + "&groupName="
            + self.group_name
        )
        await self._call_aws_api(full_path, "DELETE", None)

    async def update_event_schedule(
        self,
        schedule_name,
        new_schedule_expression,
        new_msg="",
        new_schedule_description="",
        new_target_resource_name="",
        new_target_type="",
    ):
        """
        Updates an Event Bridge Schedule
        :param schedule_name: Name of the schedule to be updated
        :param new_schedule_expression: New schedule expression
        :param new_msg: New context for the schedule.
        :param new_schedule_description: New description of this schedule job
        :param new_target_resource_name: New Target resource name
        :param new_target_type: New Target Type sqs or lambda

        """
        event_schedule = await self.get_event_schedule(schedule_name)
        if event_schedule:
            del event_schedule["CreationDate"]
            del event_schedule["LastModificationDate"]
            event_schedule["ClientToken"] = str(uuid.uuid1())

            if len(new_schedule_description) > 0:
                event_schedule["Description"] = new_schedule_description

            event_schedule[Constant.SCHEDULER_EXPRESSION] = new_schedule_expression

            if len(new_msg) > 0:
                event_schedule[Constant.SCHEDULER_TARGET]["Input"] = new_msg

            if len(new_target_type) > 0 and len(new_target_resource_name) > 0:
                await self._add_target_definition(
                    schedule_definition=event_schedule,
                    target_resource_name=new_target_resource_name,
                    target_type=new_target_type,
                )
            full_path = self.path.format(schedule_name)
            await self._call_aws_api(full_path, "PUT", event_schedule)
        else:
            logger.info("Schedule not found, update schedule failed..")

    async def _create_schedule_definition(
        self,
        msg,
        schedule_definition,
        schedule_description,
        schedule_expression,
        schedule_name,
        target_resource_name,
        target_type,
    ):

        schedule_definition[Constant.SCHEDULER_NAME] = schedule_name
        schedule_definition[Constant.SCHEDULER_GROUP_NAME] = self.group_name
        schedule_definition["Description"] = schedule_description
        schedule_definition[Constant.SCHEDULER_EXPRESSION] = schedule_expression
        schedule_definition[Constant.SCHEDULER_TARGET]["Input"] = msg
        await self._add_target_definition(
            schedule_definition, target_resource_name, target_type
        )

    async def _add_target_definition(
        self, schedule_definition, target_resource_name, target_type
    ):
        if target_type.strip() == EventBridgeSchedulerType.SQS.value:
            arn = await self._get_sqs_arn(target_resource_name)
            _account_id = arn.split(":")[4]
            schedule_definition[Constant.SCHEDULER_TARGET][Constant.SCHEDULER_ARN] = arn
            schedule_definition[Constant.SCHEDULER_TARGET][
                "RoleArn"
            ] = "arn:aws:iam::{}:role/service-role/Amazon_EventBridge_Scheduler_SQS".format(
                _account_id
            )

        elif target_type.strip() == EventBridgeSchedulerType.LAMBDA.value:
            arn = await self._get_lambda_arn(target_resource_name)
            _account_id = arn.split(":")[4]
            schedule_definition[Constant.SCHEDULER_TARGET][Constant.SCHEDULER_ARN] = arn
            schedule_definition[Constant.SCHEDULER_TARGET][
                "RoleArn"
            ] = "arn:aws:iam::{}:role/service-role/Amazon_EventBridge_Scheduler_LAMBDA".format(
                _account_id
            )
        logger.debug("schedule definition {}".format(schedule_definition))

    async def _get_sqs_arn(self, target_resource_name):
        arn = self.sqs_arn_dict.get(target_resource_name, None)
        if arn:
            return arn
        else:
            arn = await self._add_sqs_arn(target_resource_name)
            return arn

    async def _add_sqs_arn(self, sqs_name):
        arn = await self.base_sqs_wrapper.get_queue_arn(sqs_name)
        self.sqs_arn_dict[sqs_name] = arn
        return arn

    async def _get_lambda_arn(self, target_resource_name):
        arn = await self.lambda_wrapper.get_lambda_arn(target_resource_name)
        return arn

    async def _get_aws_auth_headers(self, method, full_path, payload=""):
        if not payload:
            data = None
        else:
            data = json.dumps(payload).encode("utf-8")
        request = AWSRequest(
            method=method,
            url=full_path,
            headers={
                "Host": f"{self.aws_service}.{self.aws_region}.{Constant.AWS_DOMAIN}"
            },
            data=data,
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                executor, partial(self.aws_sigv4.add_auth, request)
            )
        return dict(request.headers)


class SchedulerDefinition:
    def __init__(self):
        self.event_scheduler_definition = copy.deepcopy(
            EVENT_SCHEDULER_CREATE_DEFINITION
        )
