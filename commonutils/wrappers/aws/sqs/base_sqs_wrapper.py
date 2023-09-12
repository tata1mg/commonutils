import logging

import botocore.exceptions

from commonutils.handlers import SQSHandler

from ....constants import (AwsErrorType, DelayQueueTime, ErrorMessages,
                           SQSQueueType)
from .sqs_client import SQSClient

logger = logging.getLogger()


class BaseSQSWrapper:
    def __init__(self, config: dict, config_key: str = "SQS"):
        self.config = config.get(config_key, None)
        self._app_config = config
        self.client = None
        self.queue_url = None

    async def get_sqs_client(self, queue_name=""):
        aws_access_key_id = self.config.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = self.config.get("AWS_SECRET_ACCESS_KEY")
        region = self.config.get("SQS_REGION", "ap-south-1")
        endpoint_url = self.config.get("SQS_ENDPOINT_URL") or None
        max_connections = self.config.get("SQS_MAX_CONNECTIONS") or None
        connect_timeout = self.config.get("connect_timeout") or None
        read_timeout = self.config.get("read_timeout") or None
        signature_version = self.config.get("signature_version") or None
        concurrency_limit = self._app_config.get("CONCURRENCY_LIMIT") or 0
        concurrency_limit_host = self._app_config.get("CONCURRENCY_LIMIT_HOST") or 0
        client = await SQSClient.create_sqs_client(
            region,
            aws_secret_access_key=aws_secret_access_key,
            aws_access_key_id=aws_access_key_id,
            endpoint_url=endpoint_url,
            max_pool_connections=max_connections,
            concurrency_limit=concurrency_limit,
            concurrency_limit_host=concurrency_limit_host,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            signature_version=signature_version,
        )

        self.client = await client.__aenter__()
        if client:
            queue_url = await self.get_queue_url(queue_name)
            self.queue_url = queue_url
        return client

    async def get_queue_url(self, queue_name):
        try:
            response = await self.client.get_queue_url(QueueName=queue_name)
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == AwsErrorType.SQSNotExist.value:
                raise Exception(
                    ErrorMessages.AwsSQSConnectionError.value.format(
                        error=err.response["Error"]["Code"]
                    )
                )
            else:
                raise Exception(
                    ErrorMessages.AwsSQSConnectionError.value.format(
                        error=err.response["Error"]["Code"]
                    )
                )
        queue_url = response.get("QueueUrl")
        return queue_url

    async def get_queue_arn(self, queue_name):
        queue_url = await self.get_queue_url(queue_name)
        response = await self.get_queue_attributes(
            queue_url=queue_url, attribute_names=["QueueArn"]
        )
        return response.get("Attributes").get("QueueArn")

    async def subscribe(self, **kwargs):
        max_no_of_messages = kwargs.get("max_no_of_messages") or 1
        wait_time_in_seconds = kwargs.get("wait_time_in_seconds") or 5
        message_attribute_names = kwargs.get("message_attribute_names") or ["All"]
        attribute_names = kwargs.get("attribute_names") or ["All"]

        if kwargs.get("visibility_timeout") is not None:
            visibility_timeout = kwargs.get("visibility_timeout")
            messages = await self.client.receive_message(
                QueueUrl=self.queue_url,
                WaitTimeSeconds=wait_time_in_seconds,
                MaxNumberOfMessages=max_no_of_messages,
                AttributeNames=attribute_names,
                MessageAttributeNames=message_attribute_names,
                VisibilityTimeout=visibility_timeout,
            )
        else:
            messages = await self.client.receive_message(
                QueueUrl=self.queue_url,
                WaitTimeSeconds=wait_time_in_seconds,
                MaxNumberOfMessages=max_no_of_messages,
                AttributeNames=attribute_names,
                MessageAttributeNames=message_attribute_names,
            )
        return messages

    async def subscribe_all(self, event_handler: SQSHandler, **kwargs):
        """
        Subscribe and process SQS messages
        :param event_handler: Pass your implementation od SQSHandler having a method handle_event(body)
        """

        while True:
            try:
                response = await self.subscribe(**kwargs)
                messages = response.get("Messages") if response else None
                if messages:
                    for message in messages:
                        try:
                            body = message["Body"]
                            receipt_handle = message["ReceiptHandle"]
                            await event_handler.handle_event(body)
                            logger.debug("Successfully processed SQS message")
                            await self.purge(receipt_handle=receipt_handle)
                        except Exception as e:
                            logger.exception(
                                "Exception while processing SQS message {}".format(
                                    str(e)
                                )
                            )
            except Exception as e:
                logger.exception(
                    "Exception while fetching SQS messages {}".format(str(e))
                )

    async def close(self):
        await self.client.close()

    async def purge(self, receipt_handle):
        await self.client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )

    async def publish_to_sqs(
        self,
        messages: list = None,
        attributes: dict = None,
        payload: dict = None,
        batch=True,
        **kwargs
    ):
        """
        :param messages: entity payload to be sent to SQS queue for batch requests
        :param payload: entity payload to be sent to SQS queue for not batch requests (single events)
        :param attributes: message attributes related to payload
        :param batch: tells if request is a batch request or not.
        :return: True or False
        """
        messages, attributes, payload = messages or [], attributes or {}, payload or ""
        message_group_id, message_deduplication_id = kwargs.get(
            "message_group_id"
        ), kwargs.get("message_deduplication_id")
        delay_seconds = kwargs.get("delay_seconds", DelayQueueTime.MINIMUM_TIME.value)
        queue_type = self.get_queue_type()
        self._validate_publish_to_sqs(
            queue_type, message_group_id, message_deduplication_id
        )
        _send, _retry_count, sent_response_data = False, 0, {}
        _max_retries = kwargs.get("max_retries") or 3
        while _send is not True and _retry_count < _max_retries:
            try:
                if not batch:
                    send_message_data = {
                        "QueueUrl": self.queue_url,
                        "MessageBody": payload,
                        "MessageAttributes": attributes,
                    }

                    if queue_type == SQSQueueType.STANDARD_QUEUE_FIFO.value:
                        send_message_data["MessageGroupId"] = message_group_id
                        if message_deduplication_id:
                            send_message_data[
                                "MessageDeduplicationId"
                            ] = message_deduplication_id
                    elif (
                        delay_seconds
                        and isinstance(delay_seconds, int)
                        and DelayQueueTime.MINIMUM_TIME.value
                        < delay_seconds
                        < DelayQueueTime.MAXIMUM_TIME.value
                    ):
                        send_message_data.update({"DelaySeconds": delay_seconds})

                    sent_response_data = await self.client.send_message(
                        **send_message_data
                    )
                else:
                    sent_response_data = await self.client.send_message_batch(
                        QueueUrl=self.queue_url, Entries=messages
                    )
                _send = True
            except botocore.exceptions.ClientError as err:
                if (
                    err.response["Error"]["Code"]
                    == AwsErrorType.SQSRequestSizeExceeded.value
                ):
                    raise Exception(ErrorMessages.AwsSQSPayloadSize.value)
                else:
                    logger.info(
                        ErrorMessages.AwsSQSPublishError.value.format(
                            error=err, count=_retry_count
                        )
                    )
            except Exception as e:
                logger.info(
                    ErrorMessages.AwsSQSPublishError.value.format(
                        error=e, count=_retry_count
                    )
                )
            finally:
                _retry_count += 1
        if kwargs.get("return_response") and _send:
            return sent_response_data
        return _send

    @staticmethod
    def _validate_publish_to_sqs(
        queue_type, message_group_id, message_deduplication_id
    ):
        if queue_type == SQSQueueType.STANDARD_QUEUE_FIFO.value:
            if not message_group_id:
                raise Exception(
                    ErrorMessages.PARAMETER_REQUIRED.value.format(
                        param_key="message_group_id", queue_name="sqs fifo queue push"
                    )
                )
        else:
            if message_group_id or message_deduplication_id:
                raise Exception(
                    ErrorMessages.PARAMETERS_NOT_ALLOWED.value.format(
                        param_key="message_group_id and message_deduplication_id",
                        queue_name="sqs standard queue push",
                    )
                )

    def get_queue_type(self):
        if ".fifo" in self.queue_url:
            return SQSQueueType.STANDARD_QUEUE_FIFO.value
        return SQSQueueType.STANDARD_QUEUE.value

    async def get_queue_attributes(self, queue_url, attribute_names=[]):
        try:
            response = await self.client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=attribute_names
            )
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == AwsErrorType.SQSNotExist.value:
                raise Exception(
                    ErrorMessages.AwsSQSConnectionError.value.format(
                        error=err.response["Error"]["Code"]
                    )
                )
            else:
                raise Exception(
                    ErrorMessages.AwsSQSConnectionError.value.format(
                        error=err.response["Error"]["Code"]
                    )
                )
        return response
