from enum import Enum, unique

from ..utils import CustomEnum

DEFAULTS = {
    "UPLOAD_FOLDER": "dummy",
    "ACL": "public-read",
    "MAX_FILE_SIZE_IN_BYTES": 8 * (10**6),
    "PRESIGNED_POST": {
        "TTL_IN_SECONDS": 60 * 15,
        "SUCCESS_ACTION_STATUS": 201,
        "MAX_FILE_SIZE_IN_BYTES": (10**6) * 5,
        "ALLOWED_SUCCESS_ACTION_STATUS": {"200", "201", "204"},
    },
}

EVENT_SCHEDULER_CREATE_DEFINITION = {
    "Name": "",
    "Description": "",
    "GroupName": "",
    "ScheduleExpression": "",
    "FlexibleTimeWindow": {"Mode": "OFF"},
    "ScheduleExpressionTimezone": "Asia/Calcutta",
    "State": "ENABLED",
    "Target": {
        "Arn": "",
        "Input": "",
        "RetryPolicy": {"MaximumEventAgeInSeconds": 1800, "MaximumRetryAttempts": 10},
        "RoleArn": "",
    },
}


class Constant:
    UTF8 = "utf-8"
    SCHEDULER_ARN = "Arn"
    SCHEDULER_TARGET = "Target"
    SCHEDULER_EXPRESSION = "ScheduleExpression"
    SCHEDULER_GROUP_NAME = "GroupName"
    SCHEDULER_NEXT_TOKEN = "NextToken"
    SCHEDULER_NAME = "Name"
    AWS_DOMAIN = "amazonaws.com"
    AWS_SIGNED_HEADERS = "host;x-amz-date"


@unique
class PrefixType(Enum):
    ORDER = "O"


class SQSQueueType(CustomEnum):
    STANDARD_QUEUE = "sqs"
    STANDARD_QUEUE_FIFO = "sqs.fifo"


class DelayQueueTime(Enum):
    MINIMUM_TIME = 0
    MAXIMUM_TIME = 300


class AwsErrorType(Enum):
    SQSNotExist = "AWS.SimpleQueueService.NonExistentQueue"
    SQSRequestSizeExceeded = "AWS.SimpleQueueService.BatchRequestTooLong"


class EventBridgeSchedulerType(CustomEnum):
    SQS = "sqs"
    LAMBDA = "lambda"
