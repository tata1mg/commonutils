from enum import Enum


class ErrorMessages(Enum):
    AwsConnectionError = "Could not create connection with AWS: {error}"
    AwsS3InvalidFileTypeError = (
        "Invalid file type. Only {valid_content_types} are allowed"
    )
    AwsS3FileUploadWithResponseError = "Could not upload the file, response: {response}"
    AwsS3FileUploadErrorWithException = (
        "Exception occurred while uploading file {exception_type}:" " {exception}"
    )
    SomethingWentWrongError = "Something went wrong, Please try again, error: {error}"
    AWSS3InvalidContentTypeError = "Invalid aws s3 content type: {content_type}"
    MaxFileSizeExceeded = (
        "Max file size (in bytes) exceeded, max allowed file size: {max_size},"
        " uploaded file size: {uploaded_file_size}"
    )
    PresignedPostInvalidSuccessStatusError = (
        "Invalid success action status {invalid_status}"
    )
    PresignedUrlDoesNotExist = "Presigned url does not exist in aws"
    RateLimitExceeded = (
        "Rate limit exceeded - Operation: {action_name} identifier: {identifier}"
    )
    RateLimitUpdateError = "Error occurred in updating rate limit. Error: {error}"
    RateLimitFunctionError = (
        "Error occurred during function call func-name: {func_name}," " error: {error}"
    )
    AwsSQSConnectionError = "Problem connecting to SQS queue, error: {error}"
    PARAMETER_REQUIRED = "Required parameters {param_key} for {queue_name}"
    PARAMETERS_NOT_ALLOWED = "Parameters {param_key} not allowed for {queue_name}"
    AwsSQSPayloadSize = "Payload size exceeds SQS limit of 256 KBs."
    AwsSQSPublishError = "Error publishing to sqs: {error}, retrying count: {count}"
