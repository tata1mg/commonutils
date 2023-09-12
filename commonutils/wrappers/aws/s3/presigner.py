from commonutils.constants import ErrorMessages

from .base_s3_wrapper import BaseS3Wrapper
from .s3_client import S3Client


class Presigner(BaseS3Wrapper):
    """
    author: @himanshulakra
    Wrapper class over AWS S3 client presigning URL functions
    Note:
         1. IAM Role Credentials are used to create S3 client which have expiration of 6 hours.
         2. Url will have expiry equal to the time minimum of
            (expires_in passed as argument and IAM Role credentials expiry time).
            - So If you use IAM Role Credentials 5 mins before its expiration to generate Presigned URL with
            expiration time of 1 hour the the URL will expire within 5 mins.
         3. IAM Role Credentials usually rotate in around 1 hour on AWS instance.
         4. That's why we are using new S3 client in each call of this function to support the
            expires_in value correctly.
    """

    DEFAULT_PRESIGNED_URL_EXPIRY = 1800  # 30 minutes
    PRESIGNER_GET_CLIENT_METHOD_NAME = "get_object"

    def __init__(self, config: dict):
        super().__init__(config, None)

    async def presigned_get_url(
        self, bucket_name, object_name, expires_in=DEFAULT_PRESIGNED_URL_EXPIRY
    ):
        """Generate a presigned URL to share an S3 object
        :param bucket_name: string
        :param object_name: string (with extension of file object)
        :param expires_in: Time in seconds for then Presigned URL to remain valid
        :return: Presigned URL as string. If error, return error with formatted message.
        """

        try:
            s3_client = await self._s3_client()
            presigned_get_url = await s3_client.generate_presigned_url(
                self.PRESIGNER_GET_CLIENT_METHOD_NAME,
                Params={"Bucket": bucket_name, "Key": object_name},
                ExpiresIn=expires_in,
            )
        except Exception as error:
            error_message = "Exception for Object {}/{} : {} => {}".format(
                bucket_name, object_name, type(error), error
            )
            raise Exception(
                ErrorMessages.SomethingWentWrongError.value.format(error=error_message)
            )
        finally:
            # closing s3 client session
            if s3_client is not None:
                await s3_client.close()

        return presigned_get_url

    async def get_presigned_url(
        self,
        bucket_name,
        object_name,
        operation=PRESIGNER_GET_CLIENT_METHOD_NAME,
        expires_in=DEFAULT_PRESIGNED_URL_EXPIRY,
    ):
        """Generate a presigned URL to operate on a S3 object
        :param bucket_name: string
        :param object_name: string (with extension of file object)
        :param operation: string ('get_object'|'put_object',..etc.)
        :param expires_in: Time in seconds for then Presigned URL to remain valid
        :return: Presigned URL as string. If error, return error with formatted message.
        """

        try:
            s3_client = await self._s3_client()
            presigned_url = await s3_client.generate_presigned_url(
                operation,
                Params={"Bucket": bucket_name, "Key": object_name},
                ExpiresIn=expires_in,
            )
        except Exception as error:
            error_message = "Exception for Object {}/{} : {} => {}".format(
                bucket_name, object_name, type(error), error
            )
            raise Exception(
                ErrorMessages.SomethingWentWrongError.value.format(error=error_message)
            )
        finally:
            # closing s3 client session
            if s3_client is not None:
                await s3_client.close()

        return presigned_url

    async def _s3_client(self):
        """
        Function returns S3 client if it is present otherwise generates S3 client
        with config passed on to this class.
        :return: AioBaseClient S3 Object
        """
        return await self._create_s3_client()

    async def _create_s3_client(self):
        """
        We are using IAM Role Credentials for the Presign functions so there is no need to pass
        the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, that will be taken care of by AWS SDK.
        :return: S3 client object (built from config attr)
        """

        timeout_config = self.config.get("TIMEOUT", {})
        connect_timeout, read_timeout = timeout_config.get(
            "CONNECT"
        ), timeout_config.get("READ")
        client = await S3Client.create_s3_client(
            self.config.get("S3_REGION"),
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            signature_version=self.config.get("SIG_VERSION"),
        )
        return await client.__aenter__()
