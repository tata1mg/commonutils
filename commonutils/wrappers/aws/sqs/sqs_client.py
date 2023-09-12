from ..aws_client import AWSClient


class SQSClient(AWSClient):
    aws_service_name = "sqs"

    @classmethod
    async def create_sqs_client(
        cls,
        region_name: str,
        aws_secret_access_key=None,
        aws_access_key_id=None,
        **kwargs
    ):
        client = await cls.create_aws_client(
            cls.aws_service_name,
            region_name=region_name,
            aws_secret_access_key=aws_secret_access_key,
            aws_access_key_id=aws_access_key_id,
            **kwargs
        )
        return client
