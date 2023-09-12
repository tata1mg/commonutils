from ..aws_client import AWSClient


class S3Client(AWSClient):
    aws_service_name = "s3"

    @classmethod
    async def create_s3_client(
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
