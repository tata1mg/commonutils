import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from botocore.client import Config
from botocore.session import get_session

from commonutils.utils import Singleton


class BaseLambdaWrapper(metaclass=Singleton):
    DEFAULT_TIMEOUT_IN_SECONDS = 10

    def __init__(self, config: dict):
        self.config = config.get("LAMBDA", None)
        self.client = None
        self.arn_dict = {}

    async def get_client(self):
        if self.client:
            return self.client
        else:
            config = self.config
            aws_access_key_id = config.get("AWS_ACCESS_KEY_ID", None)
            aws_secret_access_key = config.get("AWS_SECRET_ACCESS_KEY", None)
            region_name = config.get("LAMBDA_REGION", "ap-south-1")
            connect_timeout = config.get(
                "connect_timeout", BaseLambdaWrapper.DEFAULT_TIMEOUT_IN_SECONDS
            )
            read_timeout = config.get(
                "read_timeout", BaseLambdaWrapper.DEFAULT_TIMEOUT_IN_SECONDS
            )
            signature_version = config.get("signature_version", None)
            endpoint_url = config.get("endpoint_url", None)
            config = Config(
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                signature_version=signature_version,
            )
            client_args = {
                "service_name": "lambda",
                "region_name": region_name,
                "config": config,
                "endpoint_url": endpoint_url,
                "aws_secret_access_key": aws_secret_access_key,
                "aws_access_key_id": aws_access_key_id,
            }
            with ThreadPoolExecutor(max_workers=1) as executor:
                loop = asyncio.get_running_loop()
                session = get_session()
                self.client = await loop.run_in_executor(
                    executor, partial(session.create_client, **client_args)
                )
            return self.client

    async def get_lambda_arn(self, lambda_name):
        arn = self.arn_dict.get(lambda_name, None)
        if arn:
            return arn
        else:
            with ThreadPoolExecutor(max_workers=1) as executor:
                loop = asyncio.get_running_loop()
                arn = await loop.run_in_executor(
                    executor, partial(self._add_lambda_arn, lambda_name)
                )
            return arn

    def _add_lambda_arn(self, lambda_name):
        response = self.client.get_function(FunctionName=lambda_name)
        arn = response.get("Configuration").get("FunctionArn")
        self.arn_dict[lambda_name] = arn
        return arn
