from aiobotocore.config import AioConfig
from aiobotocore.endpoint import MAX_POOL_CONNECTIONS
from aiobotocore.session import get_session

from ...constants import ErrorMessages


class AWSClient:
    DEFAULT_TIMEOUT_IN_SECONDS = 10

    @classmethod
    async def create_aws_client(
        cls,
        aws_service_name: str,
        region_name: str,
        aws_secret_access_key=None,
        aws_access_key_id=None,
        **kwargs
    ):
        try:
            session = get_session()

            connect_timeout = (
                kwargs.get("connect_timeout") or cls.DEFAULT_TIMEOUT_IN_SECONDS
            )
            read_timeout = kwargs.get("read_timeout") or cls.DEFAULT_TIMEOUT_IN_SECONDS
            signature_version = kwargs.get("signature_version", None)
            endpoint_url = kwargs.get("endpoint_url")
            max_pool_connections = (
                kwargs.get("max_pool_connections") or MAX_POOL_CONNECTIONS
            )
            # _connector_args = {'limit': kwargs.get('concurrency_limit') or 0,
            #                    'limit_per_host': kwargs.get('concurrency_limit_host') or 0,
            #                    'enable_cleanup_closed': True}
            config = AioConfig(
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                signature_version=signature_version,
                max_pool_connections=max_pool_connections,
            )
            client_args = {
                "service_name": aws_service_name,
                "region_name": region_name,
                "config": config,
                "endpoint_url": endpoint_url,
                "aws_secret_access_key": aws_secret_access_key,
                "aws_access_key_id": aws_access_key_id,
            }
            client = session.create_client(**client_args)
            return client
        except Exception as error:
            raise Exception(ErrorMessages.AwsConnectionError.value.format(error=error))
