from .sns_client import SNSClient


class BaseSNSWrapper:

    def __init__(self, config: dict, config_key: str = "SNS"):
        self.config = config.get(config_key, None) or dict()
        self._app_config = config
        self.client = None

    async def get_sns_client(self):
        aws_access_key_id = self.config.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = self.config.get("AWS_SECRET_ACCESS_KEY")
        region = self.config.get("SNS_REGION", "ap-south-1")
        endpoint_url = self.config.get("SNS_ENDPOINT_URL") or None
        max_connections = self.config.get("SNS_MAX_CONNECTIONS") or None
        connect_timeout = self.config.get("connect_timeout") or None
        read_timeout = self.config.get("read_timeout") or None
        signature_version = self.config.get("signature_version") or None
        concurrency_limit = self._app_config.get("CONCURRENCY_LIMIT") or 0
        concurrency_limit_host = self._app_config.get("CONCURRENCY_LIMIT_HOST") or 0
        client = await SNSClient.create_sns_client(
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
        return client

    async def publish_sms(self, message: str, phone_number: str, message_attributes: dict=None):
        if not self.client:
            await self.get_sns_client()
        resp = await self.client.publish(
            PhoneNumber=phone_number,
            Message=message,
            MessageAttributes=message_attributes
        )
        return resp
