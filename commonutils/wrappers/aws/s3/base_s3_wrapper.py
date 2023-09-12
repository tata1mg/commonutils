import asyncio
import logging
import uuid
from functools import wraps
from sys import getsizeof
from urllib.parse import unquote

from botocore.session import get_session

from commonutils.base_api_request import BaseApiRequest
from commonutils.constants import DEFAULTS, ErrorMessages, HttpHeaderType
from commonutils.utils import Singleton, get_file_extension_from_content_type

from .s3_client import S3Client

logger = logging.getLogger()


def create_client(func):
    @wraps(func)
    async def _create_client(*args, **kwargs):
        if args[0].client is None:
            args[0].client = await args[0]._create_s3_client()
        result = await func(*args, **kwargs)
        return result

    return _create_client


class BaseS3Wrapper(metaclass=Singleton):
    presigned_post_defaults = DEFAULTS["PRESIGNED_POST"]
    default_content_types = ["application/pdf", "image", "txt", "pdf", "json"]

    def __init__(self, config: dict, client, allowed_content_types=None):
        self.client = client
        self.config = config
        self.allowed_content_types = allowed_content_types

    @create_client
    async def upload(self, file, content_type, key=None):
        self.validate(file)
        config = self.config
        bucket = config["S3_BUCKET"]
        region = config["S3_REGION"]
        path = config.get("S3_PATH", DEFAULTS["UPLOAD_FOLDER"])
        acl = config.get("ACL", DEFAULTS["ACL"])
        resource_type, file_format = content_type.split("/")

        if not key:
            full_file_name = self._get_file_name(content_type)
            key = self._get_file_key(path, full_file_name)

        valid_content_types = self.allowed_content_types or self.default_content_types
        if not any(x in content_type for x in valid_content_types):
            raise Exception(
                ErrorMessages.AwsS3InvalidFileTypeError.value.format(
                    valid_content_types=", ".join(valid_content_types)
                )
            )

        try:
            # upload object to amazon s3
            if acl == "no-acl":
                resp = await self.client.put_object(
                    Bucket=bucket, Key=key, Body=file, ContentType=content_type
                )
            else:
                resp = await self.client.put_object(
                    Bucket=bucket, Key=key, Body=file, ContentType=content_type, ACL=acl
                )
            if resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
                url = "https://s3.{}.amazonaws.com/{}/{}".format(region, bucket, key)
            else:
                raise Exception(
                    ErrorMessages.AwsS3FileUploadWithResponseError.value.format(
                        response=resp
                    )
                )
        except Exception as error:
            exception_type = type(error).__name__
            raise Exception(
                ErrorMessages.AwsS3FileUploadErrorWithException.value.format(
                    exception_type=exception_type, exception=error
                )
            )
        return {
            "file_url": url,
            "public_id": key,
            "resource_type": "image",
            "format": file_format,
            "file_check_sum": resp.get("ETag").replace('"', ""),
            "file_name": key,
        }

    async def generate_presigned_post(
        self,
        content_type: str,
        file_name: str = None,
        ttl_in_seconds=None,
        upload_directory: str = None,
        **kwargs
    ):
        ttl_in_seconds = (
            ttl_in_seconds or self.presigned_post_defaults["TTL_IN_SECONDS"]
        )
        success_action_status = str(
            kwargs.get(
                "success_action_status",
                self.presigned_post_defaults["SUCCESS_ACTION_STATUS"],
            )
        )
        file_size_in_bytes = kwargs.get("file_size_in_bytes")
        acl = DEFAULTS["ACL"]
        self._validate_presigned_post(
            content_type, file_size_in_bytes, success_action_status
        )
        upload_directory_ = (
            upload_directory
            or self.presigned_post_defaults.get("UPLOAD_FOLDER")
            or DEFAULTS["UPLOAD_FOLDER"]
        )
        file_name = self._get_file_name(content_type, file_name)
        bucket_name = (
            self.config.get("PRESIGNED_POST_BUCKET_NAME") or self.config["S3_BUCKET"]
        )
        key = self._get_file_key(upload_directory_, file_name)

        fields = {
            HttpHeaderType.CONTENT_TYPE.value: content_type,
            "acl": acl,
            "success_action_status": success_action_status,
        }
        conditions = [
            {
                HttpHeaderType.CONTENT_TYPE.value: fields[
                    HttpHeaderType.CONTENT_TYPE.value
                ]
            },
            {"acl": fields["acl"]},
            {"success_action_status": fields["success_action_status"]},
        ]

        if file_size_in_bytes:
            conditions.append(["content-length-range", 0, file_size_in_bytes])

        try:
            response = await self.client.generate_presigned_post(
                Bucket=bucket_name,
                Key=key,
                Fields=fields,
                ExpiresIn=ttl_in_seconds,
                Conditions=conditions,
            )
        except Exception as error:
            error_message = (
                "exception type {}, exception {},  bucket_name {} , key {}, fields {}, ttl_in_seconds {},"
                " conditions {}".format(
                    type(error),
                    error,
                    bucket_name,
                    key,
                    fields,
                    ttl_in_seconds,
                    conditions,
                )
            )
            raise Exception(
                ErrorMessages.SomethingWentWrongError.value.format(error=error_message)
            )
        return response

    def _validate_presigned_post(
        self, content_type: str, file_size_in_bytes: int, success_action_status: str
    ):
        self._validate_presigned_post_content_type(content_type)
        self._validate_presigned_post_file_size(file_size_in_bytes)
        self._validate_presigned_post_success_action_status(success_action_status)

    @staticmethod
    def _get_file_name(content_type, file_name: str = None):
        if file_name:
            return file_name
        file_extension = get_file_extension_from_content_type(content_type)
        file_name = "{}.{}".format(str(uuid.uuid4()), file_extension)
        return file_name

    @staticmethod
    def _get_file_key(path, file_name: str):
        return "{}/{}".format(path, file_name)

    def _validate_presigned_post_content_type(self, content_type: str):
        allowed_content_types = self.allowed_content_types or self.default_content_types
        content_type = str(content_type).lower()
        valid_content_type = False
        for allowed_type in allowed_content_types:
            if content_type == allowed_type or content_type.startswith(allowed_type):
                valid_content_type = True
                break

        if not valid_content_type:
            raise Exception(
                ErrorMessages.AWSS3InvalidContentTypeError.value.format(
                    content_type=content_type
                )
            )

    def _validate_presigned_post_file_size(self, file_size_in_bytes: int):
        if file_size_in_bytes:
            max_size_in_bytes = (
                self.config.get("MAX_FILE_SIZE_IN_BYTES")
                or self.presigned_post_defaults.get("MAX_FILE_SIZE_IN_BYTES")
                or DEFAULTS["MAX_FILE_SIZE_IN_BYTES"]
            )
            if file_size_in_bytes > max_size_in_bytes:
                raise Exception(
                    ErrorMessages.MaxFileSizeExceeded.value.format(
                        max_size=max_size_in_bytes,
                        uploaded_file_size=file_size_in_bytes,
                    )
                )

    def _validate_presigned_post_success_action_status(
        self, success_action_status: str
    ):
        allowed_success_action_status = self.presigned_post_defaults[
            "ALLOWED_SUCCESS_ACTION_STATUS"
        ]
        if success_action_status not in allowed_success_action_status:
            raise Exception(
                ErrorMessages.PresignedPostInvalidSuccessStatusError.value.format(
                    invalid_status=success_action_status
                )
            )

    async def _create_s3_client(self, **kwargs):
        """
        :return: S3 client object (built from config attr)
        """

        config = self.config
        timeout_config = config.get("TIMEOUT", {})
        connect_timeout, read_timeout = timeout_config.get(
            "CONNECT"
        ), timeout_config.get("READ")
        endpoint_url = config.get("S3_ENDPOINT_URL", "").strip() or None

        aws_access_key_id = config.get("AWS_ACCESS_KEY_ID", "").strip() or None
        aws_secret_access_key = config.get("AWS_SECRET_ACCESS_KEY", "").strip() or None

        client = await S3Client.create_s3_client(
            config.get("S3_REGION"),
            aws_secret_access_key=aws_secret_access_key,
            aws_access_key_id=aws_access_key_id,
            endpoint_url=endpoint_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            **kwargs
        )

        return await client.__aenter__()

    async def _s3_client(self):
        """
        Function returns S3 client if it is present otherwise generates S3 client
        with config passed on to this class.
        :return: AioBaseClient S3 Object
        """

        if self.client is not None:
            return self.client
        return await self._create_s3_client()

    def validate(self, file_object):
        max_size_in_bytes = self.config.get(
            "MAX_FILE_SIZE_IN_BYTES", DEFAULTS["MAX_FILE_SIZE_IN_BYTES"]
        )
        if isinstance(file_object, bytes):
            file_size = getsizeof(file_object)
            if file_size > max_size_in_bytes:
                raise Exception(
                    ErrorMessages.MaxFileSizeExceeded.value.format(
                        max_size=max_size_in_bytes, uploaded_file_size=file_size
                    )
                )

    async def get_presigned_urls(self, user_data, user_id):
        response = list()
        try:
            for url_data in user_data:
                response.append(
                    await self.get_presigned_data_after_saving(user_id, url_data)
                )
        except Exception as error:
            raise Exception(
                ErrorMessages.SomethingWentWrongError.value.format(error=error)
            )
        presigned_data, db_data = self.format_get_presigned_data_response(response)
        return presigned_data, db_data

    async def get_presigned_data_after_saving(self, user_id, data):
        content_type, file_size_in_bytes = data["content_type"], data.get(
            "file_size_in_bytes"
        )
        try:
            presigned_data = await self.generate_presigned_post(
                content_type=content_type, file_size_in_bytes=file_size_in_bytes
            )
        except Exception as e:
            raise Exception(e)
        full_url = "{}/{}".format(
            presigned_data["url"], presigned_data["fields"]["key"]
        )
        db_data = {
            "user_id": user_id,
            "url": unquote(full_url),
            "checksum": data.get("checksum"),
            "content_type": content_type,
            "file_size_in_bytes": file_size_in_bytes,
            "meta": presigned_data,
        }
        presigned_data["identifier"] = data["identifier"]
        result = {"presigned_data": presigned_data, "db_data": db_data}
        return result

    @staticmethod
    def format_get_presigned_data_response(response):
        if not response:
            return response
        result = {}
        db_data_list = list()
        for _data in response:
            data = _data.get("presigned_data")
            db_data_list.append(_data.get("db_data"))
            identifier = data["identifier"]
            result[identifier] = data
            data.pop("identifier")
        return result, db_data_list

    async def validate_presigned_urls_aws(self, urls):
        tasks = []
        for url in urls:
            tasks.append(self.validate_url_exists_in_aws(url))
        try:
            response = await asyncio.gather(*tasks)
        except Exception as error:

            raise Exception(
                ErrorMessages.SomethingWentWrongError.value.format(error=error)
            )
        return response

    async def validate_url_exists_in_aws(self, url):
        _aiohttp_session = await BaseApiRequest.get_session()
        aws_response = await _aiohttp_session.request("head", url)
        try:
            if aws_response.status != 200:
                raise Exception(ErrorMessages.PresignedUrlDoesNotExist.value)
            filtered_headers = self._filter_aws_headers_response(aws_response.headers)

            file_name = url.split("/")[-1]
            filtered_headers.update(name=file_name)
        finally:
            if aws_response and hasattr(aws_response, "release"):
                await aws_response.release()

        return filtered_headers

    @staticmethod
    def _filter_aws_headers_response(headers):
        file_size = int(headers["CONTENT-LENGTH"])
        checksum = headers["ETAG"].replace('"', "")
        content_type = headers["CONTENT-TYPE"]
        file_format, resource_type = content_type.split("/")
        return {
            "size": file_size,
            "checksum": checksum,
            "content_type": content_type,
            "format": file_format,
            "resource_type": resource_type,
        }

    async def delete_file(self, key):
        config = self.config
        bucket = config["S3_BUCKET"]
        await self.client.delete_object(Bucket=bucket, Key=key)

    async def fetch_files(self, prefix: str = "", delimiter: str = "/"):
        config = self.config
        bucket = config["S3_BUCKET"]
        paginator = self.client.get_paginator("list_objects")
        files = []
        async for result in paginator.paginate(
            Bucket=bucket, Prefix=prefix, Delimiter=delimiter
        ):
            for file in result.get("Contents", []):
                files.append(file.get("Key"))
        return files
