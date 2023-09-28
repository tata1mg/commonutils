from enum import Enum


class HttpHeaderType(Enum):
    CONTENT_TYPE = "Content-Type"
    ACCEPT = "Accept"
    AUTHORIZATION = "Authorization"


class HTTPMethods(Enum):
    """HTTP Request Methods"""
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    PATCH = "PATCH"
    DELETE = "DELETE"
