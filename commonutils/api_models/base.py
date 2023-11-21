import ujson as json
from typing import List, Optional, Union

from pydantic import BaseModel


class Error(BaseModel):
    """Error object"""

    message: str


class ApiV4Error(BaseModel):
    """V4 API error object"""

    message: str
    errors: List[Error]

    @staticmethod
    def from_message(message):
        return ApiV4Error(**{"message": message, "errors": [{"message": message}]})


class ApiV4Response(BaseModel):
    """V4 API response object"""

    is_success: bool
    status_code: int
    data: Optional[Union[BaseModel, None]]
    meta: Optional[Union[BaseModel, None]]
    error: Optional[Union[ApiV4Error, None]]


class BaseCustomModel(BaseModel):
    def custom_dict(self):
        # json_loads() here is temporary work around to get jsonable dicts from pydantic model.
        # Currently no support exists to provide this and hence field types like datetime break during serialization.
        return json.loads(self.json())


class ApiModelBaseClass:
    """
    Base API model for request & response handling.
    Publicly exposes
    - RequestParamsModel - For request validation
    - RequestBodyModel - For request validation
    - ResponseDataModel - For response modelling
    - success_response() & error_response() methods to get the respective response body objects
    Other parameters like _uri, _method etc are internally used to generate API documentation.
    Default success and error resp methods can be overriden for custom handling.
    """

    _uri = None
    _method = None
    _summary = ""
    _description = ""
    _request_params = None
    _request_body = None
    _data = None
    _error = ApiV4Error(**{"message": "Bad Request", "errors": [{"message": "Bad Request"}]})

    class RequestParamsModel(BaseModel):
        """Represents the request query params object. Used for request validation & documentation"""

    class RequestBodyModel(BaseModel):
        """Represents the request body object. Used for request validation & documentation"""

    class ResponseDataModel(BaseModel):
        """Represents the response data object. Used for constructing response json & documentation"""

    @classmethod
    def success_response(cls, status_code=200, data=None):
        """Constructs success response"""
        fields = cls.ResponseDataModel.__fields__.keys()
        # Special handling for data objects which don't have any keys at root level.
        # Check https://pydantic-docs.helpmanual.io/usage/models/#custom-root-types for more details.
        if "__root__" in fields and data:
            data = {"__root__": data}

        _data = cls.ResponseDataModel(**data) if data else cls._data
        return ApiV4Response(is_success=True, status_code=status_code, data=_data)

    @classmethod
    def success_response_dict(cls, status_code=200, data=None):
        # json_loads() here is temporary work around to get jsonable dicts from pydantic model.
        # Currently no support exists to provide this and hence field types like datetime break during serialization.
        res = cls.success_response(status_code=status_code, data=data)
        return json.loads(res.json(exclude_unset=True))

    @classmethod
    def error_response(cls, status_code=400, err_dict=None, err_msg=None):
        """Constructs error response"""
        if err_msg:
            _error = ApiV4Error.from_message(err_msg)
        else:
            _error = ApiV4Error(**err_dict) if err_dict else cls._error
        return ApiV4Response(is_success=False, status_code=status_code, error=_error)

    @classmethod
    def error_response_dict(cls, status_code=400, err_dict=None, err_msg=None):
        # json_loads() here is temporary work around to get jsonable dicts from pydantic model.
        # Currently no support exists to provide this and hence field types like datetime break during serialization.
        res = cls.error_response(status_code=status_code, err_dict=err_dict, err_msg=err_msg)
        return json.loads(res.json(exclude_unset=True))

    @classmethod
    def doc(cls):
        """Generate API doc string"""

    @classmethod
    def get_response_model_data(cls):
        return cls._data

    @classmethod
    def get_request_param(cls):
        return cls._request_params

    @classmethod
    def get_request_body(cls):
        return cls._request_body

    @classmethod
    def uri(cls):
        return cls._uri

    @classmethod
    def description(cls):
        return cls._description

    @classmethod
    def summary(cls):
        return cls._summary
