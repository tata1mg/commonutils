from enum import Enum

from formencode import variabledecode
from multidict import MultiDict

UTF8 = "utf-8"
ASCII = "ascii"


def get_file_extension_from_content_type(content_type):
    return str(content_type).split("/")[-1]


def get_dict_from_multi_dict(multi_dict: MultiDict) -> dict:
    return variabledecode.variable_decode(multi_dict)


class CustomEnum(Enum):
    @classmethod
    def get_enum(cls, value):
        for mode in cls:
            if value == mode.value:
                return mode
        return None

    @classmethod
    def get_all_values(cls):
        return [custom_enum.value for custom_enum in cls]


class Singleton(type):
    _instances = {}

    def __call__(self, *args, **kwargs):
        if self not in self._instances:
            self._instances[self] = super(Singleton, self).__call__(*args, **kwargs)
        return self._instances[self]
