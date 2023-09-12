from abc import ABC, abstractmethod


class SQSHandler(ABC):
    @classmethod
    @abstractmethod
    def handle_event(cls, body):
        pass
