import asyncio
import logging

logger = logging.getLogger()


class RedisProducerConsumerManager:
    """
    Redis Producer Consumer Handler.
    Can be used to push data to a redis queue (produce data) OR read data available in a redis queue (consume data)
    The data is pushed to the start of the queue and is popped from the end of the queue (FIFO).
    """

    def __init__(self, redis_wrapper, queue_name: str, wait_between_consume=0.05):
        """
        Create an instance of the producer consumer handler
        :param redis_wrapper: Redis Wrapper written for Sanic services, must be an instance of the
        redis_wrapper.RedisWrapper class
        :param queue_name : redis queue name
        """
        self._queue_name = queue_name
        self._redis_wrapper = redis_wrapper
        self._wait_between_consume = wait_between_consume

    async def produce_data(self, payload: str):
        """
        Push the data to the queue.
        Data is pushed to the start of the queue.
        :param str payload: Payload to publish with the event
        :return:
        """
        try:
            await self._redis_wrapper.lpush(self._queue_name, payload)
        except Exception as e:
            logger.error("Push to queue failed with error %s", repr(e))

    async def consume_data(self, handler):
        """
        :param handler : a method to be called when the data is received from the queue
        """
        # TODO details to be added here
        while True:
            # wait indefinitely on looking for data to be pushed on the self._queue_name queue
            # When data arrives, call the handler function with received data
            redis_data = await self._redis_wrapper.brpop([self._queue_name])
            payload = redis_data[1]
            asyncio.create_task(handler(payload))
            # sleep for 50ms before looking for the data again
            await asyncio.sleep(self._wait_between_consume)
