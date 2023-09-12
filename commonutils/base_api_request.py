from aiohttp import ClientSession, TCPConnector

SESSION = None


class BaseApiRequest:
    @classmethod
    async def get_session(cls):
        global SESSION
        if SESSION is None:
            conn = TCPConnector(limit=0, limit_per_host=0)
            SESSION = ClientSession(connector=conn)
        return SESSION
