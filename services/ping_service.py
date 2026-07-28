import asyncio


class PingIp:

    async def execute(self, payload):
        print("executed the workflow for Ping")


# Entry point for Celery
def process(payload):
    workflow = PingIp()
    asyncio.run(workflow.execute(payload))        