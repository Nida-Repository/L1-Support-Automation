import asyncio

class UnusualWorkflow:

    async def execute(self, payload):
        print("executed the workflow for Unusual sensor")

# Entry point for Celery
def process(payload):
    workflow = UnusualWorkflow()
    asyncio.run(workflow.execute(payload))         