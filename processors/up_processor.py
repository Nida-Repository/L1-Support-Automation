import asyncio


class UpWorkflow:

    async def execute(self, payload):
        print("executed the workflow for Up sensor")

# Entry point for Celery
def process(payload):
    workflow = UpWorkflow()
    asyncio.run(workflow.execute(payload))               