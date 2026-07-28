import asyncio


class PausedWorkflow:

    async def execute(self, payload):
        print("executed the workflow for Paused sensor")


# Entry point for Celery
def process(payload):
    workflow = PausedWorkflow()
    asyncio.run(workflow.execute(payload))        