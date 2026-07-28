
import asyncio
class WarningWorkflow:

    async def execute(self, payload):
        print("executed the workflow for Warning sensor")

# Entry point for Celery
def process(payload):
    workflow = WarningWorkflow()
    asyncio.run(workflow.execute(payload))          