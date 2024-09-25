from nameko.rpc import rpc
from nameko.exceptions import NamekoException
from abc import ABC
from typing import Any
from .dependency import Runnables, RunnablesProvider


class LangRPC(ABC):
    runnables: Runnables = RunnablesProvider()

    def runnable(self, runnable_id: Any):
        runnable = self.runnables.get(runnable_id)
        if runnable:
            return runnable
        raise NamekoException("Runnable not found.")

    def run_chain(self, _id: Any, input_data: dict):
        return self.runnable(_id).invoke(input_data)

    def invoke(self, _id: Any, input_data: str) -> str:
        runnable = self.runnable(_id)
        return runnable.invoke(input_data)

    def batch(self, _id: Any, input_data: list[dict]):
        runnable = self.runnable(_id)
        return runnable.batch(input_data)

    def stream(self, _id: Any, input_data: str) -> str:
        runnable = self.runnable(_id)
        return runnable.stream(input_data)

    def stream_log(self, _id: Any, input_data: str) -> str:
        runnable = self.runnable(_id)
        return runnable.stream_log(input_data)

    def astream_events(self, _id: Any, input_data: str) -> str:
        runnable = self.runnable(_id)
        return runnable.astream_events(input_data)

    def get_schema(self, _id: Any) -> str:
        runnable = self.runnable(_id)
        return runnable.get_schema()
