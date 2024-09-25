from nameko.rpc import rpc
from nameko.exceptions import RemoteError
from nameko.events import EventDispatcher, event_handler
from abc import ABC
from typing import Any
from .dependency import Runnables, RunnablesProvider
from langchain_core.messages import AIMessage, BaseMessage


class LangRPC(ABC):
    name = "abstract"

    runnables: Runnables = RunnablesProvider()
    dispatch = EventDispatcher()

    def runnable(self, runnable_id: Any):
        runnable = self.runnables.get(runnable_id)
        if runnable:
            return runnable
        raise RemoteError("Runnable not found.")

    @rpc
    def invoke(self, _id: Any, input_data: str) -> dict:
        runnable = self.runnable(_id)
        ai_message: AIMessage = runnable.invoke(input_data)
        return ai_message.to_json()

    @rpc
    def batch(self, _id: Any, input_data: list[dict]) -> list[dict]:
        runnable = self.runnable(_id)
        ai_messages: list[AIMessage] = runnable.batch(input_data)
        return [ai_message.to_json() for ai_message in ai_messages]

    def stream(self, _id: Any, input_data: str) -> dict:
        pass

    #####
    @rpc
    def start_stream(self, _id: Any, input_data: str) -> dict:
        runnable = self.runnable(_id)
        return runnable.start_stream(input_data)

    #####
    @rpc
    def stream_log(self, _id: Any, input_data: str) -> str:
        runnable = self.runnable(_id)
        return runnable.stream_log(input_data)

    @rpc
    def astream_events(self, _id: Any, input_data: str) -> str:
        runnable = self.runnable(_id)
        return runnable.astream_events(input_data)

    @rpc
    def get_schema(self, _id: Any) -> str:
        runnable = self.runnable(_id)
        return runnable.get_schema()
