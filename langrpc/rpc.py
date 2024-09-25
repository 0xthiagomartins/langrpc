from nameko.rpc import rpc
from nameko.exceptions import RemoteError
from nameko.events import EventDispatcher, event_handler
from abc import ABC
from typing import Any
from .dependency import Runnables, RunnablesProvider
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
import uuid
import threading
from time import sleep


class LangRPC(ABC):
    name = "abstract"

    runnables: Runnables = RunnablesProvider()
    dispatch = EventDispatcher()

    def __init__(self):
        self.active_streams = {}

    def runnable(self, runnable_id):
        runnable = self.runnables.get(runnable_id)
        if runnable:
            return runnable
        raise RemoteError("Runnable not found.")

    @rpc
    def invoke(self, _id, *args, **kwargs) -> dict:
        runnable = self.runnable(_id)
        ai_message: AIMessage = runnable.invoke(*args, **kwargs)
        return ai_message.to_json()

    @rpc
    def batch(self, _id, *args, **kwargs) -> list[dict]:
        runnable = self.runnable(_id)
        ai_messages: list[AIMessage] = runnable.batch(*args, **kwargs)
        return [ai_message.to_json() for ai_message in ai_messages]

    #####
    @rpc
    def start_stream(self, _id, *args, **kwargs):
        stream_id = str(uuid.uuid4())
        runnable = self.runnable(_id)
        self.active_streams[stream_id] = runnable.stream(*args, **kwargs)
        threading.Thread(
            target=self._stream_data, args=(stream_id,), daemon=True
        ).start()
        return stream_id

    def _stream_data(self, stream_id):
        stream = self.active_streams.get(stream_id)
        if not stream:
            return
        for chunk in stream:
            ai_message_chunk: AIMessageChunk = chunk
            self.dispatch(
                "stream_data",
                {"stream_id": stream_id, "chunk": ai_message_chunk.to_json()},
            )
        self.dispatch("stream_complete", {"stream_id": stream_id, "status": "done"})
        del self.active_streams[stream_id]

    #####
    @rpc
    def stream_log(self, _id, args) -> str:
        runnable = self.runnable(_id)
        return runnable.stream_log(args)

    @rpc
    def astream_events(self, _id, args) -> str:
        runnable = self.runnable(_id)
        return runnable.astream_events(args)

    @rpc
    def get_schema(self, _id) -> str:
        runnable = self.runnable(_id)
        return runnable.get_schema()
