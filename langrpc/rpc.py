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
import eventlet


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
    def stream(self, _id, stream_id, input_data):
        """
        Initiates a streaming process with the provided stream_id.
        """
        # Register the active stream
        self.active_streams[stream_id] = (_id, input_data)
        # Start streaming data
        try:
            print(f"Starting stream with ID {stream_id}", flush=True)
            self._stream_data(stream_id)
        except Exception as e:
            self.dispatch(
                "stream_error",
                {"stream_id": stream_id, "error": str(e)},
            )
        finally:
            del self.active_streams[stream_id]
            print(
                f"Stream with ID {stream_id} has been removed from active streams due to error: {e}",
                flush=True,
            )
        print(f"Stream complete for ID {stream_id}", flush=True)
        return stream_id

    def _stream_data(self, stream_id):
        """
        Handles the streaming logic, dispatching events as data chunks are processed.
        """
        _id, input_data = self.active_streams.get(stream_id)
        runnable = self.runnable(_id)
        for chunk in runnable.stream(input_data):
            ai_message_chunk: AIMessageChunk = chunk
            print(
                f"Dispatching chunk for stream ID {stream_id}, chunk: {ai_message_chunk.to_json()}",
                flush=True,
            )
            self.dispatch(
                "stream_data",
                {"stream_id": stream_id, "chunk": ai_message_chunk.to_json()},
            )
        print(f"Stream complete for ID {stream_id}", flush=True)
        self.dispatch(
            "stream_complete",
            {"stream_id": stream_id, "status": "done"},
        )

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
