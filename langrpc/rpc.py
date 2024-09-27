from nameko.rpc import rpc
from nameko.exceptions import RemoteError
from nameko.events import EventDispatcher
from abc import ABC
from .dependency import Runnables, RunnablesProvider
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from typing import AsyncGenerator
import uuid
import asyncio
from rstream import Producer


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
    async def stream(self, _id, *args, **kwargs):
        runnable = self.runnable(_id)
        await self.astream(runnable, *args, **kwargs)

    async def astream(self, runnable, *args, **kwargs):
        async with Producer(
            host="localhost",
            username="guest",
            password="guest",
            port=5672,
        ) as producer:
            STREAM_NAME = "langchain_stream"
            STREAM_RETENTION = 5000000000
            await producer.create_stream(
                STREAM_NAME,
                exists_ok=True,
                arguments={"MaxLengthBytes": STREAM_RETENTION},
            )
            for chunk in runnable.stream(*args, **kwargs):
                ai_message_chunk: AIMessageChunk = chunk
                await producer.send(
                    stream=STREAM_NAME, message=ai_message_chunk.to_json()
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
