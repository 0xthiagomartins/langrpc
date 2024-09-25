import pytest
from langrpc.client import RemoteRunnable
from nameko.standalone.rpc import ClusterRpcClient
from langchain_core.messages import AIMessage, AIMessageChunk
from rich import print


def test_rpc(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        assert rpc.runnables.test() is True


def test_invoke(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        response = rpc.runnables.invoke("chain_with_prompt", "Hello, world!")
        assert isinstance(response, dict)
        ai_message = AIMessage(**response.get("kwargs", {}))
        assert isinstance(ai_message, AIMessage)


def test_batch(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        runnable = RemoteRunnable(rpc.runnables, "chain_with_prompt")
        response = runnable.batch(["Hello, world!", "Hello, world!"])
        assert isinstance(response, list)
        assert isinstance(response[0], AIMessage)
        assert isinstance(response[1], AIMessage)


def test_stream(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        runnable = RemoteRunnable(rpc.runnables, "chain_with_prompt")
        for chunk in runnable.stream("Hello World"):
            ai_message_chunk: AIMessage = chunk
            print(f"Chunk: {ai_message_chunk}")
