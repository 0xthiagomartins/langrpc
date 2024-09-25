import pytest
from langrpc.client import RemoteRunnable
from nameko.standalone.rpc import ClusterRpcClient
from langchain_core.messages import AIMessage
from rich import print


def test_rpc(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        assert rpc.runnables.test() is True


def test_invoke(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        response = rpc.runnables.invoke("chain_with_prompt", "Hello, world!")
        print(response)
        assert isinstance(response, dict)
        ai_message = AIMessage(**response.get("kwargs", {}))
        assert isinstance(ai_message, AIMessage)


def test_batch(rabbitmq_config):
    with ClusterRpcClient(rabbitmq_config) as rpc:
        runnable = RemoteRunnable(rpc.runnables, "chain_with_prompt")
        response = runnable.batch(["Hello, world!", "Hello, world!"])
        print(response)
        assert isinstance(response, list)
        assert isinstance(response[0], AIMessage)
        assert isinstance(response[1], AIMessage)
