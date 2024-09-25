import pytest
from nameko.standalone.rpc import ClusterRpcProxy
from rich import print


def test_rpc(rabbitmq_config):
    with ClusterRpcProxy(rabbitmq_config) as rpc:
        assert rpc.runnables.test() is True


def test_invoke(rabbitmq_config):
    with ClusterRpcProxy(rabbitmq_config) as rpc:
        response = rpc.runnables.invoke("chain_with_prompt", "Hello, world!")
        print(response)
        assert isinstance(response, dict)


def test_batch(rabbitmq_config):
    with ClusterRpcProxy(rabbitmq_config) as rpc:
        response = rpc.runnables.batch(
            "chain_with_prompt", ["Hello, world!", "Hello, world!"]
        )
        print(response)
        assert isinstance(response, list)
        assert isinstance(response[0], dict)
        assert isinstance(response[1], dict)
