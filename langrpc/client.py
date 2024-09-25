from typing import Iterator
from nameko.standalone.rpc import ClusterRpcClient
from nameko.rpc import Client
from langchain_core.messages import AIMessage
import os


class RemoteRunnable:
    def __init__(self, rpc: Client, chain_id: str):
        self.rpc = rpc
        self.chain_id = chain_id

    def invoke(self, input_data) -> AIMessage:
        ai_message: dict = self.rpc.invoke(self.chain_id, input_data)
        return AIMessage(**ai_message.get("kwargs", {}))

    def batch(self, input_data) -> list[AIMessage]:
        ai_messages: list[dict] = self.rpc.batch(self.chain_id, input_data)
        return [AIMessage(**ai_message.get("kwargs", {})) for ai_message in ai_messages]

    def stream(self, input_data) -> Iterator[AIMessage]:
        pass


if __name__ == "__main__":
    config = {"AMQP_URI": os.getenv("AMQP_URI", "amqp://guest:guest@localhost/")}

    with ClusterRpcClient(config) as rpc:
        Runnable = RemoteRunnable(rpc, "langrpc_service", "run_chain")
