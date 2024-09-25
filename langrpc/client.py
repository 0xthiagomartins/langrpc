from nameko.standalone.rpc import ClusterRpcProxy
from nameko.rpc import ProxyRpc
from pydantic import BseModel
import os


class RemoteRunnable:
    def __init__(self, rpc: ProxyRpc, chain_id: str):
        self.rpc = rpc
        self.chain_id = chain_id

    def invoke(self, input_data):
        return self.rpc.invoke(self.chain_id, input_data)

    def batch(self, input_data):
        return self.rpc.batch(self.chain_id, input_data)

    def stream(self, input_data):
        stream_id = self.rpc.start_stream(self.chain_id, input_data)
        return self.rpc.stream(self.chain_id, stream_id)


if __name__ == "__main__":
    config = {"AMQP_URI": os.getenv("AMQP_URI", "amqp://guest:guest@localhost/")}

    with ClusterRpcProxy(config) as rpc:
        Runnable = RemoteRunnable(rpc, "langrpc_service", "run_chain")
