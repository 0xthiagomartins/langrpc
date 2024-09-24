from nameko.standalone.rpc import ClusterRpcProxy
from pydantic import BaseModel
import os


class RunRequest(BaseModel):
    input_data: dict


if __name__ == "__main__":
    config = {"AMQP_URI": os.getenv("AMQP_URI", "amqp://guest:guest@localhost/")}

    with ClusterRpcProxy(config) as rpc:
        request = RunRequest(input_data={"key": "value"})  # Replace with actual input
        response = rpc.langrpc_service.run_chain(request)
        print(response.output_data)
