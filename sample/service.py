from nameko.rpc import rpc
from langrpc import add_routes
from .chains import chain_genai

add_routes(chain_genai, "genai")


class SampleService:
    name = "sample_service"

    @rpc
    def sample_method(self, input_data: str) -> str:
        return f"Hello, {input_data}!"
