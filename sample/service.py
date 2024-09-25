from nameko.rpc import rpc
from langrpc import LangRPC, Runnables
from .chains import chain_with_prompt, chain_genai


def register_runnables():
    runnables = Runnables()
    runnables.add("chain_with_prompt", chain_with_prompt)
    runnables.add("chain_genai", chain_genai)
    return runnables


def apply_runnable_interface(cls):
    for name in [
        "invoke",  # invoke the runnable on a single input
        "batch",  # invoke the runnable on a batch of inputs
        "stream",  # stream the runnable on a single input
        "stream_log",  # stream the runnable on a single input
        "astream_events",  # stream the runnable on a single input
    ]:
        setattr(cls, name, rpc(name))
    return cls


@apply_runnable_interface
class Runnables(LangRPC):
    name = "runnables"

    def __init__(self):
        super().__init__()
        self.runnables = register_runnables()
