from nameko.rpc import rpc
from langrpc import LangRPC, Runnables
from .chains import chain_with_prompt, chain_genai


def register_runnables():
    runnables = Runnables()
    runnables.add("chain_with_prompt", chain_with_prompt)
    runnables.add("chain_genai", chain_genai)
    return runnables


class RunnablesRPC(LangRPC):
    name = "runnables"
    runnables = register_runnables()

    @rpc
    def test(self):
        return True
