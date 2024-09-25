from nameko.extensions import DependencyProvider
from .runnables import Runnables


class RunnablesProvider(DependencyProvider):
    def get_dependency(self, worker_ctx):
        return Runnables()
