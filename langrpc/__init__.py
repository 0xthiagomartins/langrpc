"""
Main entrypoint into package.

This is the ONLY public interface into the package. All other modules are
to be considered private and subject to change without notice.
"""

from langrpc.api_handler import APIHandler
from langrpc.client import RemoteRunnable
from langrpc.schema import CustomUserType
from langrpc.server import router
from langrpc.version import __version__
from langchain_core.runnables import Runnable


def add_runnable(runnable: Runnable, runnable_id: str):
    router.add(runnable_id, runnable)


__all__ = [
    "RemoteRunnable",
    "APIHandler",
    "add_routes",
    "__version__",
    "CustomUserType",
]
