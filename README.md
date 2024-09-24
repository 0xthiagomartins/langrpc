# 🦜️🏓 LangServe RPC

LangServe RPC is a library that allows you to run RPC servers and clients for your LangServe apps.

## Installation

```bash
pip install langserve-rpc
```

## Usage

```python
from langserve_rpc import RemoteRunnable

class MyRunnable(RemoteRunnable):
    def run(self, input: str) -> str:
        return f"Hello, {input}!"

app = LangServe(
    runnables=[MyRunnable()],
    title="My LangServe App",
    description="This is a simple LangServe app.",
)
```
