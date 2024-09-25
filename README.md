# 🦜️🏓 LangServe RPC

LangServe RPC is a library that allows you to run RPC servers and clients for your LangServe apps.

## Features

- Input and Output schemas automatically inferred from your LangChain object, and enforced on every RPC, with rich error messages
- Efficient .invoke, .batch and .stream rpc methods with support for many concurrent requests on a single microservice
- .stream_log rpc method for streaming all (or some) intermediate steps from your chain/agent
- Built on top of Nameko microservices framework for easy deployment and scaling



## Installation

```bash
pip install langserve-rpc
```