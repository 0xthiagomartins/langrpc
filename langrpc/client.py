from nameko.standalone.rpc import ClusterRpcClient
from nameko.rpc import Client
from langchain_core.messages import AIMessage
import os
from nameko.events import event_handler
from langchain_core.messages import AIMessage, AIMessageChunk
import queue
from nameko.runners import ServiceRunner
import threading
import uuid
import sys
import time


class RemoteRunnable:
    def __init__(self, rpc: Client, runnable_id: str):
        self.rpc = rpc
        self.runnable_id = runnable_id
        self.stream_id = None
        self.data_queue = queue.Queue()
        self.error_queue = queue.Queue()
        self.completed = threading.Event()
        self.runner = ServiceRunner()
        self.runner.add_service(self.EventHandler(self))
        self.event_thread = threading.Thread(target=self.run_event_handler, daemon=True)
        self.event_thread.start()

    def invoke(self, input_data) -> AIMessage:
        ai_message: dict = self.rpc.invoke(self.runnable_id, input_data)
        return AIMessage(**ai_message.get("kwargs", {}))

    def batch(self, input_data) -> list[AIMessage]:
        ai_messages: list[dict] = self.rpc.batch(self.runnable_id, input_data)
        return [AIMessage(**ai_message.get("kwargs", {})) for ai_message in ai_messages]

    class EventHandler:
        name = "remote_client_stream_event_handler"

        def __init__(self, rpc):
            self.rpc = rpc

        @event_handler("lang_rpc_service", "stream_data")
        def handle_stream_data(self, payload):
            if payload["stream_id"] != self.rpc.stream_id:
                return  # Ignore unrelated streams
            chunk_data = payload["chunk"]
            ai_message_chunk = AIMessageChunk(**chunk_data.get("kwargs", {}))
            self.rpc.data_queue.put(ai_message_chunk)

        @event_handler("lang_rpc_service", "stream_complete")
        def handle_stream_complete(self, payload):
            if payload["stream_id"] != self.rpc.stream_id:
                return  # Ignore unrelated streams
            self.rpc.completed.set()

        @event_handler("lang_rpc_service", "stream_error")
        def handle_stream_error(self, payload):
            if payload["stream_id"] != self.rpc.stream_id:
                return  # Ignore unrelated streams
            error = payload.get("error", "Unknown error")
            self.rpc.error_queue.put(error)
            self.rpc.completed.set()

    def run_event_handler(self):
        try:
            self.runner.start()
            self.runner.wait()
        except Exception as e:
            self.error_queue.put(str(e))
            self.completed.set()

    def stream(self, input_data):
        """
        Generator that yields AIMessageChunk instances as they are received.
        """
        self.stream_id = self.rpc.start_stream(self.runnable_id, input_data)
        print(f"Started stream with ID: {self.stream_id}")
        try:
            while not self.completed.is_set() or not self.data_queue.empty():
                try:
                    chunk = self.data_queue.get(timeout=1)
                    yield chunk
                except queue.Empty:
                    if self.completed.is_set():
                        break
            # After completion, check for errors
            if not self.error_queue.empty():
                error = self.error_queue.get()
                raise Exception(f"Stream encountered an error: {error}")
            else:
                print("Stream completed successfully.")
        except Exception as e:
            print(f"An error occurred during streaming: {e}")
        finally:
            self.runner.stop()
            self.event_thread.join()


if __name__ == "__main__":
    config = {"AMQP_URI": os.getenv("AMQP_URI", "amqp://guest:guest@localhost/")}

    with ClusterRpcClient(config) as rpc:
        Runnable = RemoteRunnable(rpc, "langrpc_service", "run_chain")
