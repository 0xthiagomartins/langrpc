import uuid
import queue
import threading
from nameko.rpc import Client
from nameko.events import event_handler
from nameko.runners import ServiceRunner
from langchain_core.messages import AIMessage, AIMessageChunk


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

        def __init__(self, client):
            self.client = client

        @event_handler("runnables", "stream_data")
        def handle_stream_data(self, payload):
            if payload["stream_id"] != self.client.stream_id:
                return  # Ignore unrelated streams
            chunk_data = payload["chunk"]
            ai_message_chunk = AIMessageChunk(**chunk_data.get("kwargs", {}))
            self.client.data_queue.put(ai_message_chunk)

        @event_handler("runnables", "stream_complete")
        def handle_stream_complete(self, payload):
            if payload["stream_id"] != self.client.stream_id:
                return  # Ignore unrelated streams
            self.client.completed.set()

        @event_handler("runnables", "stream_error")
        def handle_stream_error(self, payload):
            if payload["stream_id"] != self.client.stream_id:
                return  # Ignore unrelated streams
            error = payload.get("error", "Unknown error")
            self.client.error_queue.put(error)
            self.client.completed.set()

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
        Uses call_async to initiate the stream and listens to events.
        """
        # Generate a unique stream_id
        stream_id = str(uuid.uuid4())
        self.stream_id = stream_id

        # Initiate the stream asynchronously with the given stream_id
        # Pass stream_id as a separate parameter
        future = self.rpc.stream.call_async(self.runnable_id, stream_id, input_data)
        # Optionally, wait for confirmation if the service returns the stream_id
        # stream_id_confirmed = future.result()
        # assert stream_id_confirmed == stream_id

        print(f"Started stream with ID: {self.stream_id}")
        try:
            while not self.completed.is_set() or not self.data_queue.empty():
                try:
                    chunk = self.data_queue.get(timeout=1)
                    print("Chunk received from data_queue", flush=True)
                    yield chunk
                except queue.Empty:
                    print("empty", flush=True, end=" ")
                    if self.completed.is_set():
                        break
            # After completion, check for errors
            if not self.error_queue.empty():
                error = self.error_queue.get()
                print(f"Error found in error_queue: {error}", flush=True)
                raise Exception(f"Stream encountered an error: {error}")
            else:
                print("Stream completed successfully.", flush=True)
        except Exception as e:
            print(f"An error occurred during streaming: {e}", flush=True)
            raise e
        finally:
            print("Stopping runner and joining event_thread", flush=True)
            self.runner.stop()
            self.event_thread.join()
