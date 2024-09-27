from nameko.rpc import Client
from langchain_core.messages import AIMessage, AIMessageChunk
from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    ConsumerOffsetSpecification,
    OffsetType,
)
import asyncio
from asyncio import Queue  # Add this line


class RemoteRunnable:
    def __init__(self, rpc: Client, runnable_id: str):
        self.rpc = rpc
        self.runnable_id = runnable_id
        self.stream_id = None
        self.stream_queue = Queue()  # This is now an asyncio.Queue

    def invoke(self, input_data) -> AIMessage:
        ai_message: dict = self.rpc.invoke(self.runnable_id, input_data)
        return AIMessage(**ai_message.get("kwargs", {}))

    def batch(self, input_data) -> list[AIMessage]:
        ai_messages: list[dict] = self.rpc.batch(self.runnable_id, input_data)
        return [AIMessage(**ai_message.get("kwargs", {})) for ai_message in ai_messages]

    async def stream(self, input_data):
        """
        Initiates an asynchronous stream and yields AIMessageChunk instances.
        """
        print("Starting stream with input data:", input_data, flush=True)
        future = self.rpc.stream.call_async(self.runnable_id, input_data)
        await self.consume_stream()
        while True:
            chunk = await self.stream_queue.get()
            print("Yielding chunk:", chunk, flush=True)
            yield chunk

    async def consume_stream(self):
        try:
            STREAM_NAME = "langchain_stream"
            STREAM_RETENTION = 5000000000
            print("Setting up consumer for stream:", STREAM_NAME, flush=True)
            consumer = Consumer(
                host="localhost", port=5672, username="guest", password="guest"
            )
            await consumer.create_stream(
                STREAM_NAME,
                exists_ok=True,
                arguments={"MaxLengthBytes": STREAM_RETENTION},
            )
            print("Stream created with retention:", STREAM_RETENTION, flush=True)

            async def on_message(msg: AMQPMessage, message_context: MessageContext):
                stream = message_context.consumer.get_stream(
                    message_context.subscriber_name
                )
                print("Got message: {} from stream {}".format(msg, stream), flush=True)
                # Assuming msg.body contains the AIMessageChunk data
                ai_message_chunk = AIMessageChunk(**msg.body)
                print(
                    "Putting AIMessageChunk into queue:", ai_message_chunk, flush=True
                )
                await self.stream_queue.put(ai_message_chunk)

            await consumer.start()
            print("Consumer started", flush=True)
            await consumer.subscribe(
                stream=STREAM_NAME,
                callback=on_message,
                offset_specification=ConsumerOffsetSpecification(
                    OffsetType.FIRST, None
                ),
            )
            print("Subscribed to stream:", STREAM_NAME, flush=True)

            # Run the consumer in a background task
            asyncio.create_task(consumer.run())
            print("Consumer running in background task", flush=True)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", flush=True)
            # Handle other exceptions
