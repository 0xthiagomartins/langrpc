from typing import Any
from nameko.exceptions import RemoteError


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Router(metaclass=Singleton):
    """

    RPC Router for a chain or runnable

    # Register chain
    >>> router = ChainRouter()
    >>> router.add(chain)

    # Retrieve chain
    >>> router.get(chain)
    """

    __runnables: dict[str, Any] = {}
    __schemas: dict[str, dict[str, Any]] = {}

    @classmethod
    def add(cls, runnable_id: str, runnable: Any):
        """
        Add a runnable with a unique identifier.

        :param runnable_id: Unique identifier for the runnable.
        :param runnable: An instance of a LangChain runnable.
        :return: Confirmation message.
        """
        if runnable_id in cls.__runnables:
            raise RemoteError(
                "RunnableExists",
                f"Runnable or Chain with ID '{runnable_id}' already exists.",
            )
        # Add the runnable/chain to the storage
        cls.__runnables[runnable_id] = runnable

        # Infer schemas based on chain's input and output keys
        input_keys = getattr(runnable, "input_keys", ["input"])
        output_keys = getattr(runnable, "output_keys", ["output"])
        config_keys = []

        # Create simple JSON schemas (you can enhance this as needed)
        input_schema = {key: {"type": "string"} for key in input_keys}
        output_schema = {key: {"type": "string"} for key in output_keys}
        config_schema = (
            {key: {"type": "string"} for key in config_keys} if config_keys else {}
        )

        cls.__schemas[runnable_id] = {
            "input_schema": input_schema,
            "output_schema": output_schema,
            "config_schema": config_schema,
        }
        print(f"Runnable/Chain '{runnable_id}' added successfully.")

    @classmethod
    def get(cls, runnable_id: str):
        return cls.__runnables.get(runnable_id, lambda: None)

    @classmethod
    def get_schema(cls, runnable_id: str):
        return cls.__schemas.get(runnable_id)


router = Router()
