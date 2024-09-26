import os
import sys
import pytest
from dotenv import load_dotenv
from nameko.standalone.rpc import ClusterRpcClient

load_dotenv()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def rabbitmq_config() -> dict[str, str]:
    protocol = os.getenv("RABBIT_PROTOCOL")
    user = os.getenv("RABBIT_USER")
    password = os.getenv("RABBIT_PASSWORD")
    host = os.getenv("RABBIT_HOST")
    port = os.getenv("RABBIT_PORT")
    return {"AMQP_URI": f"{protocol}://{user}:{password}@{host}:{port}"}


@pytest.fixture(scope="session")
def client(rabbitmq_config):
    return ClusterRpcClient(rabbitmq_config)
