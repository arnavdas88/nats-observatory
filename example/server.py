import asyncio
import logging

from nats.aio.msg import Msg

from nats_observe.tracing import setup_tracer
from nats_observe.config import NATSotelSettings, NATSConfig
from nats_observe.client import Client as NATSotel
from nats_observe.handlers import default_trace_handler

from nats_observe.utils import get_trace_spancontext


async def callack_C(msg: Msg):
    logger = logging.getLogger('server')
    logger.info("Received Result!")
    pass

async def run():
    settings = NATSotelSettings(service_name='server', servers=["nats://127.0.0.1:4221"])

    client = NATSotel(settings)
    logger = logging.getLogger('server')

    await client.connect()
    
    logger.info("Requesting Task!")
    await client.publish("dummy.foo", b"Hello World!")
    await client.subscribe("dummy.bar", cb=callack_C)

    
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(run())
