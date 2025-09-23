import asyncio
import logging
from nats.aio.msg import Msg

from nats_observe.tracing import setup_tracer
from nats_observe.config import NATSotelSettings, NATSConfig, PROPAGATOR
from nats_observe.client import Client as NATSotel
from nats_observe.handlers import default_trace_handler


settings = NATSotelSettings(service_name='worker', servers=["nats://127.0.0.1:4223"])
client = NATSotel(settings)
logger = logging.getLogger('worker')

async def callack_A(msg: Msg):
    logger.info("Received Task!", )
    await do_some_work(msg)
    logger.info("Returning Result!", )
    await client.publish("dummy.bar", b"Bye World!")

async def do_some_work(msg: Msg):
    logger.info("Completing Task!", )
    await asyncio.sleep(2)
    logger.info("Task Completed!", )

async def run():
    await client.connect()
    
    await client.subscribe("dummy.foo", cb=callack_A)
    
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(run())
