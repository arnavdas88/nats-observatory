import asyncio

from nats_observe.tracing import setup_tracer
from nats_observe.logging import setup_logging
from nats_observe.config import NATSotelSettings
from nats_observe.client import Client as NATSotel
from nats_observe.handlers import default_trace_handler

async def run():
    cfg = NATSotelSettings()
    cfg.otlp_trace_header["stream-name"] = "natsotel"
    cfg.otlp_logs_header["stream-name"] = "natsotel"
    tracer = setup_tracer(cfg)
    logger = setup_logging(cfg)
    client = NATSotel(cfg, tracer, logger)

    await client.connect(cfg.servers)

    # Default tracing subscription
    await client.raw_subscribe(
        cfg.trace_subject,
        cb=default_trace_handler(client.tracer)
    )
    
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(run())
