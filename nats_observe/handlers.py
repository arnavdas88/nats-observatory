import json
import socket
import logging

from collections import Counter

from .config import PROPAGATOR
from .utils import get_trace_spancontext

from opentelemetry.context import Context
from opentelemetry.trace import Tracer


def default_trace_handler(tracer: Tracer, verbose_logging: bool = True):
    async def handler(msg):
        try:
            payload = json.loads(msg.data.decode())
        except Exception:
            payload = {}

        logger = logging.getLogger('natsotel')

        trace_dest = payload.get("request", {}).get("header", {}).get("Nats-Trace-Dest", [msg.subject])
        header = payload.get("request", {}).get("header", {})
        traceparent = payload.get("request", {}).get("header", {}).get("traceparent", None)
        server = payload.get("server", {})
        events = payload.get("events", [])


        ctx = PROPAGATOR.extract(header)
        span_context_list = [v.get_span_context() for v in ctx.values()]

        ctx_extra = {}
        
        if span_context_list:
            ctx_extra["traceparent"] = traceparent[0]
            ctx_extra["trace_id"] = f"{span_context_list[0].trace_id:x}"
            ctx_extra["span_id"] = f"{span_context_list[0].span_id:x}"

        # Nats.io Event Stats
        nats_event_stats = Counter([event.get("type") for event in events])
        # nats_event_stats.get('in', 0)
        # nats_event_stats.get('eg', 0)

        if verbose_logging:
            for event in events:
                event_type = event.get("type")
                event_src_subject = event.get("subj")
                extra_information = {
                    **ctx_extra,
                    **event
                }
                if event_type == "in":
                    logger.info(f"Nats.io Ingress - {event_src_subject}", extra=event)
                elif event_type == "eg":
                    logger.info(f"Nats.io Egress - {event_src_subject}", extra=event)
        
        stats_msg = f"Nats.io Msg Ingress {nats_event_stats.get('in', 0)}, Egress {nats_event_stats.get('eg', 0)}"
        if 'eg' in nats_event_stats:
            logger.info(stats_msg, extra=ctx_extra)
        else:
            logger.warn(stats_msg, extra=ctx_extra)

        # with tracer.start_as_current_span("nats.message", ) as span:
        #     for span_ctx in span_context_list:
        #         span.add_link(span_ctx,)

        #     for k, v in server.items():
        #         span.set_attribute(f"nats.server.{k}", v)

        #     # logger.info("Received Result!")
        #     for ev in payload.get("events", []):
        #         # logger.info("Received Result!")
        #         with tracer.start_as_current_span(f"event.{ev.get('type','unknown')}") as ev_span:
        #             for k, v in ev.items():
        #                 ev_span.set_attribute(f"event.{k}", v)

    return handler
