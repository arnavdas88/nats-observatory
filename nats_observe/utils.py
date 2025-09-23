from typing import List
from nats.aio.msg import Msg

from opentelemetry.trace.span import SpanContext
from .config import PROPAGATOR

def get_trace_context(msg: Msg) -> dict | None:
    # Extract tracing context from headers
    ctx = PROPAGATOR.extract(msg.header or {})

    return (ctx if ctx else None)

def get_trace_spancontext(msg: Msg) -> List[SpanContext] | None:
    # Extract tracing context from headers
    ctx = get_trace_context(msg)

    if not ctx:
        return None

    span_context_list = [v.get_span_context() for v in ctx.values()]
    return span_context_list