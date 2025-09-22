import socket
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter, ChannelCredentials

from opentelemetry import _logs as logs
from opentelemetry.sdk._logs import Logger, LoggerProvider, LogRecord
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor, ConsoleLogExporter

from .config import NATSotelSettings

def setup_tracer(config: NATSotelSettings, instrumenting_module_name: Optional[str] = None):
    # Define resource attributes for your service
    resource = Resource.create(attributes={SERVICE_NAME: config.service_name})

    # Create a TraceProvider
    trace_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(trace_provider)

    # OTLP Exporter
    if config.otlp_trace_endpoint:
        otlp = OTLPSpanExporter(
            endpoint=config.otlp_trace_endpoint,
            insecure=config.otlp_trace_insecure,
            headers=config.otlp_trace_header,
        )
        # Trace
        trace_provider.add_span_processor(BatchSpanProcessor(otlp))

    return trace.get_tracer(
        instrumenting_module_name if instrumenting_module_name else "natsotel"
    )
