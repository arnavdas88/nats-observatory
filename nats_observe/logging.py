from typing import Optional

from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME


from .config import NATSotelSettings

def setup_logging(config: NATSotelSettings, instrumenting_module_name: Optional[str] = None):

    # Define resource attributes for your service
    resource = Resource.create(attributes={SERVICE_NAME: config.service_name})

    # Create a LoggerProvider
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)

    # Configure an OTLP exporter (e.g., to an OpenTelemetry Collector)
    # Replace the endpoint with your OTLP receiver address
    exporter = OTLPLogExporter(
            endpoint=config.otlp_logs_endpoint,
            insecure=config.otlp_logs_insecure,
            headers=config.otlp_logs_header,
    )

    # Add a BatchLogRecordProcessor to process and export logs
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

    # Attach the OpenTelemetry handler to the root logger or a specific logger
    # logging.NOTSET = 0
    handler = LoggingHandler(level=0, logger_provider=logger_provider)

    return handler