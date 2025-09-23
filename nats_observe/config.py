from typing import List, Optional, Any, Mapping
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

PROPAGATOR: Any = TraceContextTextMapPropagator()

class OTLPTraceConfig(BaseModel):
    otlp_trace_endpoint: Optional[str] = "localhost:5081"
    otlp_trace_insecure: bool = True
    otlp_trace_header: Optional[Mapping[str, str]] = None

class OTLPLogsConfig(BaseModel):
    otlp_logs_endpoint: Optional[str] = "localhost:5081"
    otlp_logs_insecure: bool = True
    otlp_logs_header: Optional[Mapping[str, str]] = None

class ZipkinExporterConfig(BaseModel):
    zipkin_endpoint: Optional[str] = "http://localhost:9411/api/v2/spans"

class ConsoleExporterConfig(BaseModel):
    console: bool = False

class NATSConfig(BaseModel):
    servers: List[str] = ["nats://127.0.0.1:4222"]
    trace_subject: str = "trace.logs"
    trace_only: str = "true"

class NATSotelSettings(BaseSettings, NATSConfig, OTLPTraceConfig, OTLPLogsConfig):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="_",
        case_sensitive=False
    )
    
    service_name: str = "natsotel-service"
