import socket
import logging
import asyncio, ssl
from urllib.parse import urlunparse
from typing import List, Optional, Union, Awaitable, Callable

from nats.aio import client
from nats.aio.msg import Msg

from nats.aio.client import ( 
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_RECONNECT_TIME_WAIT,
    DEFAULT_MAX_RECONNECT_ATTEMPTS,
    DEFAULT_PING_INTERVAL,
    DEFAULT_MAX_OUTSTANDING_PINGS,
    DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_INBOX_PREFIX,
    DEFAULT_PENDING_SIZE,

    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    DEFAULT_SUB_PENDING_BYTES_LIMIT,

    Subscription,

    ErrorCallback,
    Callback,

    SignatureCallback,
    JWTCallback,
    Credentials,
)

from opentelemetry.trace import Tracer, use_span, Context, set_span_in_context, get_current_span
from opentelemetry import _logs as logs
from opentelemetry._logs import LogRecord

from .tracing import setup_tracer
from .logging import setup_logging
from .config import NATSotelSettings, PROPAGATOR


class Client(client.Client):
    def __init__(self, config: NATSotelSettings = None, tracer: Optional[Tracer] = None, log_handler = None ):
        self.config = config if config else NATSotelSettings()
        self.tracer = tracer if tracer else setup_tracer(self.config)
        self.log_handler = log_handler if log_handler else setup_logging(self.config)

        for logger_name in ["natsotel", self.config.service_name]:
            logging.getLogger(logger_name).addHandler(self.log_handler)
            logging.getLogger(logger_name).setLevel(logging.INFO)

        super().__init__()

    async def connect(self,
        servers: Union[str, List[str]] = ["nats://localhost:4222"],
        error_cb: Optional[ErrorCallback] = None,
        disconnected_cb: Optional[Callback] = None,
        closed_cb: Optional[Callback] = None,
        discovered_server_cb: Optional[Callback] = None,
        reconnected_cb: Optional[Callback] = None,
        name: Optional[str] = None,
        pedantic: bool = False,
        verbose: bool = False,
        allow_reconnect: bool = True,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: int = DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: bool = False,
        flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: bool = False,
        tls: Optional[ssl.SSLContext] = None,
        tls_hostname: Optional[str] = None,
        tls_handshake_first: bool = False,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Optional[SignatureCallback] = None,
        user_jwt_cb: Optional[JWTCallback] = None,
        user_credentials: Optional[Credentials] = None,
        nkeys_seed: Optional[str] = None,
        nkeys_seed_str: Optional[str] = None,
        inbox_prefix: Union[str, bytes] = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: Optional[float] = None,
    ):
        await super().connect(
            servers = servers,
            name = name,
            reconnected_cb = self._make_event_cb("nats.reconnected", reconnected_cb),
            disconnected_cb = self._make_event_cb("nats.disconnected", disconnected_cb),
            discovered_server_cb = self._make_event_cb("nats.discovered_server", discovered_server_cb),
            error_cb = self._make_event_cb("nats.error", error_cb),
            closed_cb = self._make_event_cb("nats.closed", closed_cb),
            pedantic = pedantic,
            verbose = verbose,
            allow_reconnect = allow_reconnect,
            connect_timeout = connect_timeout,
            reconnect_time_wait = reconnect_time_wait,
            max_reconnect_attempts = max_reconnect_attempts,
            ping_interval = ping_interval,
            max_outstanding_pings = max_outstanding_pings,
            dont_randomize = dont_randomize,
            flusher_queue_size = flusher_queue_size,
            no_echo = no_echo,
            tls = tls,
            tls_hostname = tls_hostname,
            tls_handshake_first = tls_handshake_first,
            user = user,
            password = password,
            token = token,
            drain_timeout = drain_timeout,
            signature_cb = signature_cb,
            user_jwt_cb = user_jwt_cb,
            user_credentials = user_credentials,
            nkeys_seed = nkeys_seed,
            nkeys_seed_str = nkeys_seed_str,
            inbox_prefix = inbox_prefix,
            pending_size = pending_size,
            flush_timeout = flush_timeout,
        )

    def _make_event_cb(self, cb_name: str, _cb: Optional[ErrorCallback | Callback] = None):
        async def cb(*args, **kwargs):            
            # Add a Trace
            with self.tracer.start_as_current_span(cb_name) as span:
                span_attributes = {}
                span_attributes["host"] = socket.gethostname()
                span_attributes.update(self._server_info)

                # Log the event
                logger = logging.getLogger(self.config.service_name)

                logger.warn(
                    f"{cb_name.capitalize()}",
                    extra = span_attributes
                )

                # Set span attributes
                span.set_attributes(span_attributes)

                if _cb:
                    await _cb(*args, **kwargs)

                    callback_attributes = {
                        "callback.module": cb.__module__,
                        "callback.repr": str(cb.__repr__()),
                        "callback.name": cb.__code__.co_name,
                        "callback.names": list(cb.__code__.co_names),
                        "callback.qualname": cb.__code__.co_qualname,
                        "callback.filename": cb.__code__.co_filename,
                    }

                    # Create an event for triggered callback
                    span.add_event(
                        "callback",
                        attributes={
                            **span_attributes,
                            # Callback
                            **callback_attributes
                        }
                    )


        return cb

    async def publish(self, subject: str, data: bytes, headers: dict = None, context: Optional[Context] = None):
        headers = (headers or {})

        if self.config.trace_subject:
            headers["Nats-Trace-Dest"] = self.config.trace_subject

        if self.config.trace_only:
            headers["NATS-Trace-Only"] = self.config.trace_only

        with self.tracer.start_as_current_span(f"nats.publish({subject})", context=context) as span:
            # Log the event
            span_attributes = {}
            span_attributes.update(self._server_info)
            span_attributes["host"] = socket.gethostname()
            span_attributes["nats.subject"] = subject
            span_attributes["nats.msgsize"] = len(data)
            span_attributes["nats.payload"] = data.decode()

            logger = logging.getLogger(self.config.service_name)
            logger.info(
                f"Publishing {len(data)} bytes of data in `{subject}`", 
                extra=span_attributes
            )

            # Set span attributes
            span.set_attributes(span_attributes)

            # Create an event for Msg sent
            span.add_event(
                "sent",
                attributes={
                    "nats.subject": subject,
                    "nats.payload": data.decode(),
                }
            )

            # Inject current context into headers
            PROPAGATOR.inject(headers)

            await super().publish(subject, data, headers=headers)

    async def subscribe(self, subject: str, cb: Callback):
        async def wrapper(msg):
            # Extract tracing context from headers
            ctx = PROPAGATOR.extract(msg.header or {})

            with self.tracer.start_as_current_span(f"nats.subscribe({subject})", context=ctx ) as span:
                span_attributes = {}
                span_attributes["host"] = socket.gethostname()
                span_attributes.update(self._server_info)
                span_attributes["nats.subject"] = subject
                span_attributes["nats.payload"] = msg.data.decode()

                callback_attributes = {
                    "callback.module": cb.__module__,
                    "callback.repr": str(cb.__repr__()),
                    "callback.name": cb.__code__.co_name,
                    "callback.names": list(cb.__code__.co_names),
                    "callback.qualname": cb.__code__.co_qualname,
                    "callback.filename": cb.__code__.co_filename,
                }

                # Log the event
                logger = logging.getLogger(self.config.service_name)

                logger.info(
                    f"Received {len(msg.data)} bytes of data in `{subject}`", 
                    extra=span_attributes
                )

                # Set span attributes
                span.set_attributes(span_attributes)

                # Create an event for Msg received
                span.add_event(
                    "received",
                    attributes=span_attributes
                )

                # Trigger the callback
                await cb(msg)

                # Create an event for triggered callback
                span.add_event(
                    "callback",
                    attributes={
                        **span_attributes,
                        # Callback
                        **callback_attributes
                    }
                )

        await super().subscribe(subject, cb=wrapper)

    async def raw_subscribe(self, 
        subject: str,
        queue: str = "",
        cb: Optional[Callable[[Msg], Awaitable[None]]] = None,
        future: Optional[asyncio.Future] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
        return await super().subscribe(
            subject = subject,
            queue = queue,
            cb = cb,
            future = future,
            max_msgs = max_msgs,
            pending_msgs_limit = pending_msgs_limit,
            pending_bytes_limit = pending_bytes_limit,
        )