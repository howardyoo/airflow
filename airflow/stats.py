#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import logging
import socket
import string
import textwrap
import time
from functools import wraps
from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, TypeVar, Union, cast
from unicodedata import name

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.typing_compat import Protocol
# ------- OPEN TELEMETRY --------
from opentelemetry._metrics import get_meter_provider, set_meter_provider
from opentelemetry._metrics.instrument import Instrument
from opentelemetry.sdk._metrics.measurement import Measurement
from opentelemetry.sdk.trace import Tracer
from opentelemetry.sdk.trace import Span
from opentelemetry import trace
from opentelemetry.sdk import util
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.exporter.otlp.proto.grpc._metric_exporter import (
    OTLPMetricExporter
)
from opentelemetry.sdk._metrics.export import ConsoleMetricExporter
from opentelemetry.sdk._metrics import MeterProvider
from opentelemetry.sdk._metrics.export import PeriodicExportingMetricReader
import json
from opentelemetry.trace import Link

log = logging.getLogger(__name__)


class TimerProtocol(Protocol):
    """Type protocol for StatsLogger.timer"""

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_value, traceback):
        ...

    def start(self):
        """Start the timer"""
        ...

    def stop(self, send=True):
        """Stop, and (by default) submit the timer to statsd"""
        ...


class StatsLogger(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)"""

    @classmethod
    def incr(cls, stat: str, count: int = 1, rate: int = 1, attributes: Optional[dict[str,str]] = None) -> None:
        """Increment stat"""

    @classmethod
    def decr(cls, stat: str, count: int = 1, rate: int = 1, attributes: Optional[dict[str,str]] = None) -> None:
        """Decrement stat"""

    @classmethod
    def gauge(cls, stat: str, value: float, rate: int = 1, delta: bool = False, attributes: Optional[dict[str,str]] = None) -> None:
        """Gauge stat"""

    @classmethod
    def timing(cls, stat: str, dt: Union[float, datetime.timedelta], attributes: Optional[dict[str,str]] = None) -> None:
        """Stats timing"""

    @classmethod
    def timer(cls, attributes: Optional[dict[str,str]] = None, *args, **kwargs) -> TimerProtocol:
        """Timer metric that can be cancelled"""


class Timer:
    """
    Timer that records duration, and optional sends to statsd backend.

    This class lets us have an accurate timer with the logic in one place (so
    that we don't use datetime math for duration -- it is error prone).

    Example usage:

    .. code-block:: python

        with Stats.timer() as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)

    Or without a context manager:

    .. code-block:: python

        timer = Stats.timer().start()

        # Something to time
        frob_the_foos()

        timer.end()

        log.info("Frobbing the foos took %.2f", timer.duration)

    To send a metric:

    .. code-block:: python

        with Stats.timer("foos.frob"):
            # Something to time
            frob_the_foos()

    Or both:

    .. code-block:: python

        with Stats.timer("foos.frob") as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)
    """

    # pystatsd and dogstatsd both have a timer class, but present different API
    # so we can't use this as a mixin on those, instead this class is contains the "real" timer

    _start_time: Optional[int]
    duration: Optional[int]

    def __init__(self, real_timer=None):
        self.real_timer = real_timer

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        """Start the timer"""
        if self.real_timer:
            self.real_timer.start()
        self._start_time = time.perf_counter()
        return self

    def stop(self, send=True):
        """Stop the timer, and optionally send it to stats backend"""
        self.duration = time.perf_counter() - self._start_time
        if send and self.real_timer:
            self.real_timer.stop()


class DummyStatsLogger:
    """If no StatsLogger is configured, DummyStatsLogger is used as a fallback"""

    @classmethod
    def incr(cls, stat, count=1, rate=1, attributes: Optional[dict[str,str]] = None):
        """Increment stat"""

    @classmethod
    def decr(cls, stat, count=1, rate=1, attributes: Optional[dict[str,str]] = None):
        """Decrement stat"""

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False, attributes: Optional[dict[str,str]] = None):
        """Gauge stat"""

    @classmethod
    def timing(cls, stat, dt, attributes: Optional[dict[str,str]] = None):
        """Stats timing"""

    @classmethod
    def timer(cls, attributes: Optional[dict[str,str]] = None, *args, **kwargs):
        """Timer metric that can be cancelled"""
        return Timer()


# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = set(string.ascii_letters + string.digits + '_.-')


def stat_name_default_handler(stat_name, max_length=250) -> str:
    """A function that validate the statsd stat name, apply changes to the stat name
    if necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException('The stat_name has to be a string')
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            textwrap.dedent(
                """\
            The stat_name ({stat_name}) has to be less than {max_length} characters.
        """.format(
                    stat_name=stat_name, max_length=max_length
                )
            )
        )
    if not all((c in ALLOWED_CHARACTERS) for c in stat_name):
        raise InvalidStatsNameException(
            textwrap.dedent(
                """\
            The stat name ({stat_name}) has to be composed with characters in
            {allowed_characters}.
            """.format(
                    stat_name=stat_name, allowed_characters=ALLOWED_CHARACTERS
                )
            )
        )
    return stat_name


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    """Get Stat Name Handler from airflow.cfg"""
    return conf.getimport('metrics', 'stat_name_handler') or stat_name_default_handler


T = TypeVar("T", bound=Callable)


def validate_stat(fn: T) -> T:
    """Check if stat name contains invalid characters.
    Log and not emit stats if name is invalid
    """

    @wraps(fn)
    def wrapper(_self, stat=None, *args, **kwargs):
        try:
            if stat is not None:
                handler_stat_name_func = get_current_handler_stat_name_func()
                stat = handler_stat_name_func(stat)
            return fn(_self, stat, *args, **kwargs)
        except InvalidStatsNameException:
            log.error('Invalid stat name: %s.', stat, exc_info=True)
            return None

    return cast(T, wrapper)


class AllowListValidator:
    """Class to filter unwanted stats"""

    def __init__(self, allow_list=None):
        if allow_list:

            self.allow_list = tuple(item.strip().lower() for item in allow_list.split(','))
        else:
            self.allow_list = None

    def test(self, stat):
        """Test if stat is in the Allow List"""
        if self.allow_list is not None:
            return stat.strip().lower().startswith(self.allow_list)
        else:
            return True  # default is all metrics allowed


class SafeStatsdLogger:
    """Statsd Logger"""

    def __init__(self, statsd_client, allow_list_validator=AllowListValidator()):
        self.statsd = statsd_client
        self.allow_list_validator = allow_list_validator

    @validate_stat
    def incr(self, stat, count=1, rate=1, attributes: Optional[dict[str,str]] = None):
        """Increment stat"""
        if self.allow_list_validator.test(stat):
            return self.statsd.incr(stat, count, rate)
        return None

    @validate_stat
    def decr(self, stat, count=1, rate=1, attributes: Optional[dict[str,str]] = None):
        """Decrement stat"""
        if self.allow_list_validator.test(stat):
            return self.statsd.decr(stat, count, rate)
        return None

    @validate_stat
    def gauge(self, stat, value, rate=1, delta=False, attributes: Optional[dict[str,str]] = None):
        """Gauge stat"""
        if self.allow_list_validator.test(stat):
            return self.statsd.gauge(stat, value, rate, delta)
        return None

    @validate_stat
    def timing(self, stat, dt, attributes: Optional[dict[str,str]] = None):
        """Stats timing"""
        if self.allow_list_validator.test(stat):
            return self.statsd.timing(stat, dt)
        return None

    @validate_stat
    def timer(self, stat=None, attributes: Optional[dict[str,str]] = None, *args, **kwargs):
        """Timer metric that can be cancelled"""
        if stat and self.allow_list_validator.test(stat):
            return Timer(self.statsd.timer(stat, *args, **kwargs))
        return Timer()


class SafeDogStatsdLogger:
    """DogStatsd Logger"""

    def __init__(self, dogstatsd_client, allow_list_validator=AllowListValidator()):
        self.dogstatsd = dogstatsd_client
        self.allow_list_validator = allow_list_validator

    @validate_stat
    def incr(self, stat, count=1, rate=1, tags=None, attributes: Optional[dict[str,str]] = None):
        """Increment stat"""
        if self.allow_list_validator.test(stat):
            tags = tags or []
            return self.dogstatsd.increment(metric=stat, value=count, tags=tags, sample_rate=rate)
        return None

    @validate_stat
    def decr(self, stat, count=1, rate=1, tags=None, attributes: Optional[dict[str,str]] = None):
        """Decrement stat"""
        if self.allow_list_validator.test(stat):
            tags = tags or []
            return self.dogstatsd.decrement(metric=stat, value=count, tags=tags, sample_rate=rate)
        return None

    @validate_stat
    def gauge(self, stat, value, rate=1, delta=False, tags=None, attributes: Optional[dict[str,str]] = None):
        """Gauge stat"""
        if self.allow_list_validator.test(stat):
            tags = tags or []
            return self.dogstatsd.gauge(metric=stat, value=value, tags=tags, sample_rate=rate)
        return None

    @validate_stat
    def timing(self, stat, dt: Union[float, datetime.timedelta], tags: Optional[List[str]] = None, attributes: Optional[dict[str,str]] = None):
        """Stats timing"""
        if self.allow_list_validator.test(stat):
            tags = tags or []
            if isinstance(dt, datetime.timedelta):
                dt = dt.total_seconds()
            return self.dogstatsd.timing(metric=stat, value=dt, tags=tags)
        return None

    @validate_stat
    def timer(self, stat=None, *args, tags=None, attributes: Optional[dict[str,str]] = None, **kwargs):
        """Timer metric that can be cancelled"""
        if stat and self.allow_list_validator.test(stat):
            tags = tags or []
            return Timer(self.dogstatsd.timed(stat, *args, tags=tags, **kwargs))
        return Timer()


class _Stats(type):
    factory = None
    instance: Optional[StatsLogger] = None

    def __getattr__(cls, name):
        if not cls.instance:
            try:
                print("Stats >> creating instance..")
                cls.instance = cls.factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using DummyStatsLogger instead.", e)
                cls.instance = DummyStatsLogger()
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs):
        super().__init__(cls)
        if cls.__class__.factory is None:
            is_datadog_enabled_defined = conf.has_option('metrics', 'statsd_datadog_enabled')
            if is_datadog_enabled_defined and conf.getboolean('metrics', 'statsd_datadog_enabled'):
                cls.__class__.factory = cls.get_dogstatsd_logger
            elif conf.has_option('metrics', 'otel_on') and conf.getboolean('metrics', 'otel_on'):
                cls.__class__.factory = cls.get_otel_logger
            elif conf.getboolean('metrics', 'statsd_on'):
                cls.__class__.factory = cls.get_statsd_logger
            else:
                cls.__class__.factory = DummyStatsLogger

    @classmethod
    def get_statsd_logger(cls):
        """Returns logger for statsd"""
        # no need to check for the scheduler/statsd_on -> this method is only called when it is set
        # and previously it would crash with None is callable if it was called without it.
        from statsd import StatsClient

        stats_class = conf.getimport('metrics', 'statsd_custom_client_path', fallback=None)

        if stats_class:
            if not issubclass(stats_class, StatsClient):
                raise AirflowConfigException(
                    "Your custom Statsd client must extend the statsd.StatsClient in order to ensure "
                    "backwards compatibility."
                )
            else:
                log.info("Successfully loaded custom Statsd client")

        else:
            stats_class = StatsClient

        statsd = stats_class(
            host=conf.get('metrics', 'statsd_host'),
            port=conf.getint('metrics', 'statsd_port'),
            prefix=conf.get('metrics', 'statsd_prefix'),
        )
        allow_list_validator = AllowListValidator(conf.get('metrics', 'statsd_allow_list', fallback=None))
        return SafeStatsdLogger(statsd, allow_list_validator)

    @classmethod
    def get_dogstatsd_logger(cls):
        """Get DataDog statsd logger"""
        from datadog import DogStatsd

        dogstatsd = DogStatsd(
            host=conf.get('metrics', 'statsd_host'),
            port=conf.getint('metrics', 'statsd_port'),
            namespace=conf.get('metrics', 'statsd_prefix'),
            constant_tags=cls.get_constant_tags(),
        )
        dogstatsd_allow_list = conf.get('metrics', 'statsd_allow_list', fallback=None)
        allow_list_validator = AllowListValidator(dogstatsd_allow_list)
        return SafeDogStatsdLogger(dogstatsd, allow_list_validator)

    @classmethod
    def get_constant_tags(cls):
        """Get constant DataDog tags to add to all stats"""
        tags = []
        tags_in_string = conf.get('metrics', 'statsd_datadog_tags', fallback=None)
        if tags_in_string is None or tags_in_string == '':
            return tags
        else:
            for key_value in tags_in_string.split(','):
                tags.append(key_value)
            return tags

    # ------- OTEL logger -------
    @classmethod
    def get_otel_logger(cls):
        """Get Otel logger"""
        host=conf.get('metrics', 'otel_host')
        port=conf.getint('metrics', 'otel_port')
        prefix=conf.get('metrics', 'otel_prefix')
        debug=conf.getboolean('metrics','otel_debug')
        otel_allow_list = conf.get('metrics', 'otel_allow_list', fallback=None)
        allow_list_validator = AllowListValidator(otel_allow_list)
        print(f"[Metric Exporter] Connecting to OTLP at ---> http://{host}:{port}")
        if debug == True:
            exporter = ConsoleMetricExporter()
        else:
            exporter = OTLPMetricExporter(endpoint=f"http://{host}:{port}", insecure=True, timeout=300)
        # initial interval is set to 30 seconds
        print("getting reader..")
        reader = PeriodicExportingMetricReader(exporter=exporter, export_interval_millis=60000)
        # if we do not set 'shutdown_on_exit' to False, somehow(?) the 
        # MeterProvider will constantly get shutdown every second
        # something having to do with the following code:
        # if shutdown_on_exit:
        #     self._atexit_handler = register(self.shutdown)
        # not sure why...
        provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
        set_meter_provider(provider)
        return SafeOtelLogger(get_meter_provider(), prefix, allow_list_validator)


# ------ FOR OTEL POC --------
class DummyInstrument(Instrument):
    def __init__(self, name, unit="", description="", attributes=None):
        self.name = name
        self.unit = unit
        self.description = description
        self.attributes = attributes

# gauge map which holds gauge metrics
# for POC purpose this map has been made rather simple
class GaugeMap():
    def __init__(self, meter):
        self.meter = meter
        self.map = {}
    
    def clear(self) -> None:
        self.map.clear()

    # set value would store the value of the gauge
    def set_value(self, name, value, unit="", description="", attributes=None) -> None:

        if attributes == None:
            attributes = {}

        key = name + str(util.get_dict_as_key(attributes))
        # any previously existing measurement would get effectively overwritten
        self.map[key] = Measurement(value, DummyInstrument(name, unit, description), attributes)

    # retrieve readings
    def get_readings(self) -> Iterable[Measurement]:
        ret = self.poke_readings()
        # clear the map when getting the readings
        # in this way, any accumulated gauge wouldn't survive
        # once the readings are extracted.
        self.clear()
        return ret
    
    # poke readings, without clearing the gauge map
    def poke_readings(self) -> Iterable[Measurement]:
        ret = []
        for val in self.map.values():
            ret.append(val)
        return ret

class CounterMap():
    def __init__(self, meter):
        self.meter = meter
        self.map = {}
    
    def clear(self) -> None:
        self.map.clear()

    def get_counter(self, name, attributes: Optional[dict[str,str]] = None):
        key = name + str(attributes)
        if key in self.map.keys():
            # print("returning counter with " + key)
            return self.map[key]
        else:
            # create if doesn't exist
            # print("--> creating counter with " + key)
            counter = self.meter.create_up_down_counter(name)
            self.map[key] = counter
            return counter

    def del_counter(self, name, attributes: Optional[dict[str,str]] = None) -> None:
        key = name + str(attributes)
        if key in self.map.keys():
            del self.map[key]



# ----- OTEL (metric) Logger ------
class SafeOtelLogger:
    """Otel Logger"""

    def __init__(self, otel_provider, prefix="airflow", point_tags=None, allow_list_validator=AllowListValidator()):
        self.otel = otel_provider
        self.prefix = prefix
        self.point_tags = point_tags
        self.meter = otel_provider.get_meter("airflow_otel")
        self.gaugemap = {}
        self.point_tags = dict(conf.getsection('metrics_tags'))
        self.allow_list_validator = allow_list_validator
        self.gauge_map = GaugeMap(self.meter)
        self.counter_map = CounterMap(self.meter)
        self.meter.create_observable_gauge(name="airflow", callback=self.gauge_map.get_readings)


    # --- HOWARD ---
    # merge attributes if exists
    def merge_attributes(self, attributes=None):
        if self.point_tags != None:
            if attributes != None:
                attr = {}
                attr.update(attributes)
                attr.update(self.point_tags)
                return attr
            else:
                return self.point_tags
        elif attributes != None:
            return attributes
        else:
            return None

    @validate_stat
    def incr(self, stat, count=1, rate=1, attributes: Optional[dict[str,str]] = None):
        """Increment stat"""
        if self.allow_list_validator.test(stat):
            counter = self.counter_map.get_counter(f"{self.prefix}.{stat}")
            # we do not use 'rate' here, but only the count
            return counter.add(count * rate, attributes=self.merge_attributes(attributes))
        return None

    @validate_stat
    def decr(self, stat, count=1, rate=1, attributes: Optional[dict[str,str]] = None):
        """Decrement stat"""
        if self.allow_list_validator.test(stat):
            counter = self.counter_map.get_counter(f"{self.prefix}.{stat}")
            return counter.add(-1 * (count * rate), attributes=self.merge_attributes(attributes))
        return None

    @validate_stat
    def gauge(self, stat, value, rate=1, delta=False, attributes: Optional[dict[str,str]] = None):
        """Gauge stat"""
        if self.allow_list_validator.test(stat):
            self.gauge_map.set_value(f"{self.prefix}.{stat}", value, attributes=self.merge_attributes(attributes))
        return None

    @validate_stat
    def timing(self, stat, dt, attributes: Optional[dict[str,str]] = None):
        """Stats timing"""
        if self.allow_list_validator.test(stat):
            if isinstance(dt, datetime.timedelta):
                dt = dt.total_seconds()
            self.gauge_map.set_value(f"{self.prefix}.{stat}", dt, attributes=self.merge_attributes(attributes))
            # -- experimental --
            # using histogram to contain these timer data does not seem
            # appropriate, so commenting out.
            # print(f"timing --> {self.prefix}.{stat} : {dt}")
            # histogram = self.meter.create_histogram(f"{self.prefix}.{stat}")
            # return histogram.record(dt, attributes=self.merge_attributes(attributes))
        return None

    @validate_stat
    def timer(self, stat=None, attributes: Optional[dict[str,str]] = None, *args, **kwargs):
        """Timer metric that can be cancelled"""
        return Timer()

# OTEL (trace) Logger ----- which is used to produce distributed tracing
class OtelTraceLogger:
    
    # --- HOWARD ---
    # merge attributes if exists
    def merge_attributes(self, attributes=None):
        if self.point_tags != None:
            if attributes != None:
                attr = {}
                attr.update(attributes)
                attr.update(self.point_tags)
                return attr
            else:
                return self.point_tags
        elif attributes != None:
            return attributes
        else:
            return None

    def __init__(self, span_exporter, prefix=""):
        self.span_exporter = span_exporter
        self.prefix = prefix
        if self.prefix != "":
            self.prefix = f"{prefix}."
        # map of airflow ID and open telemetry ID
        # key might be dag_id or task_id.
        # in case of dag_id, trace_id of open telemetry would be returned.
        # in case of task_id, span_id of open telemetry would be returned.
        self.span_map = {}
        self.poin_tags = dict(conf.getsection('metrics_tags'))

    def get_tracer(self, service="service", application="airflow") -> Tracer:
        # get the tracer if already registered, and create a new one if not.
        key = f"{application}.{self.prefix}{service}"
        if key in _Trace.tracerMap.keys():
            return _Trace.tracerMap[key]
        else:
            resource = Resource(attributes={
                "application": application,
                "service.name": service
            })
            # Need to make sure to set shutdown_on_exit to False
            tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=True)
            span_processor = BatchSpanProcessor(self.span_exporter)
            tracer_provider.add_span_processor(span_processor)
            tracer = tracer_provider.get_tracer(key)
            _Trace.tracerMap[key] = tracer
            return tracer

    def set_span(self, key, value):
        self.span_map[key] = value

    # returns None if key is not found
    def get_span(self, key):
        if key in self.span_map.keys():
            return self.span_map[key]
        else:
            return None

    def get_span_keys(self):
        if len(self.span_map) > 0:
            return self.span_map.keys()
        else:
            return iter([])

    def remove_span(self, key):
        if key in self.span_map.keys():
            del self.span_map[key]

    def start_span(self, service_name, span_name) -> Span:
        tracer = self.get_tracer(service_name, "airflow")
        span = tracer.start_span(span_name)
        return span

    def start_span_from_dagrun(self, service_name, span_name, dagrun) -> Span:
        span_json = json.loads(dagrun.span_json)
        tracer = self.get_tracer(service_name, "airflow")
        span_context = SpanContext(
            trace_id=int(span_json['context']['trace_id'], 16),
            span_id=int(span_json['context']['span_id'], 16),
            is_remote=True,
            trace_flags=TraceFlags(0x01) # setting this to 0x01 is Necessary
        )
        ctx = trace.set_span_in_context(NonRecordingSpan(span_context))
        # link = Link(span_context)
        # links = []
        # links.append(link)
        span = tracer.start_span(span_name, context=ctx, links=None)
        print(f"start_span_from_dagrun -> {span.to_json()}")
        return span

    def start_span_from_taskinstance(self, service_name, span_name, ti) -> Span:
        span_json = json.loads(ti.span_json)
        tracer = self.get_tracer(service_name, "airflow")
        span_context = SpanContext(
            trace_id=int(span_json['context']['trace_id'], 16),
            span_id=int(span_json['context']['span_id'], 16),
            is_remote=True,
            trace_flags=TraceFlags(0x01) # setting this to 0x01 is Necessary
        )
        ctx = trace.set_span_in_context(NonRecordingSpan(span_context))
        # link = Link(span_context)
        # links = []
        # links.append(link)
        span = tracer.start_span(span_name, context=ctx, links=None)
        return span

    def get_span_from_json(self, span_json):
        span_json = json.loads(span_json)        
        application = span_json['resource']['application']
        service_name = span_json['resource']['service.name']

        if span_json['parent_id'] != None:
            # parent context
            span_context = SpanContext(
                trace_id=int(span_json['context']['trace_id'], 16),
                span_id=int(span_json['parent_id'], 16),
                is_remote=True,
                trace_flags=TraceFlags(0x01) # setting this to 0x01 is Necessary
            )
            ctx = trace.set_span_in_context(NonRecordingSpan(span_context))
            tracer = self.get_tracer(service=service_name, application=application)
            span = tracer.start_span(span_json['name'], context=ctx)
        else:
            tracer = self.get_tracer(service=service_name, application=application)
            span = tracer.start_span(span_json['name'])

        span_context = SpanContext(
            trace_id=int(span_json['context']['trace_id'], 16),
            span_id=int(span_json['context']['span_id'], 16),
            is_remote=False,
            trace_flags=TraceFlags(0x01)    # setting this to 0x01 is Necessary
        )
        # should change everything
        span._context = span_context
        span.set_attributes(span_json['attributes'])
        # setup events
        events = span_json['events']
        for event in events:
            _name = event['name']
            _attr = event['attributes']
            span.add_event(_name, _attr)
        
        start_time = span_json['attributes']['start_time']
        # span._kind = span_json['kind']
        span._start_time = start_time
        # create links
        # span._links = span_json['links']
        return span


# Trace related classes
class TraceLogger(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)"""

    @classmethod
    def get_tracer(cls, service, application) -> Tracer:
        """get tracer"""

    @classmethod
    def set_span(cls, key, value):
        """set span"""

    @classmethod
    def get_span(cls, key):
        """get span"""

    @classmethod
    def get_span_keys(cls):
        """get span keys"""

    @classmethod
    def remove_span(self, key):
        """remove span"""

    @classmethod
    def start_span(self, service_name, span_name):
        """start span"""

    @classmethod
    def start_span_from_dagrun(self, service_name, span_name, dagrun):
        """start span from dagrun"""

    @classmethod
    def start_span_from_taskinstance(self, service_name, span_name, ti):
        """start span from taskinstance"""

    @classmethod
    def get_span_from_json(self, span_json):
        """get span from json"""

class DummyTraceLogger:
    """If no TraceLogger is configured, DummyTraceLogger is used as a fallback"""

    @classmethod
    def get_tracer(cls, service, application):
        """get tracer"""

    @classmethod
    def set_span(cls, key, value):
        """set span"""

    @classmethod
    def get_span(cls, key):
        """get span"""

    @classmethod
    def get_span_keys(cls):
        """get span keys"""
    
    @classmethod
    def remove_span(self, key):
        """remove span"""

    @classmethod
    def start_span(self, service_name, span_name):
        """start span"""

    @classmethod
    def start_span_from_dagrun(self, service_name, span_name, dagrun):
        """start span from dagrun"""

    @classmethod
    def start_span_from_taskinstance(self, service_name, span_name, ti):
        """start span from taskinstance"""

    @classmethod
    def get_span_from_json(self, span_json):
        """get span from json"""


class _Trace(type):
    factory = None
    instance: Optional[TraceLogger] = None
    tracerMap = {}

    def __getattr__(cls, name):
        if not cls.instance:
            try:
                cls.instance = cls.factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure Trace: %s, using DummyTraceLogger instead.", e)
                cls.instance = DummyTraceLogger()
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs):
        super().__init__(cls)
        if cls.__class__.factory is None:
            if conf.has_option('metrics', 'otel_on') and conf.getboolean('metrics', 'otel_on'):
                cls.__class__.factory = cls.get_otel_tracer
            else:
                cls.__class__.factory = DummyTraceLogger

    @classmethod
    def get_otel_tracer(cls):
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import(
            OTLPSpanExporter
        )
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter
        
        host=conf.get('metrics', 'otel_host')
        port=conf.getint('metrics', 'otel_port')
        prefix=conf.get('metrics', 'otel_prefix')
        debug=conf.getboolean('metrics','otel_debug')

        if debug == True:
            span_exporter = ConsoleSpanExporter()
        else:
            print(f"[Span Exporter] Connecting to OTLP at ---> http://{host}:{port}")
            span_exporter = OTLPSpanExporter(endpoint=f"http://{host}:{port}", insecure=True, timeout=1)

        return OtelTraceLogger(span_exporter, prefix)


if TYPE_CHECKING:
    Stats: StatsLogger
    Trace: TraceLogger
else:

    class Stats(metaclass=_Stats):
        """Empty class for Stats - we use metaclass to inject the right one"""
    
    class Trace(metaclass=_Trace):
        """Empty class for Trace - we use metaclass to inject the right one"""
