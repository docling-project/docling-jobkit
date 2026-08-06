import inspect
from collections.abc import Callable, Iterable
from functools import wraps
from typing import NoReturn, ParamSpec, TypeVar, cast

from pydantic import BaseModel

_P = ParamSpec("_P")
_R = TypeVar("_R")


class ConnectorAuthenticationError(RuntimeError):
    """Safe, client-actionable connector authentication failure."""


class SourceConnectorError(RuntimeError):
    """Safe source-connector failure for core error classification."""

    def __init__(self, message: str, *, source_kind: str, retryable: bool):
        super().__init__(message)
        self.source_kind = source_kind
        self.retryable = retryable


class SourceConnectorAuthenticationError(
    ConnectorAuthenticationError, SourceConnectorError
):
    """Authentication failure while opening a task source."""

    def __init__(self, message: str, *, source_kind: str = "connector"):
        SourceConnectorError.__init__(
            self, message, source_kind=source_kind, retryable=False
        )


class SourceConnectorPolicyError(SourceConnectorError):
    def __init__(self, message: str, *, source_kind: str):
        super().__init__(message, source_kind=source_kind, retryable=False)


class SourceConnectorUnavailableError(SourceConnectorError):
    def __init__(self, message: str, *, source_kind: str, retryable: bool = True):
        super().__init__(message, source_kind=source_kind, retryable=retryable)


class SourceConnectorConfigError(ValueError):
    """Safe failure while resolving or validating a task source config."""


class TargetConnectorConfigError(ValueError):
    """Safe failure while resolving or validating a task target config."""


class KafkaConfigError(ValueError):
    """Raised when Kafka is configured on only one side of a job."""


def validate_kafka_kind_pairing(
    sources: Iterable[BaseModel], target: BaseModel
) -> None:
    """Ensures that Kafka is used as both source and target connector or neither."""
    source_kinds = {getattr(s, "kind", None) for s in sources}
    target_is_kafka = getattr(target, "kind", None) == "kafka"
    source_has_kafka = "kafka" in source_kinds

    if source_has_kafka != target_is_kafka:
        raise KafkaConfigError(
            "Kafka must be used on both sides. "
            "A Kafka source requires a kafka target and vice versa"
        )
    if source_has_kafka and source_kinds != {"kafka"}:
        raise KafkaConfigError("A Kafka job cannot mix 'kafka' with non-kafka sources")


def map_connector_authentication_errors(
    connector_name: str,
    is_authentication_error: Callable[[BaseException], bool],
    *,
    source: bool = False,
    source_kind: str = "connector",
    is_unavailable_error: Callable[[BaseException], bool] | None = None,
    unavailable_message: str = "Source could not be reached.",
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Translate recognized SDK auth failures from a connector method."""

    def translate(exc: Exception) -> NoReturn:
        error_type = (
            SourceConnectorAuthenticationError
            if source
            else ConnectorAuthenticationError
        )
        raise error_type(
            f"{connector_name} authentication failed; verify permissions and "
            "supply valid credentials.",
            **({"source_kind": source_kind} if source else {}),
        ) from exc

    def translate_or_raise(exc: Exception) -> NoReturn:
        if is_authentication_error(exc):
            translate(exc)
        if source and is_unavailable_error and is_unavailable_error(exc):
            raise SourceConnectorUnavailableError(
                unavailable_message,
                source_kind=source_kind,
            ) from exc
        raise exc

    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        if inspect.isgeneratorfunction(func):

            @wraps(func)
            def generator_wrapper(*args: _P.args, **kwargs: _P.kwargs):
                try:
                    yield from func(*args, **kwargs)
                except Exception as exc:
                    translate_or_raise(exc)

            return cast(Callable[_P, _R], generator_wrapper)

        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                translate_or_raise(exc)

        return wrapper

    return decorator
