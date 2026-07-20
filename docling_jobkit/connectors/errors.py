import inspect
from collections.abc import Callable
from functools import wraps
from typing import NoReturn, ParamSpec, TypeVar, cast

_P = ParamSpec("_P")
_R = TypeVar("_R")


class ConnectorAuthenticationError(RuntimeError):
    """Safe, client-actionable connector authentication failure."""


class SourceConnectorAuthenticationError(ConnectorAuthenticationError):
    """Authentication failure while opening a task source."""


def map_connector_authentication_errors(
    connector_name: str,
    is_authentication_error: Callable[[BaseException], bool],
    *,
    source: bool = False,
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
            "supply valid credentials."
        ) from exc

    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        if inspect.isgeneratorfunction(func):

            @wraps(func)
            def generator_wrapper(*args: _P.args, **kwargs: _P.kwargs):
                try:
                    yield from func(*args, **kwargs)
                except Exception as exc:
                    if not is_authentication_error(exc):
                        raise
                    translate(exc)

            return cast(Callable[_P, _R], generator_wrapper)

        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if not is_authentication_error(exc):
                    raise
                translate(exc)

        return wrapper

    return decorator
