import asyncio
import logging
import threading
from typing import Union

import httpx

from docling_jobkit.datamodel.callback import (
    CallbackSpec,
    ProgressCallbackRequest,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
)

_log = logging.getLogger(__name__)


class CallbackInvoker:
    """Handles callback invocations with retry logic."""

    def __init__(
        self,
        max_retries: int = 3,
        timeout: float = 30.0,
        retry_delay: float = 1.0,
    ):
        self.max_retries = max_retries
        self.timeout = timeout
        self.retry_delay = retry_delay

    def invoke_callbacks_async(
        self,
        callbacks: list[CallbackSpec],
        task_id: str,
        progress: Union[
            ProgressSetNumDocs, ProgressDocumentCompleted, ProgressUpdateProcessed
        ],
    ) -> None:
        """
        Invoke all callbacks for a progress event in a non-blocking way.

        This method spawns a daemon thread to handle the async callback invocations,
        ensuring the conversion loop is not blocked.
        """
        if not callbacks:
            return

        def _run_async():
            asyncio.run(self._invoke_callbacks_internal(callbacks, task_id, progress))

        thread = threading.Thread(target=_run_async, daemon=True)
        thread.start()

    async def _invoke_callbacks_internal(
        self,
        callbacks: list[CallbackSpec],
        task_id: str,
        progress: Union[
            ProgressSetNumDocs, ProgressDocumentCompleted, ProgressUpdateProcessed
        ],
    ) -> None:
        """Internal async method to invoke all callbacks."""
        request = ProgressCallbackRequest(
            task_id=task_id,
            progress=progress,
        )

        # Invoke all callbacks concurrently
        tasks = [
            self._invoke_single_callback(callback, request) for callback in callbacks
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _invoke_single_callback(
        self,
        callback: CallbackSpec,
        request: ProgressCallbackRequest,
    ) -> None:
        """Invoke a single callback with retry logic."""
        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(
                    verify=callback.ca_cert or True,
                    timeout=self.timeout,
                ) as client:
                    response = await client.post(
                        str(callback.url),
                        json=request.model_dump(mode="json"),
                        headers=callback.headers,
                    )
                    response.raise_for_status()
                    _log.info(
                        f"Callback invoked successfully: {callback.url} "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    return  # Success, exit retry loop

            except Exception as e:
                _log.warning(
                    f"Callback invocation failed (attempt {attempt + 1}/{self.max_retries}): "
                    f"{callback.url} - {e}"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    _log.error(
                        f"Callback invocation failed after {self.max_retries} attempts: "
                        f"{callback.url}"
                    )
