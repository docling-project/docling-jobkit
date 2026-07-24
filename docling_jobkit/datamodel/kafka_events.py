from typing import Annotated, Any, Literal, Optional

from pydantic import BaseModel, Field

from docling_jobkit.datamodel.convert import ConvertDocumentsOptions


class JobTriggerEvent(BaseModel):
    schema_version: int = 1

    correlation_id: Annotated[
        str, Field(description="Unique id tying this job to its status/error events")
    ]

    locator: Annotated[
        str,
        Field(
            description="Backend-relative key/path resolved against the inner source",
            examples=["inbox/report.pdf"],
        ),
    ]

    metadata: Annotated[
        Optional[dict[str, Any]],
        Field(description="Tenancy/routing tags mirroring Task.metadata"),
    ] = None

    filename: Annotated[
        Optional[str],
        Field(description="Display name for file, defaults to the locator"),
    ] = None

    options: Annotated[
        Optional[ConvertDocumentsOptions],
        Field(description="Per-job override of convert options"),
    ] = None

    idempotency_key: Annotated[
        Optional[str], Field(description="dedup key, defaults to locator")
    ] = None


class JobStatusEvent(BaseModel):
    schema_version: int = 1

    status: Literal["succeeded", "failed"]

    correlation_id: Annotated[
        str,
        Field(description="Unique id to keep track of a job between source and target"),
    ]

    output_ref: Annotated[
        list[str],
        Field(
            default_factory=list,
            description=(
                "Claim-check references to the result artifacts written to the inner "
                "target (one URI per artifact, since one document exports multiple formats)"
            ),
        ),
    ]


class JobErrorEvent(BaseModel):
    schema_version: int = 1

    correlation_id: Annotated[
        str, Field(description="Unique id to track the original job that failed")
    ]

    error_type: Annotated[str, Field(description="Exception/class name or category")]

    message: Annotated[
        str, Field(description="Human-readable description of what went wrong")
    ]

    retryable: Annotated[bool, Field(description="True = transient, False = permanent")]
