from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field

from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings


class OpenSearchBasicAuth(BaseModel):
    """Username/password authentication for OpenSearch."""

    kind: Literal["basic"] = "basic"
    username: str
    password: str


class OpenSearchAWSIAMAuth(BaseModel):
    """AWS SigV4 authentication for Amazon OpenSearch Service or Serverless.

    When ``aws_access_key_id`` / ``aws_secret_access_key`` are omitted, boto3
    resolves credentials from the standard chain (env vars, ~/.aws/credentials,
    EC2 instance profile, ECS task role, EKS IRSA, etc.).
    """

    kind: Literal["aws_iam"] = "aws_iam"
    region: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    assume_role_arn: Optional[str] = None
    # 'es' = Amazon OpenSearch Service (default); 'aoss' = OpenSearch Serverless
    service: Literal["es", "aoss"] = "es"


OpenSearchAuth = Annotated[
    Union[OpenSearchBasicAuth, OpenSearchAWSIAMAuth],
    Field(discriminator="kind"),
]


class OpenSearchDocTarget(FieldMappings):
    kind: Literal["opensearch_doc"] = "opensearch_doc"

    hosts: list[str]
    index: str
    auth: Optional[OpenSearchAuth] = None
    use_ssl: bool = False
    verify_certs: bool = True
    id_field: str = "doc_id"
    # OpenSearch maps unknown ints as 'long' (64-bit signed); docling's
    # binary_hash can exceed that range, so coerce by default.
    coerce_large_ints_to_str: bool = True


class OpenSearchChunkTarget(FieldMappings, ChunkFieldSlots):
    kind: Literal["opensearch_chunks"] = "opensearch_chunks"

    hosts: list[str]
    index: str
    auth: Optional[OpenSearchAuth] = None
    use_ssl: bool = False
    verify_certs: bool = True
    # Override base default: OpenSearch maps ints as 'long' (64-bit signed);
    # docling's binary_hash can exceed that range.
    coerce_large_ints_to_str: bool = True


__all__ = [
    "OpenSearchAWSIAMAuth",
    "OpenSearchAuth",
    "OpenSearchBasicAuth",
    "OpenSearchChunkTarget",
    "OpenSearchDocTarget",
]
