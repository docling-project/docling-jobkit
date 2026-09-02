from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, model_validator


class BoxSource(BaseModel):
    """Box source connector configuration.

    Supports two mutually distinguishable auth strategies against the same set of
    connection fields: Client Credentials Grant (CCG) — ``client_id``/``client_secret``
    plus one of ``enterprise_id``/``user_id`` — or JWT service-account auth, which adds
    ``jwt_key_id``/``private_key``/``private_key_passphrase`` on top of the CCG fields.
    Which mode applies is inferred from whether the JWT-only fields are set (see
    ``auth_mode``), mirroring how the SharePoint connector infers its target from
    ``site_url`` vs ``onedrive_user`` rather than a separate discriminator field.
    """

    kind: Literal["box"] = "box"

    client_id: Annotated[
        str,
        Field(
            description=(
                "Client ID of the Box custom app (Developer Console) granted access "
                "to the enterprise or user content to be read."
            ),
        ),
    ]

    client_secret: Annotated[
        SecretStr,
        Field(description="Client secret generated for the Box custom app."),
    ]

    enterprise_id: Annotated[
        Optional[str],
        Field(
            default=None,
            description=(
                "Box enterprise ID to authenticate as the app's service account. "
                "Exactly one of 'enterprise_id' or 'user_id' must be provided."
            ),
        ),
    ] = None

    user_id: Annotated[
        Optional[str],
        Field(
            default=None,
            description=(
                "Box user ID to authenticate as (impersonation). Exactly one of "
                "'enterprise_id' or 'user_id' must be provided."
            ),
        ),
    ] = None

    jwt_key_id: Annotated[
        Optional[str],
        Field(
            default=None,
            description=(
                "Public key ID of the JWT keypair configured on the Box custom app. "
                "Setting this switches auth to JWT; must be combined with "
                "'private_key' and 'private_key_passphrase'."
            ),
        ),
    ] = None

    private_key: Annotated[
        Optional[SecretStr],
        Field(
            default=None,
            description=(
                "PEM-encoded private key of the JWT keypair. Required together with "
                "'jwt_key_id' and 'private_key_passphrase' for JWT authentication."
            ),
        ),
    ] = None

    private_key_passphrase: Annotated[
        Optional[SecretStr],
        Field(
            default=None,
            description="Passphrase protecting 'private_key'.",
        ),
    ] = None

    # optional params
    folder_id: Annotated[
        str,
        Field(
            default="0",
            description=(
                "ID of the Box folder to read from. Subfolders are traversed "
                "recursively. Defaults to '0', the root folder of the authenticated "
                "identity's content."
            ),
        ),
    ] = "0"

    file_ids: Annotated[
        Optional[list[str]],
        Field(
            default=None,
            description=(
                "IDs of individual files to process. If set, overrides folder_id "
                "traversal."
            ),
        ),
    ] = None

    max_num_elements: Annotated[
        Optional[int],
        Field(
            default=None,
            description=(
                "Optional maximum number of documents to process. "
                "If omitted, all matching documents will be processed."
            ),
        ),
    ] = None

    @model_validator(mode="after")
    def _validate_auth(self) -> "BoxSource":
        jwt_fields = (self.jwt_key_id, self.private_key, self.private_key_passphrase)
        if any(f is not None for f in jwt_fields) and not all(
            f is not None for f in jwt_fields
        ):
            raise ValueError(
                "'jwt_key_id', 'private_key', and 'private_key_passphrase' must all "
                "be provided together for JWT authentication."
            )
        if bool(self.enterprise_id) == bool(self.user_id):
            raise ValueError(
                "Exactly one of 'enterprise_id' or 'user_id' must be provided."
            )
        return self

    @property
    def auth_mode(self) -> Literal["jwt", "ccg"]:
        return "jwt" if self.private_key is not None else "ccg"


__all__ = ["BoxSource"]
