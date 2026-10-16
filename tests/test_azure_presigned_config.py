import pytest

from docling.datamodel.service.sources import AzureBlobCoordinates

from docling_jobkit.config.target_config import AzurePresignedConfig


def _coords(connection_string: str, *, account_name: str = "acct"):
    return AzureBlobCoordinates(
        account_name=account_name,
        container="artifacts",
        connection_string=connection_string,
    )


def test_azure_presigned_config_accepts_account_key_with_trailing_separator():
    config = AzurePresignedConfig(
        azure_coords=_coords("AccountName=acct;AccountKey=dGVzdA==;")
    )

    assert config.get_account_key() == "dGVzdA=="


@pytest.mark.parametrize(
    ("connection_string", "account_name", "message"),
    [
        ("AccountKey=secret", "acct", "must include AccountName"),
        ("AccountName=acct", "acct", "must include AccountKey"),
        (
            "AccountName=other;AccountKey=secret",
            "acct",
            "account_name must match",
        ),
        ("AccountName", "acct", "key=value entries"),
    ],
)
def test_azure_presigned_config_rejects_invalid_connection_strings_without_leak(
    connection_string: str,
    account_name: str,
    message: str,
):
    with pytest.raises(ValueError, match=message) as exc_info:
        AzurePresignedConfig(
            azure_coords=_coords(
                connection_string,
                account_name=account_name,
            )
        )

    assert "secret" not in str(exc_info.value)
