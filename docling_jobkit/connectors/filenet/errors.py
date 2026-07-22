from docling_jobkit.connectors.errors import SourceConnectorPolicyError


class FileNetGraphQLError(SourceConnectorPolicyError):
    def __init__(self, message: str):
        super().__init__(message, source_kind="filenet")
