import ast
from pathlib import Path


def test_jobkit_core_only_imports_connector_contracts() -> None:
    root = Path(__file__).parents[1] / "docling_jobkit"
    shared = {
        "artifact_paths",
        "auth_context",
        "connector_factory",
        "errors",
        "source_processor",
        "source_processor_factory",
        "target_processor",
        "target_processor_factory",
    }
    provider_modules = {
        "azure",
        "boto3",
        "botocore",
        "google",
        "googleapiclient",
        "requests",
    }
    leaks: list[str] = []

    for path in root.rglob("*.py"):
        if path.is_relative_to(root / "connectors"):
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            modules = (
                [node.module]
                if isinstance(node, ast.ImportFrom)
                else [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else []
            )
            for module in filter(None, modules):
                if module.startswith("docling_jobkit.connectors."):
                    contract = module.split(".")[2]
                    if contract not in shared:
                        leaks.append(
                            f"{path.relative_to(root)}:{node.lineno}: {module}"
                        )
                if module.split(".")[0] in provider_modules:
                    leaks.append(f"{path.relative_to(root)}:{node.lineno}: {module}")

    assert not leaks, "connector imports leaked into jobkit core:\n" + "\n".join(leaks)
