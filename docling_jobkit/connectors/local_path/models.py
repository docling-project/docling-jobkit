from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class LocalPathSource(BaseModel):
    kind: Literal["local_path"] = "local_path"
    path: Annotated[
        Path,
        Field(
            description=(
                "Local filesystem path to a file or directory. For files, the "
                "single file will be processed. For directories, files will be "
                "discovered based on the pattern and recursive settings. Required."
            ),
            examples=["/path/to/document.pdf", "/path/to/documents/", "./data/input/"],
        ),
    ]
    pattern: Annotated[
        str,
        Field(
            description=(
                "Glob pattern for matching files within a directory. Only applies "
                "to directories and defaults to all files."
            ),
            examples=["*.pdf", "*.{pdf,docx}", "**/*.pdf", "report_*.pdf"],
        ),
    ] = "*"
    recursive: Annotated[
        bool,
        Field(
            description="Recursively traverse subdirectories when the path is a directory."
        ),
    ] = True


class LocalPathTarget(BaseModel):
    kind: Literal["local_path"] = "local_path"
    path: Annotated[
        Path,
        Field(
            description=(
                "Local filesystem output path. Directories are created when needed."
            ),
            examples=["/path/to/output/", "./data/output/", "/path/to/output.json"],
        ),
    ]
