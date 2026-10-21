import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class OutputFile:
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    created_at: str
    input_files: List[str]
    rows_read: int
    rows_written: int
    output_files: List[OutputFile]
    output_bytes: int
    partition_by: List[str]
    compression: str
    schema: str

    @classmethod
    def create(
        cls,
        run_id: str,
        input_files: List[str],
        rows_read: int,
        output_root: Path,
        output_paths: List[Path],
        partition_by: List[str],
        compression: str,
        schema: str,
    ) -> "RunManifest":
        files = [
            OutputFile(
                path=str(path.relative_to(output_root)),
                size_bytes=path.stat().st_size,
                sha256=sha256_file(path),
            )
            for path in sorted(output_paths)
        ]
        return cls(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            input_files=input_files,
            rows_read=rows_read,
            rows_written=rows_read,
            output_files=files,
            output_bytes=sum(item.size_bytes for item in files),
            partition_by=partition_by,
            compression=compression,
            schema=schema,
        )

    def write(self, output_root: Path) -> Path:
        manifest_dir = output_root / "_manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        destination = manifest_dir / f"{self.run_id}.json"
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")
        temporary.replace(destination)
        return destination

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

