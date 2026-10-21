import mimetypes
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional

from minilake.errors import ConfigurationError, OptionalDependencyError


class S3Uploader:
    def __init__(self, region: Optional[str] = None, client: object = None) -> None:
        if client is not None:
            self.client = client
            return
        try:
            import boto3
            from boto3.s3.transfer import TransferConfig
        except ImportError as exc:
            raise OptionalDependencyError(
                "S3 support is not installed. Run: pip install -e '.[aws]'"
            ) from exc
        self.client = boto3.client("s3", region_name=region)
        self.transfer_config = TransferConfig(
            multipart_threshold=16 * 1024 * 1024,
            multipart_chunksize=16 * 1024 * 1024,
            max_concurrency=8,
            use_threads=True,
        )

    def upload_tree(self, source: Path, bucket: str, prefix: str = "") -> List[Dict[str, object]]:
        if not source.is_dir():
            raise ConfigurationError(f"Upload source must be a directory: {source}")
        files = sorted(path for path in source.rglob("*") if path.is_file() and ".staging" not in path.parts)
        if not files:
            raise ConfigurationError(f"No files found under {source}")
        uploaded = []
        for path in files:
            relative = PurePosixPath(path.relative_to(source).as_posix())
            key = str(PurePosixPath(prefix.strip("/")) / relative) if prefix.strip("/") else str(relative)
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            kwargs = {"ExtraArgs": {"ContentType": content_type}}
            if hasattr(self, "transfer_config"):
                kwargs["Config"] = self.transfer_config
            self.client.upload_file(str(path), bucket, key, **kwargs)
            uploaded.append({"uri": f"s3://{bucket}/{key}", "size_bytes": path.stat().st_size})
        return uploaded

