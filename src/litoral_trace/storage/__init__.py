"S3-compatible private object-storage boundary for Litoral Trace."
from litoral_trace.storage.s3 import (
    Boto3S3ObjectStorage,
    ObjectDeleteResult,
    ObjectHead,
    ObjectStorageClient,
    ObjectStorageConfigurationError,
    ObjectStorageError,
    ObjectStorageNotFoundError,
    ObjectStorageStream,
    ObjectWriteResult,
)

__all__ = [
    "Boto3S3ObjectStorage",
    "ObjectDeleteResult",
    "ObjectHead",
    "ObjectStorageClient",
    "ObjectStorageConfigurationError",
    "ObjectStorageError",
    "ObjectStorageNotFoundError",
    "ObjectStorageStream",
    "ObjectWriteResult",
]