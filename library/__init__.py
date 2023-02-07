from typing import TypedDict


class S3Location(TypedDict):
    S3Bucket: str
    S3ObjectName: str