"""Spark Connect transport using only ordinary public PySpark expressions."""
from typing import Any

from .base import PublicExpressionBackend


class ConnectBackend(PublicExpressionBackend):
    name="connect-public-expressions"
    protocol_version="1"
    def __init__(self,spark:Any):
        super().__init__(spark)
