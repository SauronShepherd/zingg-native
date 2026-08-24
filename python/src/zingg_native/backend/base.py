"""Transport selection for the JVM-owned ordinary Zingg implementation."""
from typing import Any, Protocol

class ExecutionBackend(Protocol):
    name: str
    def transform(self, df: Any, operation: str, **options: Any) -> Any: ...

class PublicExpressionBackend:
    """Control-only backend; it does not duplicate JVM semantic rewrites."""
    name="transport-only"
    def __init__(self,spark:Any=None): self.spark=spark
    def transform(self,df:Any,operation:str,**options:Any)->Any:
        raise NotImplementedError("Python semantic rewrites are not a production path; run ordinary Zingg through the canonical JVM NativeOperationProvider")
    def preprocess(self,df:Any,operation:str,columns:list[str],**parameters:Any)->Any:
        raise NotImplementedError("Python semantic rewrites are not a production path; run ordinary Zingg through the canonical JVM NativeOperationProvider")
    def capabilities(self)->dict[str,Any]:
        return {"protocol_version":"1","operations":[],"transport":"control-only","semantic_owner":"ai.zingg.nativebridge.NativeOperationProvider"}

# Backward-compatible name for callers that only need transport selection.
PrototypeExpressionBackend=PublicExpressionBackend

def resolve_backend(spark:Any,requested:str|None=None)->ExecutionBackend:
    if requested is None:
        from ..runtime import detect_runtime
        mode=detect_runtime(spark).api_mode
    else: mode=requested.lower()
    if mode=="classic":
        from .classic import ClassicBackend
        return ClassicBackend(spark)
    if mode == "connect":
        from .connect import ConnectBackend
        return ConnectBackend(spark)
    if mode in {"expressions","public-expressions"}:
        return PublicExpressionBackend(spark)
    raise ValueError("backend must be classic, connect, or public-expressions")
