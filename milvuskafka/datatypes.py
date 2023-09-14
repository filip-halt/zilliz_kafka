from pydantic import BaseModel
from typing import List, Optional


class MilvusDocument(BaseModel):
    chunk_id: str  # Primary key and unique for each chunk
    doc_id: str  # The ID of the original post in string format
    chunk: str  # The chunk of text
    title: str
    by: str
    url: Optional[str] = None
    embedding: Optional[List[float]] = None # Embedding will be included when inserting
    distance: Optional[float] = None # Distance will be included when returning search results


# Milvus consumer model for searches
class MilvusSearchRequest(BaseModel):
    query_id: str # Query ID that connects response back to query
    embedding: List[float]
    top_k: int


# Milvus producer model for queries
class MilvusSearchResponse(BaseModel):
    query_id: str
    results: List[MilvusDocument]


class HackerNewsPost(BaseModel):
    title: str
    by: str
    type: str
    id: int
    url: Optional[str] = None
    text: Optional[str] = None


class SearchRequest(BaseModel):
    query_id: str
    text: str
    top_k: int
