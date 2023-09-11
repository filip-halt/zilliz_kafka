from pydantic import BaseModel
from typing import List, Optional


class MilvusDocument(BaseModel):
    chunk_id: str # Primary key and unique for each chunk
    doc_id: str # The ID of the original post in string format
    chunk: str # The chunk of text
    title: str
    by: str
    url: Optional[str]
    embedding: Optional[List[float]] = None
    distance: Optional[float] = None

# Milvus consumer model for searches
class MilvusSearchRequest(BaseModel):
    query_id: str
    embedding: List[float]
    top_k: int
    
# Milvus consumer model for inserts
class MilvusInsertRequest(BaseModel):
    insert_id: str
    doc: MilvusDocument

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



