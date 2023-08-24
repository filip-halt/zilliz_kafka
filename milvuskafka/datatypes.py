from pydantic import BaseModel
from typing import List, Optional


class MilvusDocument(BaseModel):
    chunk_id: str # Primary key and unique for each chunk
    doc_id: str # ID of the document the chunk was made from (ie link or title)
    chunk: str # The chunk of text
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




