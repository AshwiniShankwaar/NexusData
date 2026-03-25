"""nexus_data/models.py"""
from __future__ import annotations
from typing import Any, List, Optional
from pydantic import BaseModel, Field


class ColumnMeta(BaseModel):
    name: str
    type: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_indexed: bool = False                         # #7 index-aware
    is_enum: bool = False                            # #1 enum detection
    sample_values: List[Any] = Field(default_factory=list)
    all_values: List[Any] = Field(default_factory=list)  # #1 full enum list
    col_min: Optional[Any] = None                   # #2 numeric stats
    col_max: Optional[Any] = None
    col_avg: Optional[float] = None
    cardinality_ratio: float = 1.0
    inferred_type: str = ""
    description: str = ""


class TableMeta(BaseModel):
    name: str
    columns: List[ColumnMeta] = Field(default_factory=list)
    description: str = ""


class DatabaseProfile(BaseModel):
    tables: List[TableMeta] = Field(default_factory=list)
    dialect: str = "unknown"


class QueryResult(BaseModel):
    sql: str
    rows: List[Any] = Field(default_factory=list)
    columns: List[str] = Field(default_factory=list)
    error: Optional[str] = None
    from_cache: bool = False
    is_clarification: bool = False
    clarification_question: Optional[str] = None
    natural_language_summary: Optional[str] = None
    confidence: float = 1.0
    anomaly_warnings: List[str] = Field(default_factory=list)   # #1 anomaly detection
    performance_hints: List[str] = Field(default_factory=list)  # #4 performance advisor
    diff_summary: Optional[str] = None                          # #6 result diffing
    execution_ms: Optional[float] = None                        # #10 audit log timing


class NLQuery(BaseModel):
    text: str
    session_id: str = ""
