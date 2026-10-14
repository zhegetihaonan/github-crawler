from typing import List, Literal
from pydantic import BaseModel, Field, field_validator


class TaskRequest(BaseModel):
    task_id: str = Field(..., min_length=1, max_length=255, description="采集任务ID")
    task_type: Literal[0, 1] = Field(..., description="0=单次任务，1=周期任务")
    keywords: List[str] = Field(..., min_length=1, description="关键词列表")
    priority: int = Field(..., ge=0, le=10, description="RabbitMQ 优先级，0-10")
    max_page: int = Field(..., ge=1, le=100, description="采集页数")
    request_interval: int = Field(..., ge=1, description="请求间隔（秒）")

    @field_validator("keywords")
    @classmethod
    def validate_keywords(cls, keywords):
        cleaned = [keyword.strip() for keyword in keywords]
        if any(not keyword for keyword in cleaned):
            raise ValueError("keywords cannot contain empty values")
        if any(len(keyword) > 200 for keyword in cleaned):
            raise ValueError("each keyword must be 200 characters or fewer")
        if len(cleaned) > 100:
            raise ValueError("no more than 100 keywords are allowed per task")
        return list(dict.fromkeys(cleaned))
