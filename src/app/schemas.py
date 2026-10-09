from typing import List
from pydantic import BaseModel, Field

class TaskRequest(BaseModel):
    task_id: str = Field(..., description="采集任务ID")
    task_type: int = Field(..., description="任务类型")
    keywords: List[str] = Field(..., description="关键词列表")
    priority: int = Field(..., description="任务优先级")
    max_page: int = Field(..., description="采集页数")
    request_interval: int = Field(..., description="请求间隔（秒）")
