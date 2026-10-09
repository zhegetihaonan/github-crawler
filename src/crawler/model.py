from pydantic import BaseModel


class GithubResult(BaseModel):
    # 任务id
    task_id: str
    # 爬虫id
    crawler_id: str
    # 数据来源标识（github / gitlab）
    source: str
    # 关键词
    keyword: str
    # 相关项目 / 仓库实体名称
    name: str
    # 项目或仓库的描述信息
    description: str
    # 仓库地址
    repo_url: str
    # 匹配到的字段（项目名 / 文件内容 等）
    matched_field: str
    # 文件路径（相对仓库路径）
    file_path: str
    # 文件在 MinIO 或对象存储中的访问 URL
    file_minio_url: str
    # 文件名
    file_name: str = ''
    # 记录发现时间（now()）
    found_at: str = ''
    # 仓库拥有者名称（用户或组织）
    repo_owner_name: str = ''
    # 仓库拥有者主页 URL
    repo_owner_url: str = ''
    # 最后一次提交的作者姓名
    last_commit_author_name: str = ''
    # 最后一次提交的作者邮箱
    last_commit_author_email: str = ''
    # 最后一次提交时间
    last_commit_time: str = ''


class DocResult(BaseModel):
    # 任务id
    task_id: str
    # 爬虫id
    crawler_id: str
    # 数据来源标识（github / gitlab）
    source: str
    # 关键词
    keyword: str
    # 相关项目 / 仓库实体名称
    name: str
    # 项目或仓库的描述信息
    description: str
    # 仓库地址
    article_url: str
    # 原文链接（页面）
    matched_field: str
    # 文件路径
    file_path: str = ''
    # 文件在 MinIO 或对象存储中的访问 URL
    file_minio_url: str
    # 文件名
    file_name: str = ''
    # 记录发现时间（now()）
    found_at: str = ''
    # 仓库拥有者名称（用户或组织）
    rating: float = 0.0
    # 浏览量 / 阅读数
    view_count: int = 0
    # 作者
    file_author: str = ''
    # 文件大小（字节）
    doc_size: int = 0
    # 文档发布时间
    create_time: str = ''
    # 文档类型
    doc_type: int = 0


if __name__ == '__main__':
    data = {
        "task_id": "",
        "crawler_id": "",
        "source": "github",  # 数据来源标识（github / gitlab）
        "keyword": "金融_转账",  # 关键词
        "name": "Kunyu1006/mdapaper",  # 相关项目 / 仓库实体名称
        "description": "",  # 项目或仓库的描述信息
        "repo_url": "https://github.com/Kunyu1006/mdapaper",  # 仓库地址
        "matched_field": "file_content",  # 匹配到的字段（项目名 / 文件内容 / 文件路径等）
        "file_path": "7_30成果汇总/行业/计算机设备/2021/300551-古鳌科技-2021.txt",  # 文件路径（相对仓库路径）
        "file_minio_url": "http://minio.xxx/github/xxx",  # 文件在 MinIO 或对象存储中的访问 URL
        "file_name": "300551-古鳌科技-2021.txt",  # 文件名
        "found_at": "2026-01-19 15:40:12",  # 记录发现时间（now()）
        "repo_owner_name": "Kunyu1006",  # 仓库拥有者名称（用户或组织）
        "repo_owner_url": "https://github.com/Kunyu1006"
    }
    t = GithubResult(**data)
    print(t.model_dump_json())
