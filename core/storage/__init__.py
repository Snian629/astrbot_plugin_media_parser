"""存储与缓存管理模块，负责文件清理和缓存目录注册。"""
from .file_cleaner import cleanup_file, cleanup_files, cleanup_directory
from .cache_registry import CacheRegistry, stamp_subdir

__all__ = [
    "cleanup_file",
    "cleanup_files",
    "cleanup_directory",
    "CacheRegistry",
    "stamp_subdir",
]
