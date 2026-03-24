"""配置管理模块，负责默认值处理、类型转换与配置兜底。"""
import os
import tempfile
from typing import List

from .logger import logger

from .constants import Config
from .downloader.utils import check_cache_dir_available
from .parser.platform import (
    BilibiliParser,
    DouyinParser,
    KuaishouParser,
    WeiboParser,
    XiaohongshuParser,
    XiaoheiheParser,
    TwitterParser
)


BILIBILI_QUALITY_MAP = {
    "不限制": 0,
    "4K": 120,
    "1080P60": 116,
    "1080P+": 112,
    "1080P": 80,
    "720P": 64,
    "480P": 32,
    "360P": 16,
}


class ConfigManager:

    """配置读取门面，向业务层提供类型安全的配置访问。"""
    def __init__(self, config: dict):
        """初始化配置管理器

        Args:
            config: 原始配置字典

        Raises:
            ValueError: 没有启用任何解析器时
        """
        self._config = config
        self.bilibili_parser = None
        self._parse_config()

    def _parse_config(self):
        """解析配置。"""

        # --- trigger ---
        trigger = self._config.get("trigger", {})
        self.is_auto_parse = trigger.get("auto_parse", True)
        self.trigger_keywords = trigger.get(
            "keywords", ["视频解析", "解析视频"]
        )
        self.enable_reply_trigger = bool(
            trigger.get("reply_trigger", False)
        )
        if (
            not self.is_auto_parse
            and not self.trigger_keywords
            and not self.enable_reply_trigger
        ):
            logger.warning(
                "自动解析已关闭且未配置任何触发关键词，"
                "回复触发也已禁用，解析功能将完全不可用"
            )

        # --- message ---
        message = self._config.get("message", {})
        self.is_auto_pack = message.get("auto_pack", False)

        opening = message.get("opening", {})
        self.enable_opening_msg = opening.get("enable", True)
        self.opening_msg_content = opening.get(
            "content", "流媒体解析bot为您服务 ٩( 'ω' )و"
        )

        self.enable_text_metadata = message.get("text_metadata", True)

        hot_comments = message.get("hot_comments", {})
        if not isinstance(hot_comments, dict):
            hot_comments = {}
        self.hot_comment_count = self._parse_non_negative_int(
            hot_comments.get("count", 0), 0
        )
        self.hot_comment_bilibili = bool(hot_comments.get("bilibili", True))
        self.hot_comment_weibo = bool(hot_comments.get("weibo", True))
        self.hot_comment_xiaohongshu = bool(
            hot_comments.get("xiaohongshu", True)
        )
        if not self.enable_text_metadata:
            self.hot_comment_count = 0

        # --- permissions ---
        permissions = self._config.get("permissions", {})
        whitelist = permissions.get("whitelist", {})
        blacklist = permissions.get("blacklist", {})
        self.admin_id = str(permissions.get("admin_id", "") or "").strip()

        self.whitelist_enable = whitelist.get("enable", False)
        self.whitelist_user = self._normalize_id_list(
            whitelist.get("user", [])
        )
        if self.admin_id and self.admin_id not in self.whitelist_user:
            self.whitelist_user.append(self.admin_id)
        self.whitelist_group = self._normalize_id_list(
            whitelist.get("group", [])
        )

        self.blacklist_enable = blacklist.get("enable", False)
        self.blacklist_user = self._normalize_id_list(
            blacklist.get("user", [])
        )
        self.blacklist_group = self._normalize_id_list(
            blacklist.get("group", [])
        )

        # --- download ---
        download = self._config.get("download", {})

        self.max_video_size_mb = self._parse_non_negative_float(
            download.get("max_video_size_mb", 1000.0), 1000.0
        )
        large_video_threshold_mb = self._parse_non_negative_float(
            download.get(
                "large_video_threshold_mb",
                Config.MAX_LARGE_VIDEO_THRESHOLD_MB
            ),
            Config.MAX_LARGE_VIDEO_THRESHOLD_MB
        )
        if large_video_threshold_mb > 0:
            large_video_threshold_mb = min(
                large_video_threshold_mb,
                Config.MAX_LARGE_VIDEO_THRESHOLD_MB
            )
        self.large_video_threshold_mb = large_video_threshold_mb

        configured_cache_dir = str(
            download.get("cache_dir", "") or ""
        ).strip()
        if (
            not configured_cache_dir
            or configured_cache_dir == Config.DEFAULT_CACHE_DIR
        ):
            if os.path.exists('/.dockerenv'):
                self.cache_dir = Config.DEFAULT_CACHE_DIR
            else:
                self.cache_dir = os.path.join(
                    tempfile.gettempdir(), "astrbot_media_parser_cache"
                )
        else:
            self.cache_dir = configured_cache_dir

        self.pre_download_all_media = download.get("pre_download", False)
        self.max_concurrent_downloads = min(
            self._parse_positive_int(
                download.get(
                    "max_concurrent",
                    Config.DOWNLOAD_MANAGER_MAX_CONCURRENT
                ),
                Config.DOWNLOAD_MANAGER_MAX_CONCURRENT
            ),
            20
        )

        # --- media_relay ---
        relay = self._config.get("media_relay", {})
        self.use_file_token_service = relay.get("enable", False)
        self.callback_api_base = str(
            relay.get("callback_url", "") or ""
        ).strip().rstrip("/")
        self.file_token_ttl = max(
            30,
            self._parse_positive_int(relay.get("ttl", 300), 300)
        )
        if self.use_file_token_service:
            self.cache_dir = os.path.join(
                tempfile.gettempdir(),
                "astrbot_media_parser_relay_cache"
            )
            self.pre_download_all_media = True
            logger.info(
                f"媒体中转模式已启用，缓存目录: {self.cache_dir}，"
                f"预下载已强制开启"
            )

        if self.pre_download_all_media:
            if not check_cache_dir_available(self.cache_dir):
                logger.warning(
                    f"预下载模式已启用，但缓存目录不可用: {self.cache_dir}，"
                    f"将自动降级为禁用预下载模式"
                )
                self.pre_download_all_media = False

        # --- bilibili_enhanced ---
        bili = self._config.get("bilibili_enhanced", {})
        if not isinstance(bili, dict):
            bili = {}

        self.bilibili_use_cookie_for_parsing = bool(
            bili.get("use_cookie", False)
        )
        if self.bilibili_use_cookie_for_parsing:
            self.bilibili_cookie = str(
                bili.get("cookie", "") or ""
            ).strip()
            max_quality_label = str(
                bili.get("max_quality", "不限制") or "不限制"
            ).strip()
            self.bilibili_max_quality = BILIBILI_QUALITY_MAP.get(
                max_quality_label, 0
            )
            admin_assist = bili.get("admin_assist", {})
            if not isinstance(admin_assist, dict):
                admin_assist = {}
            self.bilibili_enable_admin_assist_on_expire = bool(
                admin_assist.get("enable", False)
            )
            self.bilibili_admin_reply_timeout_minutes = self._parse_positive_int(
                admin_assist.get("reply_timeout_minutes", 1440), 1440
            )
            self.bilibili_admin_request_cooldown_minutes = self._parse_positive_int(
                admin_assist.get("request_cooldown_minutes", 1440), 1440
            )
        else:
            self.bilibili_cookie = ""
            self.bilibili_max_quality = 0
            self.bilibili_enable_admin_assist_on_expire = False
            self.bilibili_admin_reply_timeout_minutes = 1440
            self.bilibili_admin_request_cooldown_minutes = 1440

        self.bilibili_cookie_feature_requested = self.bilibili_use_cookie_for_parsing
        self.bilibili_cookie_runtime_enabled = bool(
            self.bilibili_use_cookie_for_parsing and self.pre_download_all_media
        )
        runtime_file_name = "cookie.json"
        core_dir = os.path.dirname(os.path.abspath(__file__))
        cookie_dir = os.path.join(
            core_dir, "parser", "runtime_manager", "bilibili"
        )
        self.bilibili_cookie_runtime_file = os.path.join(
            cookie_dir, runtime_file_name
        )
        if self.bilibili_use_cookie_for_parsing:
            try:
                os.makedirs(cookie_dir, exist_ok=True)
            except Exception as e:
                logger.warning(
                    f"B站Cookie运行时目录不可用，将回退到缓存目录保存: {e}"
                )
                fallback_cookie_dir = os.path.join(
                    self.cache_dir, "runtime_manager", "bilibili"
                )
                self.bilibili_cookie_runtime_file = os.path.join(
                    fallback_cookie_dir, runtime_file_name
                )
        if (
            self.bilibili_cookie_feature_requested and
            not self.bilibili_cookie_runtime_enabled
        ):
            logger.warning(
                '检测到已开启"是否携带Cookie解析视频"，但预下载未启用或不可用，'
                "将旁路B站Cookie与协助登录流程，直接使用无Cookie直链模式。"
            )

        # --- parsers ---
        parsers = self._config.get("parsers", {})
        self.enable_bilibili = parsers.get("bilibili", True)
        self.enable_douyin = parsers.get("douyin", True)
        self.enable_kuaishou = parsers.get("kuaishou", True)
        self.enable_weibo = parsers.get("weibo", True)
        self.enable_xiaohongshu = parsers.get("xiaohongshu", True)
        self.enable_xiaoheihe = parsers.get("xiaoheihe", True)
        self.enable_twitter = parsers.get("twitter", True)

        # --- proxy ---
        proxy = self._config.get("proxy", {})
        self.proxy_addr = proxy.get("address", "")
        self.xiaoheihe_use_video_proxy = proxy.get("xiaoheihe_video", True)
        twitter_proxy = proxy.get("twitter", {})
        self.twitter_use_parse_proxy = twitter_proxy.get("parse", False)
        self.twitter_use_image_proxy = twitter_proxy.get("image", True)
        self.twitter_use_video_proxy = twitter_proxy.get("video", True)

        # --- admin ---
        admin = self._config.get("admin", {})
        self.clean_cache_keyword = str(
            admin.get("clean_cache_keyword", "清理媒体") or "清理媒体"
        ).strip()
        self.debug_mode = admin.get("debug", False)
        if self.debug_mode:
            import logging
            logger.setLevel(logging.DEBUG)
            logger.debug("Debug模式已启用")

    @staticmethod
    def _parse_positive_int(value, default: int) -> int:
        """将配置值解析为正整数，非法值回退为默认值。"""
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return max(1, int(default))

    @staticmethod
    def _parse_non_negative_float(value, default: float) -> float:
        """将配置值解析为非负浮点数，非法值回退为默认值。"""
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            return max(0.0, float(default))

    @staticmethod
    def _parse_non_negative_int(value, default: int) -> int:
        """将配置值解析为非负整数，非法值回退为默认值。"""
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return max(0, int(default))

    @staticmethod
    def _normalize_id_list(values) -> List[str]:
        """将管理员或白名单配置规范化为字符串 ID 集合。"""
        if not isinstance(values, list):
            return []
        normalized: List[str] = []
        seen = set()
        for value in values:
            if value is None:
                continue
            value_str = str(value).strip()
            if not value_str or value_str in seen:
                continue
            seen.add(value_str)
            normalized.append(value_str)
        return normalized

    def _effective_hot_comment_count(self, enabled: bool) -> int:
        """根据开关状态返回实际生效的热评条数。"""
        if not self.enable_text_metadata:
            return 0
        if not enabled:
            return 0
        return self.hot_comment_count

    def create_parsers(self) -> List:
        """创建解析器列表

        Returns:
            解析器列表

        Raises:
            ValueError: 没有启用任何解析器时
        """
        parsers = []
        bilibili_hot_comment_count = self._effective_hot_comment_count(
            self.hot_comment_bilibili
        )
        weibo_hot_comment_count = self._effective_hot_comment_count(
            self.hot_comment_weibo
        )
        xiaohongshu_hot_comment_count = self._effective_hot_comment_count(
            self.hot_comment_xiaohongshu
        )

        if self.enable_bilibili:
            self.bilibili_parser = BilibiliParser(
                cookie_runtime_enabled=self.bilibili_cookie_runtime_enabled,
                configured_cookie=self.bilibili_cookie,
                max_quality=self.bilibili_max_quality,
                admin_assist_enabled=self.bilibili_enable_admin_assist_on_expire,
                admin_reply_timeout_minutes=self.bilibili_admin_reply_timeout_minutes,
                admin_request_cooldown_minutes=self.bilibili_admin_request_cooldown_minutes,
                credential_path=self.bilibili_cookie_runtime_file,
                hot_comment_count=bilibili_hot_comment_count
            )
            parsers.append(self.bilibili_parser)
        if self.enable_douyin:
            parsers.append(DouyinParser())
        if self.enable_kuaishou:
            parsers.append(KuaishouParser())
        if self.enable_weibo:
            parsers.append(
                WeiboParser(hot_comment_count=weibo_hot_comment_count)
            )
        if self.enable_xiaohongshu:
            parsers.append(
                XiaohongshuParser(
                    hot_comment_count=xiaohongshu_hot_comment_count
                )
            )
        if self.enable_xiaoheihe:
            parsers.append(XiaoheiheParser(
                use_video_proxy=self.xiaoheihe_use_video_proxy,
                proxy_url=self.proxy_addr if self.proxy_addr else None
            ))
        if self.enable_twitter:
            parsers.append(TwitterParser(
                use_parse_proxy=self.twitter_use_parse_proxy,
                use_image_proxy=self.twitter_use_image_proxy,
                use_video_proxy=self.twitter_use_video_proxy,
                proxy_url=self.proxy_addr if self.proxy_addr else None
            ))

        if not parsers:
            raise ValueError(
                "至少需要启用一个视频解析器。"
                "请检查配置中的 parsers 设置。"
            )

        return parsers
