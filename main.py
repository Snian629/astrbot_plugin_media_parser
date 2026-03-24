import asyncio
import json
import os
from typing import Any, Dict, Optional

import aiohttp

from .core.logger import logger

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.core.star.filter.event_message_type import EventMessageType

from .core.parser import ParserManager
from .core.downloader import DownloadManager
from .core.storage import cleanup_files
from .core.constants import Config
from .core.message_adapter.sender import MessageSender
from .core.message_adapter.node_builder import build_all_nodes
from .core.config_manager import ConfigManager
from .core.storage import CacheRegistry
from .core.interaction.platform.bilibili import BilibiliAdminCookieAssistManager


@register(
    "astrbot_plugin_media_parser",
    "drdon1234",
    "聚合解析流媒体平台链接，转换为媒体直链发送",
    "5.1.0"
)
class VideoParserPlugin(Star):

    def __init__(self, context: Context, config: dict):
        """初始化插件"""
        super().__init__(context)
        self.logger = logger
        
        self.config_manager = ConfigManager(config)
        
        parsers = self.config_manager.create_parsers()
        self.parser_manager = ParserManager(parsers)
        self.bilibili_parser = self.config_manager.bilibili_parser
        self.bilibili_auth_runtime = (
            self.bilibili_parser.get_auth_runtime()
            if self.bilibili_parser else
            None
        )
        
        self.download_manager = DownloadManager(
            max_video_size_mb=self.config_manager.max_video_size_mb,
            large_video_threshold_mb=self.config_manager.large_video_threshold_mb,
            cache_dir=self.config_manager.cache_dir,
            pre_download_all_media=self.config_manager.pre_download_all_media,
            max_concurrent_downloads=self.config_manager.max_concurrent_downloads
        )
        
        self.cache_registry = CacheRegistry()
        if self.config_manager.cache_dir:
            label = (
                "media_relay"
                if self.config_manager.use_file_token_service
                else "pre_download"
            )
            self.cache_registry.register(
                self.config_manager.cache_dir, label
            )
        
        self.message_sender = MessageSender()
        self.admin_cookie_assist = BilibiliAdminCookieAssistManager(
            context=self.context,
            admin_id=self.config_manager.admin_id,
            enabled=(
                self.config_manager.bilibili_cookie_runtime_enabled and
                self.config_manager.bilibili_enable_admin_assist_on_expire
            ),
            reply_timeout_minutes=self.config_manager.bilibili_admin_reply_timeout_minutes,
            request_cooldown_minutes=self.config_manager.bilibili_admin_request_cooldown_minutes
        )

    async def terminate(self):
        """插件终止时的清理工作"""
        await self.admin_cookie_assist.shutdown()
        await self.download_manager.shutdown()
        
        if self.download_manager.cache_dir:
            CacheRegistry.cleanup_marked_in(self.download_manager.cache_dir)

    def _trigger_bilibili_cookie_assist_if_needed(self):
        if not self.bilibili_parser:
            return
        reason = self.bilibili_parser.consume_assist_request()
        if not reason:
            return
        self.admin_cookie_assist.trigger_assist_request(reason)

    async def _register_files_with_token_service(
        self,
        metadata: Dict[str, Any]
    ):
        """将已下载的媒体文件注册到AstrBot文件Token服务，获取回调URL。

        无论注册是否成功，都会设置 use_file_token_service 标志，
        确保节点构建时不会回退到 fromFileSystem（临时目录下的文件
        对消息平台不可达）。注册失败时回退到原始直链。
        """
        metadata['use_file_token_service'] = True

        file_paths = metadata.get('file_paths', [])
        if not file_paths or metadata.get('error'):
            return

        try:
            from astrbot.core import file_token_service, astrbot_config
        except ImportError:
            logger.warning(
                "无法导入astrbot.core的file_token_service，"
                "文件Token服务不可用，将回退为直链模式"
            )
            return

        callback_host = self.config_manager.callback_api_base
        if not callback_host:
            callback_host = str(
                astrbot_config.get("callback_api_base") or ""
            ).strip().rstrip("/")
        if not callback_host:
            logger.warning(
                "文件Token服务模式已启用，但未配置回调地址"
                "（插件配置 callback_api_base 或 AstrBot 全局 callback_api_base 均为空），"
                "将回退为直链模式"
            )
            return
        ttl = self.config_manager.file_token_ttl
        file_token_urls = []
        for fp in file_paths:
            if fp and os.path.exists(fp):
                try:
                    token = await file_token_service.register_file(
                        fp, timeout=ttl
                    )
                    url = f"{callback_host}/api/file/{token}"
                    file_token_urls.append(url)
                    logger.debug(f"已注册文件到Token服务: {fp} -> {url}")
                except Exception as e:
                    logger.warning(f"注册文件到Token服务失败: {fp}, 错误: {e}")
                    file_token_urls.append(None)
            else:
                file_token_urls.append(None)

        metadata['file_token_urls'] = file_token_urls

    async def _delayed_cleanup(self, files, delay: int):
        """等待指定秒数后清理媒体文件及其所属的已标记子目录。"""
        try:
            await asyncio.sleep(delay)
            cleanup_files(files)
            cache_dir = self.download_manager.cache_dir
            if cache_dir:
                CacheRegistry.cleanup_marked_in(cache_dir)
            logger.debug(f"延迟清理完成: {len(files)} 个文件")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"延迟清理文件失败: {e}")

    def _check_permission(self, is_private: bool, sender_id: Any, group_id: Any) -> bool:
        """检查用户或群组是否有权限使用解析"""
        admin_id = self.config_manager.admin_id
        sender_id_str = str(sender_id or "").strip()
        group_id_str = "" if is_private else str(group_id or "").strip()

        if admin_id and sender_id_str == admin_id:
            return True

        w_enable = self.config_manager.whitelist_enable
        w_user = self.config_manager.whitelist_user
        w_group = self.config_manager.whitelist_group
        b_enable = self.config_manager.blacklist_enable
        b_user = self.config_manager.blacklist_user
        b_group = self.config_manager.blacklist_group

        allowed = None
        if w_enable and sender_id_str in w_user:
            allowed = True
        elif b_enable and sender_id_str in b_user:
            allowed = False
        elif w_enable and group_id_str and group_id_str in w_group:
            allowed = True
        elif b_enable and group_id_str and group_id_str in b_group:
            allowed = False
            
        if allowed is None:
            allowed = not w_enable

        return allowed
        
    def _extract_url_from_json_card(self, event: AstrMessageEvent) -> Optional[str]:
        """尝试从QQ结构化卡片消息中提取URL"""
        try:
            messages = event.get_messages()
            if not messages:
                return None
            first_msg = messages[0]
            return self._extract_url_from_card_data(first_msg.data)
        except (AttributeError, IndexError, TypeError) as e:
            if self.config_manager.debug_mode:
                self.logger.debug(f"提取JSON卡片链接失败: {e}")
            return None

    def _has_trigger_keyword(self, text: str) -> bool:
        """检查文本中是否包含任一触发关键词。"""
        for keyword in self.config_manager.trigger_keywords:
            if keyword in text:
                return True
        return False

    def _should_parse(self, message_str: str) -> bool:
        """判断是否应该解析消息"""
        if self.config_manager.is_auto_parse:
            return True
        return self._has_trigger_keyword(message_str)

    def _extract_url_from_card_data(self, msg_data) -> Optional[str]:
        """从单个消息段的 data 字段中提取 QQ 结构化卡片 URL。"""
        try:
            curl_link = None
            if isinstance(msg_data, dict) and not msg_data.get('data'):
                meta = msg_data.get("meta") or {}
                detail_1 = meta.get("detail_1") or {}
                curl_link = detail_1.get("qqdocurl")
                if not curl_link:
                    news = meta.get("news") or {}
                    curl_link = news.get("jumpUrl")

            if not curl_link:
                json_str = (
                    msg_data.get('data', '')
                    if isinstance(msg_data, dict) else msg_data
                )
                if json_str and isinstance(json_str, str):
                    message_data = json.loads(json_str)
                    meta = message_data.get("meta") or {}
                    detail_1 = meta.get("detail_1") or {}
                    curl_link = detail_1.get("qqdocurl")
                    if not curl_link:
                        news = meta.get("news") or {}
                        curl_link = news.get("jumpUrl")
            return curl_link
        except (AttributeError, KeyError, json.JSONDecodeError, TypeError):
            return None

    def _try_extract_reply_links(self, event: AstrMessageEvent):
        """从引用消息中提取可解析链接。

        Returns:
            links_with_parser 列表，无结果时为空列表。
        """
        try:
            from astrbot.api.message_components import Reply
        except ImportError:
            return []

        messages = event.get_messages()
        if not messages:
            return []

        reply_comp = None
        for comp in messages:
            if isinstance(comp, Reply):
                reply_comp = comp
                break
        if reply_comp is None:
            return []

        reply_text = reply_comp.message_str or ""
        links = self.parser_manager.extract_all_links(reply_text)
        if links:
            return links

        if reply_comp.chain:
            for comp in reply_comp.chain:
                card_url = self._extract_url_from_card_data(
                    getattr(comp, 'data', None)
                )
                if card_url:
                    links = self.parser_manager.extract_all_links(card_url)
                    if links:
                        return links

        return []

    async def _handle_clean_cache(self, event: AstrMessageEvent):
        """管理员清除全部已注册的媒体缓存目录。"""
        sender_id = str(event.get_sender_id() or "").strip()

        registered = self.cache_registry.get_all()
        if not registered:
            await event.send(event.plain_result("无已注册的缓存目录"))
            return

        try:
            subdirs_cleaned, files_cleaned, skipped = (
                self.cache_registry.cleanup_all()
            )
            parts = [
                f"缓存清理完成: "
                f"{subdirs_cleaned} 个媒体子目录, {files_cleaned} 个文件"
            ]
            if skipped:
                parts.append(
                    f"以下根目录无可清理内容: {', '.join(skipped)}"
                )
            msg = "\n".join(parts)
            await event.send(event.plain_result(msg))
            logger.info(
                f"管理员 {sender_id} 主动清理缓存: "
                f"{subdirs_cleaned} 个子目录, {files_cleaned} 个文件"
            )
        except Exception as e:
            logger.warning(f"管理员清理缓存失败: {e}")
            await event.send(event.plain_result(f"清理失败: {e}"))

    @filter.event_message_type(EventMessageType.ALL)
    async def auto_parse(self, event: AstrMessageEvent):
        """自动解析消息中的视频链接"""
        self.admin_cookie_assist.try_update_admin_origin(event)

        is_private = event.is_private_chat()
        sender_id = event.get_sender_id()
        group_id = None if is_private else event.get_group_id()

        if not self._check_permission(is_private, sender_id, group_id):
            return

        message_text = event.message_str

        clean_kw = self.config_manager.clean_cache_keyword
        if clean_kw and message_text.strip() == clean_kw:
            admin_id = self.config_manager.admin_id
            if (
                is_private
                and admin_id
                and str(sender_id or "").strip() == admin_id
            ):
                await self._handle_clean_cache(event)
            return
        card_url = self._extract_url_from_json_card(event)
        
        if card_url:
            if self.config_manager.debug_mode:
                self.logger.debug(f"[media_parser] 从JSON卡片提取到链接: {card_url}")
            message_text = card_url
        
        links_with_parser = self.parser_manager.extract_all_links(message_text)

        if not links_with_parser:
            if (
                self.config_manager.enable_reply_trigger
                and self._has_trigger_keyword(event.message_str)
            ):
                links_with_parser = self._try_extract_reply_links(event)
                if links_with_parser:
                    if self.config_manager.debug_mode:
                        self.logger.debug(
                            f"通过回复触发解析，提取到 "
                            f"{len(links_with_parser)} 个链接"
                        )
            if not links_with_parser:
                await self.admin_cookie_assist.handle_admin_reply(
                    event,
                    self.bilibili_auth_runtime
                )
                return
        
        if not self._should_parse(message_text):
            return
        
        if self.config_manager.debug_mode:
            self.logger.debug(f"提取到 {len(links_with_parser)} 个可解析链接: {[link for link, _ in links_with_parser]}")
        
        sender_name, sender_id = self.message_sender.get_sender_info(event)
        
        timeout = aiohttp.ClientTimeout(total=Config.DEFAULT_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            metadata_list = await self.parser_manager.parse_text(
                message_text,
                session,
                links_with_parser=links_with_parser
            )
            self._trigger_bilibili_cookie_assist_if_needed()
            if not metadata_list:
                if self.config_manager.debug_mode:
                    self.logger.debug("解析后未获得任何元数据")
                return
            
            has_valid_metadata = any(
                not metadata.get('error') and 
                (
                    bool(metadata.get('video_urls')) or
                    bool(metadata.get('image_urls')) or
                    bool(metadata.get('access_message'))
                )
                for metadata in metadata_list
            )
            
            if not has_valid_metadata:
                if self.config_manager.debug_mode:
                    self.logger.debug("解析后未获得任何有效元数据（可能是直播链接或解析失败）")
                return
            
            if self.config_manager.enable_opening_msg:
                msg_text = self.config_manager.opening_msg_content if self.config_manager.opening_msg_content else "流媒体解析bot为您服务 ٩( 'ω' )و"
                await event.send(event.plain_result(msg_text))
            
            if self.config_manager.debug_mode:
                self.logger.debug(f"解析获得 {len(metadata_list)} 条元数据")
                for idx, metadata in enumerate(metadata_list):
                    self.logger.debug(
                        f"元数据[{idx}]: url={metadata.get('url')}, "
                        f"video_count={len(metadata.get('video_urls', []))}, "
                        f"image_count={len(metadata.get('image_urls', []))}, "
                        f"video_force_download={metadata.get('video_force_download')}"
                    )
            
            async def process_single_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
                """异步处理单条元数据的下载与异常收敛。"""
                if metadata.get('error'):
                    return metadata
                
                try:
                    processed_metadata = await self.download_manager.process_metadata(
                        session,
                        metadata,
                        proxy_addr=self.config_manager.proxy_addr
                    )
                    return processed_metadata
                except Exception as e:
                    self.logger.exception(f"处理元数据失败: {metadata.get('url', '')}, 错误: {e}")
                    metadata['error'] = str(e)
                    return metadata
            
            tasks = [process_single_metadata(metadata) for metadata in metadata_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            processed_metadata_list = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    metadata = metadata_list[i] if i < len(metadata_list) else {}
                    error_msg = str(result)
                    self.logger.exception(
                        f"处理元数据时发生未捕获的异常: {metadata.get('url', '未知URL')}, "
                        f"错误类型: {type(result).__name__}, 错误: {error_msg}"
                    )
                    metadata['error'] = error_msg
                    processed_metadata_list.append(metadata)
                elif isinstance(result, dict):
                    processed_metadata_list.append(result)
                else:
                    metadata = metadata_list[i] if i < len(metadata_list) else {}
                    error_msg = f'未知错误类型: {type(result).__name__}'
                    self.logger.warning(
                        f"处理元数据返回了意外的结果类型: {metadata.get('url', '未知URL')}, "
                        f"类型: {type(result).__name__}"
                    )
                    metadata['error'] = error_msg
                    processed_metadata_list.append(metadata)
            
            if self.config_manager.use_file_token_service:
                for metadata in processed_metadata_list:
                    await self._register_files_with_token_service(metadata)

            temp_files = []
            video_files = []
            try:
                all_link_nodes, link_metadata, temp_files, video_files = build_all_nodes(
                    processed_metadata_list,
                    self.config_manager.is_auto_pack,
                    self.config_manager.large_video_threshold_mb,
                    self.config_manager.max_video_size_mb,
                    self.config_manager.enable_text_metadata
                )
                
                if self.config_manager.debug_mode:
                    self.logger.debug(
                        f"节点构建完成: {len(all_link_nodes)} 个链接节点, "
                        f"{len(temp_files)} 个临时文件, {len(video_files)} 个视频文件"
                    )
                
                if not all_link_nodes:
                    if self.config_manager.debug_mode:
                        self.logger.debug("未构建任何节点，跳过发送")
                    return
                
                if self.config_manager.debug_mode:
                    self.logger.debug(f"开始发送结果，打包模式: {self.config_manager.is_auto_pack}")
                
                if self.config_manager.is_auto_pack:
                    await self.message_sender.send_packed_results(
                        event,
                        link_metadata,
                        sender_name,
                        sender_id,
                        self.config_manager.large_video_threshold_mb
                    )
                else:
                    await self.message_sender.send_unpacked_results(
                        event,
                        all_link_nodes,
                        link_metadata
                    )

                if self.config_manager.debug_mode:
                    self.logger.debug("发送完成")
            except Exception as e:
                self.logger.exception(
                    f"构建节点或发送消息失败: {e}, "
                    f"临时文件数: {len(temp_files)}, 视频文件数: {len(video_files)}"
                )
                raise
            finally:
                all_files = temp_files + video_files
                if self.config_manager.use_file_token_service and all_files:
                    delay = self.config_manager.file_token_ttl
                    if self.config_manager.debug_mode:
                        self.logger.debug(
                            f"文件Token服务模式下延迟 {delay}s 后清理 "
                            f"{len(all_files)} 个文件"
                        )
                    asyncio.create_task(
                        self._delayed_cleanup(all_files, delay)
                    )
                elif all_files:
                    cleanup_files(all_files)
                    if self.config_manager.debug_mode:
                        self.logger.debug(f"已清理临时文件: {len(temp_files)} 个, 视频文件: {len(video_files)} 个")
