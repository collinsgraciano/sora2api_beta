"""Load balancing module"""
import asyncio
import random
from typing import Optional, Dict
from ..core.models import Token
from ..core.config import config
from .token_manager import TokenManager
from .token_lock import TokenLock
from .concurrency_manager import ConcurrencyManager
from ..core.logger import debug_logger

class LoadBalancer:
    """Token load balancer with random selection and image generation lock"""

    def __init__(self, token_manager: TokenManager, concurrency_manager: Optional[ConcurrencyManager] = None):
        self.token_manager = token_manager
        self.concurrency_manager = concurrency_manager
        # Use image timeout from config as lock timeout
        self.token_lock = TokenLock(lock_timeout=config.image_timeout)
        # 轮询模式锁 - 只保护内存中的选择操作（极快）
        self._round_robin_lock = asyncio.Lock()
        # 内存中的 usage_count 缓存，避免每次从数据库读取
        self._usage_cache: Dict[int, int] = {}

    def _get_cached_usage(self, token_id: int, db_usage: int) -> int:
        """获取缓存中的 usage_count，如果不存在则用数据库值初始化"""
        if token_id not in self._usage_cache:
            self._usage_cache[token_id] = db_usage
        return self._usage_cache[token_id]

    async def sync_usage_cache_from_db(self):
        """从数据库同步 usage_count 缓存（用于启动时或重置后）"""
        try:
            all_tokens = await self.token_manager.get_all_tokens()
            for token in all_tokens:
                self._usage_cache[token.id] = token.usage_count or 0
            debug_logger.log_info(f"[LOAD_BALANCER] ✅ 已同步 {len(all_tokens)} 个 Token 的 usage_count 缓存")
        except Exception as e:
            debug_logger.log_error(f"[LOAD_BALANCER] 同步 usage_count 缓存失败: {e}")

    async def reset_usage_cache(self):
        """重置内存缓存（与数据库重置同步）"""
        self._usage_cache.clear()
        debug_logger.log_info("[LOAD_BALANCER] 🔄 已清空 usage_count 内存缓存")

    async def _select_round_robin(self, available_tokens: list, for_image: bool = False, for_video: bool = False) -> Optional[Token]:
        """
        轮询模式选择 token - 高性能版本
        
        使用内存缓存的 usage_count，锁只保护"选择+计数增加"这个纯内存操作（微秒级）
        不会阻塞并发请求
        
        Args:
            available_tokens: 可用的 token 列表
            for_image: 是否用于图片生成
            for_video: 是否用于视频生成
            
        Returns:
            选中的 token，或 None
        """
        if not available_tokens:
            return None
        
        # 锁内只做纯内存操作，极快（微秒级）
        async with self._round_robin_lock:
            # 使用内存缓存获取 usage_count
            token_with_usage = []
            for token in available_tokens:
                cached_usage = self._get_cached_usage(token.id, token.usage_count or 0)
                token_with_usage.append((token, cached_usage))
            
            # 按 usage_count 升序排序
            token_with_usage.sort(key=lambda x: x[1])
            selected_token, current_usage = token_with_usage[0]
            
            # 立即在内存中增加计数（这是并发安全的关键）
            self._usage_cache[selected_token.id] = current_usage + 1
        
        # 锁外异步更新数据库（fire-and-forget，不阻塞）
        asyncio.create_task(self._async_increment_db_usage(selected_token.id))
        
        debug_logger.log_info(f"[LOAD_BALANCER] 🔄 轮询模式: 选中 Token {selected_token.id} ({selected_token.email}), usage_count: {current_usage} -> {current_usage + 1}")
        
        return selected_token

    async def _async_increment_db_usage(self, token_id: int):
        """异步更新数据库中的 usage_count（不阻塞主流程）"""
        try:
            await self.token_manager.increment_usage_count(token_id)
        except Exception as e:
            debug_logger.log_error(f"[LOAD_BALANCER] 异步更新 usage_count 失败: {e}")

    async def select_token(self, for_image_generation: bool = False, for_video_generation: bool = False, require_pro: bool = False) -> Optional[Token]:
        """
        Select a token using random load balancing

        Args:
            for_image_generation: If True, only select tokens that are not locked for image generation and have image_enabled=True
            for_video_generation: If True, filter out tokens with Sora2 quota exhausted (sora2_cooldown_until not expired), tokens that don't support Sora2, and tokens with video_enabled=False
            require_pro: If True, only select tokens with ChatGPT Pro subscription (plan_type="chatgpt_pro")

        Returns:
            Selected token or None if no available tokens
        """
        # Try to auto-refresh tokens expiring within 24 hours if enabled
        if config.at_auto_refresh_enabled:
            debug_logger.log_info(f"[LOAD_BALANCER] 🔄 自动刷新功能已启用，开始检查Token过期时间...")
            all_tokens = await self.token_manager.get_all_tokens()
            debug_logger.log_info(f"[LOAD_BALANCER] 📊 总Token数: {len(all_tokens)}")

            refresh_count = 0
            for token in all_tokens:
                if token.is_active and token.expiry_time:
                    from datetime import datetime
                    time_until_expiry = token.expiry_time - datetime.now()
                    hours_until_expiry = time_until_expiry.total_seconds() / 3600
                    # Refresh if expiry is within 24 hours
                    if hours_until_expiry <= 24:
                        debug_logger.log_info(f"[LOAD_BALANCER] 🔔 Token {token.id} ({token.email}) 需要刷新，剩余时间: {hours_until_expiry:.2f} 小时")
                        refresh_count += 1
                        await self.token_manager.auto_refresh_expiring_token(token.id)

            if refresh_count == 0:
                debug_logger.log_info(f"[LOAD_BALANCER] ✅ 所有Token都无需刷新")
            else:
                debug_logger.log_info(f"[LOAD_BALANCER] ✅ 刷新检查完成，共检查 {refresh_count} 个Token")

        active_tokens = await self.token_manager.get_active_tokens()

        if not active_tokens:
            return None

        # Filter for Pro tokens if required
        if require_pro:
            pro_tokens = [token for token in active_tokens if token.plan_type == "chatgpt_pro"]
            if not pro_tokens:
                return None
            active_tokens = pro_tokens

        # If for video generation, filter out tokens with Sora2 quota exhausted and tokens without Sora2 support
        if for_video_generation:
            from datetime import datetime
            available_tokens = []
            for token in active_tokens:
                # Skip tokens that don't have video enabled
                if not token.video_enabled:
                    continue

                # Skip tokens that don't support Sora2
                if not token.sora2_supported:
                    continue

                # Check if Sora2 cooldown has expired and refresh if needed
                if token.sora2_cooldown_until and token.sora2_cooldown_until <= datetime.now():
                    await self.token_manager.refresh_sora2_remaining_if_cooldown_expired(token.id)
                    # Reload token data after refresh
                    token = await self.token_manager.db.get_token(token.id)

                # Skip tokens that are in Sora2 cooldown (quota exhausted)
                if token and token.sora2_cooldown_until and token.sora2_cooldown_until > datetime.now():
                    continue

                if token:
                    available_tokens.append(token)

            if not available_tokens:
                return None

            active_tokens = available_tokens

        # If for image generation, filter out locked tokens and tokens without image enabled
        if for_image_generation:
            available_tokens = []
            for token in active_tokens:
                # Skip tokens that don't have image enabled
                if not token.image_enabled:
                    continue

                if not await self.token_lock.is_locked(token.id):
                    # Check concurrency limit if concurrency manager is available
                    if self.concurrency_manager and not await self.concurrency_manager.can_use_image(token.id):
                        continue
                    available_tokens.append(token)

            if not available_tokens:
                return None

            # Determine selection strategy based on admin config
            scheduling_mode = await self.token_manager.get_scheduling_mode()
            
            if scheduling_mode == "round_robin":
                # 使用互斥锁保护的轮询选择
                return await self._select_round_robin(available_tokens, for_image=True)
            else:
                return random.choice(available_tokens)
        else:
            # For video generation, check concurrency limit
            if for_video_generation and self.concurrency_manager:
                available_tokens = []
                for token in active_tokens:
                    if await self.concurrency_manager.can_use_video(token.id):
                        available_tokens.append(token)
                if not available_tokens:
                    return None
                
                # Determine selection strategy based on admin config
                scheduling_mode = await self.token_manager.get_scheduling_mode()
                if scheduling_mode == "round_robin":
                    # 使用互斥锁保护的轮询选择
                    return await self._select_round_robin(available_tokens, for_video=True)
                else:
                    return random.choice(available_tokens)
            else:
                # For video generation without concurrency manager
                # Also apply scheduling mode
                scheduling_mode = await self.token_manager.get_scheduling_mode()
                if scheduling_mode == "round_robin":
                    # 使用互斥锁保护的轮询选择
                    return await self._select_round_robin(active_tokens, for_video=for_video_generation)
                else:
                    return random.choice(active_tokens)

