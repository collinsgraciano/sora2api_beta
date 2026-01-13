"""Load balancing module"""
import random
from typing import Optional
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
            
            selected_token = None
            if scheduling_mode == "round_robin":
                # Sort by usage_count ASC, then random for tie-breaking? Or ID for stability
                # For round-robin, we want strictly the one with least usage
                available_tokens.sort(key=lambda t: t.usage_count if t.usage_count is not None else 0)
                selected_token = available_tokens[0]
                
                # Increment usage count immediately to mark it as "used" in this round
                # This ensures next request won't pick it (assuming DB update is fast)
                # Note: This is fire-and-forget to avoid blocking too much, 
                # but await is safer to ensure consistency
                await self.token_manager.increment_usage_count(selected_token.id)
            else:
                selected_token = random.choice(available_tokens)
                
            return selected_token
        else:
            # For video generation, check concurrency limit
            if for_video_generation and self.concurrency_manager:
                available_tokens = []
                for token in active_tokens:
                    if await self.concurrency_manager.can_use_video(token.id):
                        available_tokens.append(token)
                if not available_tokens:
                    return None
                
                # Consolidate logic for video generation too
                scheduling_mode = await self.token_manager.get_scheduling_mode()
                if scheduling_mode == "round_robin":
                    available_tokens.sort(key=lambda t: t.usage_count if t.usage_count is not None else 0)
                    selected_token = available_tokens[0]
                    debug_logger.log_info(f"[LOAD_BALANCER] Selected token {selected_token.id} (usage: {selected_token.usage_count})")
                    await self.token_manager.increment_usage_count(selected_token.id)
                    return selected_token
                else:
                    return random.choice(available_tokens)
            else:
                # For video generation without concurrency manager
                # Also apply scheduling mode
                scheduling_mode = await self.token_manager.get_scheduling_mode()
                if scheduling_mode == "round_robin":
                    active_tokens.sort(key=lambda t: t.usage_count if t.usage_count is not None else 0)
                    selected_token = active_tokens[0]
                    debug_logger.log_info(f"[LOAD_BALANCER] Selected token {selected_token.id} (usage: {selected_token.usage_count})")
                    await self.token_manager.increment_usage_count(selected_token.id)
                    return selected_token
                else:
                    return random.choice(active_tokens)
