"""Sora API client module"""
import base64
import io
import json
import time
import random
import string
import re
from typing import Optional, Dict, Any, Tuple
from curl_cffi.requests import AsyncSession
from curl_cffi import CurlMime
from .proxy_manager import ProxyManager
from ..core.config import config
from ..core.logger import debug_logger
from .sentinel import refresh_token_async

class SoraClient:
    """Sora API client with proxy support"""

    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.base_url = config.sora_base_url
        self.timeout = config.sora_timeout

    async def _generate_sentinel_token(self, token_id: Optional[int] = None) -> str:
        """
        生成 openai-sentinel-token
        通过集成服务获取 PoW token，使用指定代理
        """
        try:
            proxy_url = await self.proxy_manager.get_proxy_url(token_id)
            return await refresh_token_async(proxy=proxy_url)
        except Exception as e:
            debug_logger.log_error(f"Sentinel token generation failed: {str(e)}")
            # Fallback to random string if service fails to ensure request makes it out (though likely to be rejected)
            length = random.randint(10, 20)
            return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def is_storyboard_prompt(prompt: str) -> bool:
        """检测提示词是否为分镜模式格式

        格式: [time]prompt 或 [time]prompt\n[time]prompt
        例如: [5.0s]猫猫从飞机上跳伞 [5.0s]猫猫降落

        Args:
            prompt: 用户输入的提示词

        Returns:
            True if prompt matches storyboard format
        """
        if not prompt:
            return False
        # 匹配格式: [数字s] 或 [数字.数字s]
        pattern = r'\[\d+(?:\.\d+)?s\]'
        matches = re.findall(pattern, prompt)
        # 至少包含一个时间标记才认为是分镜模式
        return len(matches) >= 1

    @staticmethod
    def format_storyboard_prompt(prompt: str) -> str:
        """将分镜格式提示词转换为API所需格式

        输入: 猫猫的奇妙冒险\n[5.0s]猫猫从飞机上跳伞 [5.0s]猫猫降落
        输出: current timeline:
Shot 1:...

instructions:
猫猫的奇妙冒险

        Args:
            prompt: 原始分镜格式提示词

        Returns:
            格式化后的API提示词
        """
        # 匹配 [时间]内容 的模式
        pattern = r'\[(\d+(?:\.\d+)?)s\]\s*([^\[]+)'
        matches = re.findall(pattern, prompt)

        if not matches:
            return prompt

        # 提取总述(第一个[时间]之前的内容)
        first_bracket_pos = prompt.find('[')
        instructions = ""
        if first_bracket_pos > 0:
            instructions = prompt[:first_bracket_pos].strip()

        # 格式化分镜
        formatted_shots = []
        for idx, (duration, scene) in enumerate(matches, 1):
            scene = scene.strip()
            shot = f"Shot {idx}:\nduration: {duration}sec\nScene: {scene}"
            formatted_shots.append(shot)

        timeline = "\n\n".join(formatted_shots)

        # 如果有总述,添加instructions部分
        if instructions:
            return f"current timeline:\n{timeline}\n\ninstructions:\n{instructions}"
        else:
            return timeline

    async def _make_request(self, method: str, endpoint: str, token: str,
                           json_data: Optional[Dict] = None,
                           multipart: Optional[Dict] = None,
                           add_sentinel_token: bool = False,
                           token_id: Optional[int] = None) -> Dict[str, Any]:
        """Make HTTP request with proxy support

        Args:
            method: HTTP method (GET/POST)
            endpoint: API endpoint
            token: Access token
            json_data: JSON request body
            multipart: Multipart form data (for file uploads)
            add_sentinel_token: Whether to add openai-sentinel-token header (only for generation requests)
            token_id: Token ID for getting token-specific proxy (optional)
        """
        proxy_url = await self.proxy_manager.get_proxy_url(token_id)

        headers = {
            "Authorization": f"Bearer {token}"
        }

        # 只在生成请求时添加 sentinel token
        if add_sentinel_token:
            headers["openai-sentinel-token"] = await self._generate_sentinel_token(token_id=token_id)

        if not multipart:
            headers["Content-Type"] = "application/json"

        async with AsyncSession() as session:
            url = f"{self.base_url}{endpoint}"

            kwargs = {
                "headers": headers,
                "timeout": self.timeout,
                "impersonate": config.impersonate_preset  # 自动生成 User-Agent 和浏览器指纹
            }

            if proxy_url:
                kwargs["proxy"] = proxy_url

            if json_data:
                kwargs["json"] = json_data

            if multipart:
                kwargs["multipart"] = multipart

            # Log request
            debug_logger.log_request(
                method=method,
                url=url,
                headers=headers,
                body=json_data,
                files=multipart,
                proxy=proxy_url
            )

            # Record start time
            start_time = time.time()

            # Make request
            if method == "GET":
                response = await session.get(url, **kwargs)
            elif method == "POST":
                response = await session.post(url, **kwargs)
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000

            # Parse response
            try:
                response_json = response.json()
            except:
                response_json = None

            # Log response
            debug_logger.log_response(
                status_code=response.status_code,
                headers=dict(response.headers),
                body=response_json if response_json else response.text,
                duration_ms=duration_ms
            )

            # Check status
            if response.status_code not in [200, 201]:
                # Parse error response
                error_data = None
                try:
                    error_data = response.json()
                except:
                    pass

                # Check for unsupported_country_code error
                if error_data and isinstance(error_data, dict):
                    error_info = error_data.get("error", {})
                    if error_info.get("code") == "unsupported_country_code":
                        # Create structured error with full error data
                        error_msg = json.dumps(error_data)
                        debug_logger.log_error(
                            error_message=f"Unsupported country: {error_msg}",
                            status_code=response.status_code,
                            response_text=error_msg
                        )
                        # Raise exception with structured error data
                        raise Exception(error_msg)

                # Generic error handling
                error_msg = f"API request failed: {response.status_code} - {response.text}"
                debug_logger.log_error(
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_text=response.text
                )
                raise Exception(error_msg)

            return response_json if response_json else response.text
    
    async def get_user_info(self, token: str) -> Dict[str, Any]:
        """Get user information"""
        return await self._make_request("GET", "/me", token)
    
    async def upload_image(self, image_data: bytes, token: str, filename: str = "image.png", token_id: Optional[int] = None) -> str:
        """Upload image and return media_id

        使用 CurlMime 对象上传文件（curl_cffi 的正确方式）
        参考：https://curl-cffi.readthedocs.io/en/latest/quick_start.html#uploads
        """
        # 检测图片类型
        mime_type = "image/png"
        if filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg'):
            mime_type = "image/jpeg"
        elif filename.lower().endswith('.webp'):
            mime_type = "image/webp"

        # 创建 CurlMime 对象
        mp = CurlMime()

        # 添加文件部分
        mp.addpart(
            name="file",
            content_type=mime_type,
            filename=filename,
            data=image_data
        )

        # 添加文件名字段
        mp.addpart(
            name="file_name",
            data=filename.encode('utf-8')
        )

        result = await self._make_request("POST", "/uploads", token, multipart=mp, token_id=token_id)
        return result["id"]
    
    async def generate_image(self, prompt: str, token: str, width: int = 360,
                            height: int = 360, media_id: Optional[str] = None, token_id: Optional[int] = None) -> str:
        """Generate image (text-to-image or image-to-image)"""
        operation = "remix" if media_id else "simple_compose"

        inpaint_items = []
        if media_id:
            inpaint_items = [{
                "type": "image",
                "frame_index": 0,
                "upload_media_id": media_id
            }]

        json_data = {
            "type": "image_gen",
            "operation": operation,
            "prompt": prompt,
            "width": width,
            "height": height,
            "n_variants": 1,
            "n_frames": 1,
            "inpaint_items": inpaint_items
        }

        # 生成请求需要添加 sentinel token
        result = await self._make_request("POST", "/video_gen", token, json_data=json_data, add_sentinel_token=True, token_id=token_id)
        return result["id"]
    
    async def generate_video(self, prompt: str, token: str, orientation: str = "landscape",
                            media_id: Optional[str] = None, n_frames: int = 450, style_id: Optional[str] = None,
                            model: str = "sy_8", size: str = "small", token_id: Optional[int] = None) -> str:
        """Generate video (text-to-video or image-to-video)

        Args:
            prompt: Video generation prompt
            token: Access token
            orientation: Video orientation (landscape/portrait)
            media_id: Optional image media_id for image-to-video
            n_frames: Number of frames (300/450/750)
            style_id: Optional style ID
            model: Model to use (sy_8 for standard, sy_ore for pro)
            size: Video size (small for standard, large for HD)
            token_id: Token ID for getting token-specific proxy (optional)
        """
        inpaint_items = []
        if media_id:
            inpaint_items = [{
                "kind": "upload",
                "upload_id": media_id
            }]

        json_data = {
            "kind": "video",
            "prompt": prompt,
            "orientation": orientation,
            "size": size,
            "n_frames": n_frames,
            "model": model,
            "inpaint_items": inpaint_items,
            "style_id": style_id
        }

        # 生成请求需要添加 sentinel token
        result = await self._make_request("POST", "/nf/create", token, json_data=json_data, add_sentinel_token=True, token_id=token_id)
        return result["id"]
    
    async def get_image_tasks(self, token: str, limit: int = 20, token_id: Optional[int] = None) -> Dict[str, Any]:
        """Get recent image generation tasks"""
        return await self._make_request("GET", f"/v2/recent_tasks?limit={limit}", token, token_id=token_id)

    async def get_video_drafts(self, token: str, limit: int = 15, token_id: Optional[int] = None) -> Dict[str, Any]:
        """Get recent video drafts"""
        return await self._make_request("GET", f"/project_y/profile/drafts?limit={limit}", token, token_id=token_id)

    async def get_pending_tasks(self, token: str, token_id: Optional[int] = None) -> list:
        """Get pending video generation tasks

        Returns:
            List of pending tasks with progress information
        """
        result = await self._make_request("GET", "/nf/pending/v2", token, token_id=token_id)
        # The API returns a list directly
        return result if isinstance(result, list) else []

    async def post_video_for_watermark_free(self, generation_id: str, prompt: str, token: str, token_id: Optional[int] = None) -> str:
        """Post video to get watermark-free version

        Args:
            generation_id: The generation ID (e.g., gen_01k9btrqrnen792yvt703dp0tq)
            prompt: The original generation prompt
            token: Access token
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            Post ID (e.g., s_690ce161c2488191a3476e9969911522)
        """
        json_data = {
            "attachments_to_create": [
                {
                    "generation_id": generation_id,
                    "kind": "sora"
                }
            ],
            "post_text": ""
        }

        # 发布请求需要添加 sentinel token
        result = await self._make_request("POST", "/project_y/post", token, json_data=json_data, add_sentinel_token=True, token_id=token_id)

        # 返回 post.id
        return result.get("post", {}).get("id", "")

    async def delete_post(self, post_id: str, token: str, token_id: Optional[int] = None) -> bool:
        """Delete a published post

        Args:
            post_id: The post ID (e.g., s_690ce161c2488191a3476e9969911522)
            token: Access token
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            True if deletion was successful
        """
        proxy_url = await self.proxy_manager.get_proxy_url(token_id)

        headers = {
            "Authorization": f"Bearer {token}"
        }

        async with AsyncSession() as session:
            url = f"{self.base_url}/project_y/post/{post_id}"

            kwargs = {
                "headers": headers,
                "timeout": self.timeout,
                "impersonate": config.impersonate_preset
            }

            if proxy_url:
                kwargs["proxy"] = proxy_url

            # Log request
            debug_logger.log_request(
                method="DELETE",
                url=url,
                headers=headers,
                body=None,
                files=None,
                proxy=proxy_url
            )

            # Record start time
            start_time = time.time()

            # Make DELETE request
            response = await session.delete(url, **kwargs)

            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000

            # Log response
            debug_logger.log_response(
                status_code=response.status_code,
                headers=dict(response.headers),
                body=response.text if response.text else "No content",
                duration_ms=duration_ms
            )

            # Check status (DELETE typically returns 204 No Content or 200 OK)
            if response.status_code not in [200, 204]:
                error_msg = f"Delete post failed: {response.status_code} - {response.text}"
                debug_logger.log_error(
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_text=response.text
                )
                raise Exception(error_msg)

            return True

    async def get_watermark_free_url_custom(self, parse_url: str, parse_token: str, post_id: str, token_id: Optional[int] = None) -> str:
        """Get watermark-free video URL from custom parse server

        Args:
            parse_url: Custom parse server URL (e.g., http://example.com)
            parse_token: Access token for custom parse server
            post_id: Post ID to parse (e.g., s_690c0f574c3881918c3bc5b682a7e9fd)
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            Download link from custom parse server

        Raises:
            Exception: If parse fails or token is invalid
        """
        proxy_url = await self.proxy_manager.get_proxy_url(token_id)

        # Construct the share URL
        share_url = f"https://sora.chatgpt.com/p/{post_id}"

        # Prepare request
        json_data = {
            "url": share_url,
            "token": parse_token
        }

        kwargs = {
            "json": json_data,
            "timeout": 30,
            "impersonate": config.impersonate_preset
        }

        if proxy_url:
            kwargs["proxy"] = proxy_url

        try:
            async with AsyncSession() as session:
                # Record start time
                start_time = time.time()

                # Make POST request to custom parse server
                response = await session.post(f"{parse_url}/get-sora-link", **kwargs)

                # Calculate duration
                duration_ms = (time.time() - start_time) * 1000

                # Log response
                debug_logger.log_response(
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    body=response.text if response.text else "No content",
                    duration_ms=duration_ms
                )

                # Check status
                if response.status_code != 200:
                    error_msg = f"Custom parse failed: {response.status_code} - {response.text}"
                    debug_logger.log_error(
                        error_message=error_msg,
                        status_code=response.status_code,
                        response_text=response.text
                    )
                    raise Exception(error_msg)

                # Parse response
                result = response.json()

                # Check for error in response
                if "error" in result:
                    error_msg = f"Custom parse error: {result['error']}"
                    debug_logger.log_error(
                        error_message=error_msg,
                        status_code=401,
                        response_text=str(result)
                    )
                    raise Exception(error_msg)

                # Extract download link
                download_link = result.get("download_link")
                if not download_link:
                    raise Exception("No download_link in custom parse response")

                debug_logger.log_info(f"Custom parse successful: {download_link}")
                return download_link

        except Exception as e:
            debug_logger.log_error(
                error_message=f"Custom parse request failed: {str(e)}",
                status_code=500,
                response_text=str(e)
            )
            raise

    # ==================== Character Creation Methods ====================

    async def upload_character_video(self, video_data: bytes, token: str, token_id: Optional[int] = None) -> str:
        """Upload character video and return cameo_id

        Args:
            video_data: Video file bytes
            token: Access token

        Returns:
            cameo_id
        """
        mp = CurlMime()
        mp.addpart(
            name="file",
            content_type="video/mp4",
            filename="video.mp4",
            data=video_data
        )
        mp.addpart(
            name="timestamps",
            data=b"0,3"
        )

        result = await self._make_request("POST", "/characters/upload", token, multipart=mp, token_id=token_id)
        return result.get("id")

    async def get_cameo_status(self, cameo_id: str, token: str, token_id: Optional[int] = None) -> Dict[str, Any]:
        """Get character (cameo) processing status

        Args:
            cameo_id: The cameo ID returned from upload_character_video
            token: Access token
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            Dictionary with status, display_name_hint, username_hint, profile_asset_url, instruction_set_hint
        """
        return await self._make_request("GET", f"/project_y/cameos/in_progress/{cameo_id}", token, token_id=token_id)

    async def download_character_image(self, image_url: str, token_id: Optional[int] = None) -> bytes:
        """Download character image from URL

        Args:
            image_url: The profile_asset_url from cameo status
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            Image file bytes
        """
        proxy_url = await self.proxy_manager.get_proxy_url(token_id)

        kwargs = {
            "timeout": self.timeout,
            "impersonate": config.impersonate_preset
        }

        if proxy_url:
            kwargs["proxy"] = proxy_url

        async with AsyncSession() as session:
            response = await session.get(image_url, **kwargs)
            if response.status_code != 200:
                raise Exception(f"Failed to download image: {response.status_code}")
            return response.content

    async def finalize_character(self, cameo_id: str, username: str, display_name: str,
                                profile_asset_pointer: str, instruction_set, token: str, token_id: Optional[int] = None) -> str:
        """Finalize character creation

        Args:
            cameo_id: The cameo ID
            username: Character username
            display_name: Character display name
            profile_asset_pointer: Asset pointer from upload_character_image
            instruction_set: Character instruction set (not used by API, always set to None)
            token: Access token

        Returns:
            character_id
        """
        # Note: API always expects instruction_set to be null
        # The instruction_set parameter is kept for backward compatibility but not used
        _ = instruction_set  # Suppress unused parameter warning
        json_data = {
            "cameo_id": cameo_id,
            "username": username,
            "display_name": display_name,
            "profile_asset_pointer": profile_asset_pointer,
            "instruction_set": None,
            "safety_instruction_set": None
        }

        result = await self._make_request("POST", "/characters/finalize", token, json_data=json_data, token_id=token_id)
        return result.get("character", {}).get("character_id")

    async def set_character_public(self, cameo_id: str, token: str, token_id: Optional[int] = None) -> bool:
        """Set character as public

        Args:
            cameo_id: The cameo ID
            token: Access token
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            True if successful
        """
        json_data = {"visibility": "public"}
        await self._make_request("POST", f"/project_y/cameos/by_id/{cameo_id}/update_v2", token, json_data=json_data, token_id=token_id)
        return True

    async def upload_character_image(self, image_data: bytes, token: str, token_id: Optional[int] = None) -> str:
        """Upload character image and return asset_pointer

        Args:
            image_data: Image file bytes
            token: Access token

        Returns:
            asset_pointer
        """
        mp = CurlMime()
        mp.addpart(
            name="file",
            content_type="image/webp",
            filename="profile.webp",
            data=image_data
        )
        mp.addpart(
            name="use_case",
            data=b"profile"
        )

        result = await self._make_request("POST", "/project_y/file/upload", token, multipart=mp, token_id=token_id)
        return result.get("asset_pointer")

    async def delete_character(self, character_id: str, token: str, token_id: Optional[int] = None) -> bool:
        """Delete a character

        Args:
            character_id: The character ID
            token: Access token

        Returns:
            True if successful
        """
        proxy_url = await self.proxy_manager.get_proxy_url(token_id)

        headers = {
            "Authorization": f"Bearer {token}"
        }

        async with AsyncSession() as session:
            url = f"{self.base_url}/project_y/characters/{character_id}"

            kwargs = {
                "headers": headers,
                "timeout": self.timeout,
                "impersonate": config.impersonate_preset
            }

            if proxy_url:
                kwargs["proxy"] = proxy_url

            response = await session.delete(url, **kwargs)
            if response.status_code not in [200, 204]:
                raise Exception(f"Failed to delete character: {response.status_code}")
            return True

    async def remix_video(self, remix_target_id: str, prompt: str, token: str,
                         orientation: str = "portrait", n_frames: int = 450, style_id: Optional[str] = None) -> str:
        """Generate video using remix (based on existing video)

        Args:
            remix_target_id: The video ID from Sora share link (e.g., s_690d100857248191b679e6de12db840e)
            prompt: Generation prompt
            token: Access token
            orientation: Video orientation (portrait/landscape)
            n_frames: Number of frames
            style_id: Optional style ID

        Returns:
            task_id
        """
        json_data = {
            "kind": "video",
            "prompt": prompt,
            "inpaint_items": [],
            "remix_target_id": remix_target_id,
            "cameo_ids": [],
            "cameo_replacements": {},
            "model": "sy_8",
            "orientation": orientation,
            "n_frames": n_frames,
            "style_id": style_id
        }

        result = await self._make_request("POST", "/nf/create", token, json_data=json_data, add_sentinel_token=True)
        return result.get("id")

    async def generate_storyboard(self, prompt: str, token: str, orientation: str = "landscape",
                                 media_id: Optional[str] = None, n_frames: int = 450, style_id: Optional[str] = None, token_id: Optional[int] = None) -> str:
        """Generate video using storyboard mode

        Args:
            prompt: Formatted storyboard prompt (Shot 1:\nduration: 5.0sec\nScene: ...)
            token: Access token
            orientation: Video orientation (portrait/landscape)
            media_id: Optional image media_id for image-to-video
            n_frames: Number of frames
            style_id: Optional style ID
            token_id: Token ID for getting token-specific proxy (optional)

        Returns:
            task_id
        """
        inpaint_items = []
        if media_id:
            inpaint_items = [{
                "kind": "upload",
                "upload_id": media_id
            }]

        json_data = {
            "kind": "video",
            "prompt": prompt,
            "title": "Draft your video",
            "orientation": orientation,
            "size": "small",
            "n_frames": n_frames,
            "storyboard_id": None,
            "inpaint_items": inpaint_items,
            "remix_target_id": None,
            "model": "sy_8",
            "metadata": None,
            "style_id": style_id,
            "cameo_ids": None,
            "cameo_replacements": None,
            "audio_caption": None,
            "audio_transcript": None,
            "video_caption": None
        }

        result = await self._make_request("POST", "/nf/create/storyboard", token, json_data=json_data, add_sentinel_token=True, token_id=token_id)
        return result.get("id")
