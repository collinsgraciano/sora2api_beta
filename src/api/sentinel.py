from fastapi import APIRouter, Depends, Query, HTTPException
from ..core.auth import verify_api_key_header
from ..services.sentinel import refresh_token_async
import json

router = APIRouter(tags=["Sentinel"])

@router.get("/v1/sentinel/token")
async def get_sentinel_token(
    flow: str = Query("sora_create_task", description="Flow type for the token"),
    api_key: str = Depends(verify_api_key_header)
):
    """
    Get an OpenAI Sentinel Token (PoW + Turnstile + Token).
    Returns the string value that should be used in 'OpenAI-Sentinel-Token' header.
    """
    try:
        token_payload = await refresh_token_async(flow)
        
        # token_payload is a JSON string. We want to check if it contains error.
        try:
            data = json.loads(token_payload)
            if "e" in data:
                 raise HTTPException(status_code=500, detail=f"Sentinel generation failed: {data['e']}")
        except json.JSONDecodeError:
            pass # Should be valid json if successful
            
        return {
            "object": "sentinel_token", 
            "token": token_payload,
            "usage": "Use the 'token' value in 'OpenAI-Sentinel-Token' header"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
