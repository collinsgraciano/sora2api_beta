import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import sys

# Target server (Sora2API)
TARGET_URL = "http://127.0.0.1:8000"

app = FastAPI(title="Sora2API Local Proxy")

# Configure CORS to allow any origin (e.g. Chrome Extensions)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(request: Request, path: str):
    """
    Forward all requests to the target server
    """
    # Build complete URL
    url = f"{TARGET_URL}/{path}"
    query = request.url.query
    if query:
        url += f"?{query}"
    
    print(f"Proxying: {request.method} {path} -> {url}")

    async with httpx.AsyncClient() as client:
        # Prepare headers (filter out host to let client set it)
        headers = dict(request.headers)
        headers.pop("host", None)
        headers.pop("content-length", None) # Let httpx handle this
        
        try:
            # Read body
            body = await request.body()
            
            # Forward request
            response = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body,
                timeout=30.0
            )
            
            # Forward response
            # Filter headers that might cause issues
            res_headers = dict(response.headers)
            res_headers.pop("content-length", None)
            res_headers.pop("content-encoding", None) # Let fastapi/uvicorn handle this

            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=res_headers
            )
            
        except httpx.RequestError as exc:
            print(f"Request error: {exc}")
            return Response(content=f"Proxy Error: {str(exc)}", status_code=502)
        except Exception as exc:
            print(f"Error: {exc}")
            return Response(content=f"Internal Proxy Error: {str(exc)}", status_code=500)

if __name__ == "__main__":
    print(f"🚀 Local Proxy Server running on http://127.0.0.1:8003")
    print(f"👉 Forwarding to {TARGET_URL}")
    print("Press CTRL+C to stop")
    uvicorn.run(app, host="127.0.0.1", port=8003)
