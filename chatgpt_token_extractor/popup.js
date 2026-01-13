document.addEventListener('DOMContentLoaded', async () => {
    const cookieEl = document.getElementById('cookie');
    const tokenEl = document.getElementById('token');
    const msgEl = document.getElementById('message');
    const btnCopyCookie = document.getElementById('btn-copy-cookie');
    const btnCopyToken = document.getElementById('btn-copy-token');

    // New Elements
    const serverUrlEl = document.getElementById('server-url');
    const adminKeyEl = document.getElementById('admin-key');
    const proxyUrlEl = document.getElementById('proxy-url');
    const btnUpload = document.getElementById('btn-upload');

    // Load saved settings
    chrome.storage.local.get(['serverUrl', 'adminKey', 'proxyUrl'], (result) => {
        if (result.serverUrl) serverUrlEl.value = result.serverUrl;
        if (result.adminKey) adminKeyEl.value = result.adminKey;
        if (result.proxyUrl) proxyUrlEl.value = result.proxyUrl;
    });

    function status(text, type = 'normal') {
        msgEl.textContent = text;
        msgEl.className = 'status ' + type;
        if (type === 'success') {
            setTimeout(() => {
                msgEl.textContent = '';
                msgEl.className = 'status';
            }, 3000);
        }
    }

    async function copyToClipboard(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            console.error('Failed to copy: ', err);
            return false;
        }
    }

    btnCopyCookie.addEventListener('click', async () => {
        if (cookieEl.value && await copyToClipboard(cookieEl.value)) {
            status('Cookie copied!', 'success');
        }
    });

    btnCopyToken.addEventListener('click', async () => {
        if (tokenEl.value && await copyToClipboard(tokenEl.value)) {
            status('Token copied!', 'success');
        }
    });

    // JWT Decode function to get email
    function getEmailFromToken(accessToken) {
        try {
            const base64Url = accessToken.split('.')[1];
            const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
            const jsonPayload = decodeURIComponent(atob(base64).split('').map(function (c) {
                return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
            }).join(''));

            const payload = JSON.parse(jsonPayload);
            // Search for email in profile structure or root
            if (payload['https://api.openai.com/profile'] && payload['https://api.openai.com/profile'].email) {
                return payload['https://api.openai.com/profile'].email;
            }
            return payload.email || null;
        } catch (e) {
            console.error("Failed to parse JWT", e);
            return null;
        }
    }

    // Upload Logic
    btnUpload.addEventListener('click', async () => {
        const serverUrl = serverUrlEl.value.trim().replace(/\/$/, ""); // remove trailing slash
        const adminKey = adminKeyEl.value.trim();
        const proxyUrl = proxyUrlEl.value.trim();
        const accessToken = tokenEl.value;
        const sessionToken = cookieEl.value;

        if (!serverUrl) return status('Server URL is required.', 'error');
        if (!proxyUrl) return status('Proxy URL is required.', 'error');
        if (!accessToken || accessToken.startsWith("Error") || accessToken.startsWith("Fetching")) return status('Access Token not ready.', 'error');
        if (!sessionToken || sessionToken.startsWith("Not found") || sessionToken.startsWith("Fetching")) return status('Session Cookie not ready.', 'error');

        // Save settings
        chrome.storage.local.set({
            serverUrl: serverUrl,
            adminKey: adminKey,
            proxyUrl: proxyUrl
        });

        const email = getEmailFromToken(accessToken);
        if (!email) return status('Could not extract email from token.', 'error');

        status('Uploading...', 'normal');

        try {
            const payload = {
                "tokens": [
                    {
                        "email": email,
                        "access_token": accessToken,
                        "session_token": sessionToken,
                        "proxyUrl": proxyUrl,
                        "is_active": true,
                        "image_enabled": true,
                        "video_enabled": true,
                        "image_concurrency": -1,
                        "video_concurrency": 3
                    }
                ]
            };

            const headers = {
                "Content-Type": "application/json"
            };
            if (adminKey) {
                headers["Authorization"] = `Bearer ${adminKey}`;
            }

            const response = await fetch(`${serverUrl}/api/tokens/import`, {
                method: "POST",
                headers: headers,
                body: JSON.stringify(payload)
            });

            if (response.ok) {
                const resData = await response.json();
                if (resData.success) {
                    status(`Success! Added: ${resData.added}, Updated: ${resData.updated}`, 'success');
                } else {
                    status(`Upload failed: ${resData.message}`, 'error');
                }
            } else {
                status(`Error ${response.status}: ${response.statusText}`, 'error');
            }

        } catch (e) {
            console.error(e);
            status(`Network error: ${e.message}`, 'error');
        }
    });

    // 1. Get Cookie
    try {
        const cookie = await chrome.cookies.get({
            url: "https://chatgpt.com",
            name: "__Secure-next-auth.session-token"
        });

        if (cookie) {
            cookieEl.value = cookie.value;
        } else {
            cookieEl.value = "Not found. Please log in to ChatGPT.";
            status('Session cookie not found.', 'error');
        }
    } catch (error) {
        console.error(error);
        cookieEl.value = "Error fetching cookie.";
    }

    // 2. Get Access Token
    try {
        const response = await fetch("https://chatgpt.com/api/auth/session");

        if (response.ok) {
            const data = await response.json();
            if (data.accessToken) {
                tokenEl.value = data.accessToken;
            } else {
                tokenEl.value = "accessToken not found in response.";
            }
        } else {
            tokenEl.value = `Failed to fetch session: ${response.status}`;
            if (response.status === 403) {
                status('403 Forbidden. Please refresh page.', 'error');
            }
        }
    } catch (error) {
        console.error(error);
        tokenEl.value = `Error: ${error.message}`;
    }
});
