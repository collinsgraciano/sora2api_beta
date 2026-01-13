"""
异步视频生成示例脚本
展示如何使用 X-Sora2-Async 模式异步生成视频
"""
import requests
import time
import json

# ===== 配置 =====
# ===== 配置 =====
API_URL = "http://127.0.0.1:8000"
API_KEY = "han1234"  # 替换为你的 API Key

def submit_async_task(prompt: str, model: str = "sora2-landscape-10s") -> str:
    """提交异步任务，返回 task_id"""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "X-Sora2-Async": "true"
    }
    
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}]
    }
    
    response = requests.post(f"{API_URL}/v1/chat/completions", headers=headers, json=payload)
    if response.status_code != 200:
        print(f"提交失败: {response.text}")
        return None
        
    data = response.json()
    return data.get("task_id")

def get_task_status(task_id: str) -> dict:
    """获取任务状态"""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.get(f"{API_URL}/v1/tasks/{task_id}", headers=headers)
    if response.status_code != 200:
        return {"status": "error", "error": response.text}
    return response.json()

def wait_for_completion(task_id: str, poll_interval: int = 10) -> dict:
    """等待任务完成"""
    if not task_id:
        return {"status": "failed", "error": "Invalid task_id"}
        
    while True:
        status = get_task_status(task_id)
        current_status = status.get('status')
        progress = status.get('progress')
        
        print(f"  [{task_id[-6:]}] 状态: {current_status} | 进度: {progress}")
        
        if current_status in ["success", "completed", "failed"]:
            return status
        
        time.sleep(poll_interval)

# ... (Previous examples omitted for brevity, keeping only modified parts) ...

# ===== 示例 4: 批量提交任务 =====
def example_batch_submit():
    print("\n" + "=" * 50)
    print("示例 4: 批量提交多个任务")
    print("=" * 50)
    
    prompts = [
        "A rocket launching into space, dramatic clouds, sunset",
        "Underwater scene with colorful coral reef and tropical fish",
        "Time-lapse of flowers blooming in a garden"
    ]
    
    task_ids = []
    for i, prompt in enumerate(prompts, 1):
        print(f"\n提交任务 {i}: {prompt[:50]}...")
        task_id = submit_async_task(prompt, "sora2-landscape-10s")
        if task_id:
            task_ids.append(task_id)
            print(f"  Task ID: {task_id}")
    
    print(f"\n已提交 {len(task_ids)} 个任务，开始轮询状态...")
    
    # 轮询所有任务
    pending = task_ids.copy()
    while pending:
        for task_id in pending[:]:
            status_data = get_task_status(task_id)
            status = status_data.get("status")
            
            if status in ["success", "completed", "failed"]:
                pending.remove(task_id)
                emoji = "✅" if status in ["success", "completed"] else "❌"
                print(f"{emoji} 任务 {task_id} 完成: {status}")
                if status in ["success", "completed"]:
                    print(f"   链接: {status_data.get('result_urls', [])}")
        
        if pending:
            print(f"  还有 {len(pending)} 个任务进行中...")
            time.sleep(10)
    
    print("\n所有任务已完成!")
    return task_ids

# ===== 示例 5: 同时生成两个视频 (并发) =====
def example_concurrent_two_videos():
    print("\n" + "=" * 50)
    print("示例 5: 同时生成两个视频 (并发生成)")
    print("=" * 50)
    
    # 定义两个不同的任务
    task1_prompt = "A cute cat playing piano"
    task2_prompt = "A dog riding a skateboard"
    
    print(f"提交任务 1: {task1_prompt}")
    id1 = submit_async_task(task1_prompt)
    print(f"任务 1 ID: {id1}")
    
    print(f"提交任务 2: {task2_prompt}")
    id2 = submit_async_task(task2_prompt)
    print(f"任务 2 ID: {id2}")
    
    if not id1 or not id2:
        print("任务提交失败，无法继续")
        return

    print("\n任务已全部提交，服务器正在后台同时生成这两个视频...")
    print("开始轮询状态...\n")
    
    completed = []
    pending = [id1, id2]
    
    while pending:
        for task_id in pending[:]:
            status_data = get_task_status(task_id)
            status = status_data.get("status")
            progress = status_data.get("progress")
            
            print(f"  任务 {task_id[-6:]}: {status} ({progress})")
            
            if status in ["success", "completed", "failed"]:
                pending.remove(task_id)
                completed.append((task_id, status_data))
                
        if pending:
            print("-" * 30)
            time.sleep(20)
            
    print("\n" + "=" * 50)
    print("生成结果:")
    print("=" * 50)
    
    for task_id, data in completed:
        status = data.get("status")
        prompt = data.get("prompt", "Unknown prompt")
        print(f"\n任务 ID: {task_id}")
        print(f"提示词: {prompt}")
        if status in ["success", "completed"]:
            urls = data.get("result_urls", [])
            print("✅ 成功!")
            if urls:
                print(f"视频地址: {urls[0]}")
            else:
                print("未找到地址")
        else:
            print(f"❌ 失败: {data.get('error_message')}")

# ===== 主程序 =====
if __name__ == "__main__":
    print("Sora2 异步视频生成示例")
    print("请确保服务已启动 (python main.py)\n")
    
    # 运行并发生成示例
    example_concurrent_two_videos()
    
    # 其他示例:
    # example_batch_submit()

