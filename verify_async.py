import requests
import time
import json
import sys

# Configuration
API_URL = "http://127.0.0.1:8000"
API_KEY = "han1234" # Replace with your actual API key

def test_async_generation():
    print(f"Testing Async Generation against {API_URL}...")
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "X-Sora2-Async": "true"
    }
    
    payload = {
        "model": "sora2-landscape-10s",
        "messages": [
            {"role": "user", "content": "a dog eat banana"}
        ]
    }
    
    try:
        # Step 1: Submit Task
        print("1. Submitting async task...")
        response = requests.post(f"{API_URL}/v1/chat/completions", headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"Failed to submit task. Status: {response.status_code}")
            print(response.text)
            return

        data = response.json()
        task_id = data.get("task_id")
        print(response.text)
        if not task_id:
            print("No task_id received in response!")
            print(json.dumps(data, indent=2))
            return
            
        print(f"Task submitted successfully! Task ID: {task_id}")
        
        # Step 2: Poll Status
        print("\n2. Polling task status (Ctrl+C to stop)...")
        while True:
            status_response = requests.get(f"{API_URL}/v1/tasks/{task_id}", headers=headers)
            print(status_response.text)
            if status_response.status_code != 200:
                print(f"Failed to get status. Code: {status_response.status_code}")
                break
                
            status_data = status_response.json()
            status = status_data.get("status")
            progress = status_data.get("progress")
            
            print(f"Status: {status} | Progress: {progress}")
            
            if status == "success" or status == "completed":
                print("\nTask Completed Successfully!")
                result_urls = status_data.get("result_urls", [])
                if result_urls:
                    print(f"Video URL: {result_urls[0]}")
                else:
                    print("No result URL found.")
                print(json.dumps(status_data, indent=2))
                break
            elif status == "failed":
                print("\nTask Failed!")
                print(json.dumps(status_data, indent=2))
                break
                
            time.sleep(10)
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_async_generation()
