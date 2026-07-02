import asyncio 
import httpx, json, os
import aiosqlite
import aiofiles

from arq.connections import RedisSettings

r_settings = RedisSettings(host="localhost", port=6379, database=0)

async def startup(ctx):
    ctx["db"] = await aiosqlite.connect(database="melodyx.db")
async def shutdown(ctx):
    await ctx["db"].close()
async def gen_music(ctx, deepseek_response, id, model, user, proxy, minimax_api_key, minimax_api_key1, replicate, TaskStatusResponse):
    redis = ctx["redis"]
    db = ctx["db"]
    try:
        proxy_req = f"http://{proxy}@196.19.120.153:8000"
        async with httpx.AsyncClient(timeout=210.0, proxy=proxy_req) as http_client:
            
            if model == 1:
                headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {minimax_api_key1}"
                }
                payload = {
                "prompt": deepseek_response.get("optimized_prompt"),
                "lyrics_prompt": deepseek_response.get("lyrics"),
                "model": "v2.0",
                "sample_rate": "44100",
                "bitrate": "256000",
                "format": "mp3",
                "translate_input": False
                }
                url = "https://api.gen-api.ru/api/v1/networks/minimax-music"
            
                request = await http_client.post(url, headers=headers, json=payload)
                minimax_response = request.json()
                print(request.status_code)
                print(minimax_response)
                
                if request.status_code != 200:
                    await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                    await redis.hset(name=f"tasks:{id}", key="audio_url", value="") 
                else:
                    mm_task_id = minimax_response.get("request_id")
                    url_status = f"https://api.gen-api.ru/api/v1/request/get/{mm_task_id}"
                    for _ in range(20):
                        await asyncio.sleep(5)
                        request = await http_client.get(url_status, headers=headers)
                        minimax_response = request.json()
                        if minimax_response.get("status") == "success":
                            if minimax_response.get("result") and isinstance(minimax_response.get("result"), list):
                                await redis.hset(name=f"tasks:{id}", key="audio_url", value=minimax_response.get("result")[0])
                                await redis.hset(name=f"tasks:{id}", key="status",value="saving")
                                print(minimax_response)
                                break
                        elif request.status_code != 200:
                            print(request.status_code)
                            print(minimax_response)
                            await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                            await redis.hset(name=f"tasks:{id}", key="audio_url", value="") 
                            return
                    else:
                        await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                        return
                    
            elif model == 2:
                headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {minimax_api_key}"
                }
                payload = {
                "model": "music-2.6",
                "prompt": deepseek_response.get("optimized_prompt"),
                "lyrics": deepseek_response.get("lyrics"),
                "output_format": "url",
                "audio_settings": {
                    "sample_rate": 44100,
                    "bitrate": 256000,
                    "format": "mp3",
                    "translate_input": False
                    }
                }
                url = "https://api.minimax.io/v1/music_generation"
            
                request = await http_client.post(url, headers=headers, json=payload)
                minimax_response = request.json()
                print(request.status_code)
                print(minimax_response)
                
                if request.status_code != 200:
                    await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                    await redis.hset(name=f"tasks:{id}", key="audio_url", value="") 
                else:
                        
                    if minimax_response.get("data")["status"] == 2:
                        await redis.hset(name=f"tasks:{id}", key="audio_url", value=minimax_response.get("data")["audio"])
                        await redis.hset(name=f"tasks:{id}", key="status",value="saving")
                            
                    else:
                        await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                        await redis.hset(name=f"tasks:{id}", key="audio_url", value="") 
            elif model == 3:
                headers = {
                "Authorization": f"Bearer {replicate}",
                "Content-Type": "application/json"
                }
                payload = {
                    "input": {
                        "prompt": deepseek_response.get("optimized_prompt")
                    }
                }
                url = "https://api.replicate.com/v1/models/google/lyria-3-pro/predictions"
                request = await http_client.post(url, headers=headers, json=payload)
                lyr_response = request.json()
                print(request.status_code)
                print(lyr_response)
                
                if request.status_code != 200 and request.status_code != 201:
                    await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                    await redis.hset(name=f"tasks:{id}", key="audio_url", value="") 
                else:
                    l_task_id = lyr_response.get("id")
                    url_status = f"https://api.replicate.com/v1/predictions/{l_task_id}"
                    for _ in range(20):
                        await asyncio.sleep(5)
                        request = await http_client.get(url_status, headers=headers)
                        lyr_response = request.json()
                        if lyr_response.get("status") == "succeeded":
                            await redis.hset(name=f"tasks:{id}", key="audio_url", value=lyr_response.get("output"))
                            await redis.hset(name=f"tasks:{id}", key="status",value="saving")
                            print(lyr_response)
                            break
                        elif request.status_code != 200 and request.status_code != 201:
                            print(request.status_code)
                            print(lyr_response)
                            await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                            await redis.hset(name=f"tasks:{id}", key="audio_url", value="") 
                    else:
                        await redis.hset(name=f"tasks:{id}", key="status",value="failed")
            status = await redis.hget(name=f"tasks:{id}", key="status")
            if status == "failed":
                if model == 1 or model == 2:
                    sum = 0.16
                else:
                    sum = 0.1
                await db.execute("""
                    UPDATE users SET balance = balance - ? WHERE sub = ?
                 """, (sum, user))
                await db.commit()
            final_gen = await redis.hget(name=f"tasks:{id}", key="audio_url")
            
            if final_gen:
                format = ".wav" if ".wav" in final_gen else ".mp3"
                audio_file = await http_client.get(final_gen, follow_redirects=True)
                if audio_file.status_code == 200:
                    fname = f"{id}{format}"
                    fpath = os.path.join("static", fname)
                    async with aiofiles.open(fpath, "wb") as f:
                        await f.write(audio_file.content)
                        await redis.hset(name=f"tasks:{id}", key="audio_url", value=f"/static/{fname}")
                        await redis.hset(name=f"tasks:{id}", key="status",value="success")
        
                        
            
    except Exception as e:
        await redis.hset(name=f"tasks:{id}", key="status",value="failed")
        print(e)
    
    data = await redis.hgetall(f"tasks:{id}")
    await redis.expire(f"tasks:{id}", 3000)
    if data:
        data["meta"] = json.loads(data["meta"])
        task = TaskStatusResponse.model_validate(data)
        task =  task.model_dump()
        if task["status"] == "success":
            await db.execute("""
            INSERT INTO tracks (id, user_id, name, cover_url, genre, color, track_url, optimized_prompt) SELECT
            ?, ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM tracks WHERE id = ?)

            """, (id, user, task["meta"]["track_name"], task["meta"]["cover_url"], task["meta"]["genre"], task["meta"]["color"], task["audio_url"], task["meta"]["optimized_prompt"], id)
            )
            await db.commit()

class WorkerSettings:
    functions = []
    on_sturtup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings
