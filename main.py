from fastapi import FastAPI, Depends, BackgroundTasks, Request, Header
import dotenv, os, uuid, httpx, json
from pydantic import BaseModel
from openai import AsyncOpenAI
from fastapi import HTTPException
import datetime, jwt, base64
from datetime import timezone
from redis.asyncio import Redis
from fastapi.staticfiles import StaticFiles
import aiofiles, asyncio
from contextlib import asynccontextmanager
import aiosqlite
from fastapi.security import OAuth2PasswordBearer
from google.oauth2 import id_token
from google.auth.transport import requests
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

dotenv.load_dotenv()
deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
minimax_api_key = os.getenv("MINIMAX_API_KEY")
minimax_api_key1 = os.getenv("MINIMAX_API_KEY1")
pexels_api_key = os.getenv("PEXELS_API_KEY")
client_id = os.getenv("CL_ID")
replicate = os.getenv("R_TOKEN")
secret = os.getenv("SECRET")
proxy = os.getenv("PROXY")

redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
async def get_redis()->Redis:
    return redis
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await aiosqlite.connect(database="melodyx.db")
    await app.state.db.execute("""PRAGMA foreign_keys = ON""")
    await app.state.db.execute("""
        CREATE TABLE IF NOT EXISTS users (
               sub TEXT PRIMARY KEY,
               name TEXT,
               email TEXT,
               balance FLOAT,
               free_limit INTEGER,
               expire TIMESTAMP
    );"""
    )
    await app.state.db.execute("""
        CREATE TABLE IF NOT EXISTS tracks (
               id TEXT PRIMARY KEY,
               name TEXT,
               cover_url TEXT,
               user_id TEXT,
               genre TEXT,
               color TEXT,
               is_liked BOOLEAN DEFAULT FALSE,
               optimized_prompt TEXT,
               track_url TEXT,
               FOREIGN KEY (user_id) REFERENCES users(sub) 
        );
    """)
    await app.state.db.execute("""
        CREATE TABLE IF NOT EXISTS history (
               id TEXT PRIMARY KEY,
               user_id TEXT,
               track_id TEXT,
               FOREIGN KEY (user_id) REFERENCES users(sub),
               FOREIGN KEY (track_id) REFERENCES tracks(id)
    );"""
    )
    await app.state.db.commit()
    yield
    await app.state.db.close()

def get_db(request: Request):
    return request.app.state.db
auth_scheme = OAuth2PasswordBearer(tokenUrl="/auth/google/")
async def get_user(token: str=Depends(auth_scheme)):
    if secret:
        bytes_s = base64.b64decode(secret)
        try:
            data = jwt.decode(token, bytes_s, algorithms=["HS256"]) 
            return data["sub"]
        except jwt.MissingRequiredClaimError:
            raise HTTPException(status_code=401)
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401)
        except jwt.InvalidKeyError:
            raise HTTPException(status_code=401)
    raise HTTPException(status_code=502)
    
        
client = AsyncOpenAI(
    api_key=deepseek_api_key,
    base_url="https://api.deepseek.com"
)
app = FastAPI(lifespan=lifespan)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.mount("/static", StaticFiles(directory="static"), name="static")
async def get_client()->AsyncOpenAI:
    return client
class Prompt(BaseModel):
    content: str
    model: int
class DeepseekGenResponse(BaseModel):
    optimized_prompt: str 
    color: str 
    lyrics: str
    genre: str
    track_name: str
    duration: float
    cover_url_request: str 
class TaskStatusResponse(BaseModel):
    audio_url: None | str 
    status: str 
    meta: dict | None
class GoogleToken(BaseModel):
    token: str
class TrackResponse(BaseModel):
    id: str
    name: str
    cover_url: str
    user_id: str
    genre: str
    is_liked: bool
    color: str
    optimized_prompt: str
    track_url: str 
async def search(deepseek_response, id):
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": str(pexels_api_key)
        }
        params = {
            "per_page": 1,
            "query": deepseek_response.get('cover_url_request')
        }
        proxy_req = f"http://{proxy}@196.19.120.153:8000"
        url = f"https://api.pexels.com/v1/search"
        async with httpx.AsyncClient(timeout=30.0, proxy=proxy_req) as http:
            request = await http.get(url, headers=headers,params=params)
            result = request.json()
            url = result["photos"][0]["src"]["large"]
            print(url)
            headers = {
                 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            file = await http.get(url, headers=headers, follow_redirects=True)
            print("Статус ответа Pexels CDN:", file.status_code)
            if file.status_code == 200:
                fname = f"{id}.jpeg"
                fpath = os.path.join("static", fname)
                async with aiofiles.open(fpath, "wb") as f:
                    await f.write(file.content)
                    deepseek_response["cover_url"] = f"/static/{fname}"

            else:
                deepseek_response["cover_url"] = None
    except Exception as e:
        print(repr(e))
        deepseek_response["cover_url"] = None
    return deepseek_response
async def gen_music(deepseek_response, id, model, redis, db, user):
    try:
        if model == 0:
            async with httpx.AsyncClient(timeout=210.0) as http_client:
                payload = {
                    "prompt": deepseek_response.get("optimized_prompt"),
                    "audio_duration":  deepseek_response.get("duration"),
                    "inference_steps": 8,
                    "thinking": False,
                    "use_cot_caption": False,
                    "use_cot_language": False,
                    "offload_to_cpu": True,
                    "batch_size": 1,
                    "audio_format": "wav"
                }
                url = "http://127.0.0.1:8001/release_task"
            
                request = await http_client.post(url, json=payload)
                acestep_resp = request.json()
                print(acestep_resp)
                idd = acestep_resp["data"]["task_id"]

                for _ in range(30):
                    await asyncio.sleep(3)
                    url = "http://127.0.0.1:8001/query_result"
                    pld = {
                        "task_id_list": [idd]
                    }
                    request = await http_client.post(url, json=pld)
                    result = request.json()
                    print(result)
                    if result["data"][0]["status"] == 1:
                        await redis.hset(name=f"tasks:{id}", key="audio_url", value=f"http://127.0.0.1:8001{json.loads(result['data'][0]['result'])[0]['file']}") 
                        await redis.hset(name=f"tasks:{id}", key="status",value="saving")
                        break
                    elif result["data"][0]["status"] != 1:
                        await redis.hset(name=f"tasks:{id}", key="status",value="processing")
                        continue
                else:
                    await redis.hset(name=f"tasks:{id}", key="status",value="failed")
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
                
        
        else:
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
                        return
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
                                return
                        else:
                            await redis.hset(name=f"tasks:{id}", key="status",value="failed")
                            return
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
    
async def get_response(input_prompt: Prompt, id, client):
    prompt = input_prompt.model_dump()
    prompt["role"] = "user"

    messages = [
        {"role": "system", "content": (
            "Ты — ассистент для генерации музыки. Пользователь описывает желаемую музыку. "
            "Твоя задача — создать строгий JSON со следующими полями:\n"
            "- optimized_prompt: детальное описание музыки на **английском языке**, чтобы ии генерации музыки точно понял, что от него хочет пользователь, спецсимволы нужно экранировать в json если что"
            "Опиши жанр, текст песни, настроение, инструменты, темп.\n"
            "- lyrics: если пользователь явно указал слова для песни, напиши их с добавлением "
            "структурных тегов [intro], [verse], [chorus], [bridge], [outro] на английском. "
            "Если пользователь не дал текст (только инструментальное описание), "
            "строго напиши '[Instrumental]'. Поле не должно быть пустым.\n"
            "- color: HEX-цвет, ассоциирующийся с музыкой (например, '#FF5733').\n"
            "- genre: жанр трека на русском или английском.\n"
            "- track_name: оригинальное название трека на английском.\n"
            "- cover_url_request: краткий поисковый запрос на английском для картинки-обложки "
            "(например, 'synthwave night city cover art').\n"
            "- duration: длительность трека, исходя из запроса пользователя, но не более 180сек и не менее 40сек\n\n"
            "Ответь **только** JSON-объектом с ключами: "
            "optimized_prompt, lyrics, color, genre, duration, track_name, cover_url_request."
        )},
        prompt
    ]
    deepseek_response = await client.chat.completions.create(
        model="deepseek-chat",
        messages=messages,
        response_format={"type": "json_object"}
    )
    raw_response = deepseek_response.choices[0].message.content
    deepseek_response = DeepseekGenResponse.model_validate_json(raw_response)
    deepseek_response = deepseek_response.model_dump()
    deepseek_response = await search(deepseek_response, id)
    return deepseek_response


async def check_sub(db, user, model):
    cursor = await db.execute("""SELECT balance, free_limit, expire FROM users WHERE sub = ?""", (user,))
    row = await cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404)
    balance = float(row[0])
    limit = int(row[1])
    expire = datetime.datetime.fromisoformat(row[2])
    
    if model != 0:
        if model in (1, 2):
            if balance >= 0.15:
                await db.execute("""UPDATE users SET balance = balance - ? WHERE sub = ?""", (0.15, user))
                await db.commit()
                return True
            return False
    
        else:
            if balance >= 0.1:
                await db.execute("""UPDATE users SET balance = balance - ? WHERE sub = ?""", (0.1, user))
                await db.commit()
                return True 
            return False
    
    if expire < datetime.datetime.now(tz=timezone.utc):
        await db.execute("""UPDATE users SET expire = ?, free_limit = ? WHERE sub = ?""", ((datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(hours=3)).isoformat(), 10, user))
        await db.commit()
        return True
    elif limit == 0:
        return False 
    else:
        await db.execute("""UPDATE users SET free_limit = free_limit - ? WHERE sub = ?""", (1, user))
        await db.commit()
        return True


@app.post("/api/generate")
@limiter.limit("5/minute")
async def generate(request: Request, background_tasks: BackgroundTasks, input_prompt: Prompt, client=Depends(get_client), redis=Depends(get_redis), db=Depends(get_db), user=Depends(get_user)):
    task_id: str = str(uuid.uuid4())
    input_model = input_prompt.model
    sub = await check_sub(db, user, input_model)
    if not sub:
        raise HTTPException(status_code=503)
    response = await get_response(input_prompt, task_id, client)
    meta = {
            "cover_url": response.get("cover_url"),
            "track_name": response.get("track_name"),
            "genre": response.get("genre"),
            "color": response.get("color"),
            "optimized_prompt": response.get("optimized_prompt")
        }
    await redis.hset(name=f"tasks:{task_id}", key="status", value="processing")
    await redis.hset(name=f"tasks:{task_id}", key="audio_url", value="") 
    await redis.hset(name=f"tasks:{task_id}", key="meta", value=json.dumps(meta)) 
    background_tasks.add_task(gen_music, response, task_id, input_model, redis, db, user)
    return {
        "task_id": task_id,
        "status": "processing",
        "message": "Генерация музыки успешно запущена в фоновом режиме."
    }
    
    
@app.get("/api/status/{task_id}")
async def return_status(task_id):
    data = await redis.hgetall(f"tasks:{task_id}")
    await redis.expire(f"tasks:{task_id}", 300)
    if data:
        data["meta"] = json.loads(data["meta"])
        task = TaskStatusResponse.model_validate(data)
        task =  task.model_dump()
        return task
        
    raise HTTPException(status_code=404)

@app.post("/auth/google/")
@limiter.limit("5/minute")
async def google_auth(request: Request, data: GoogleToken, db=Depends(get_db)):
    token = data.token
    try:
        id_inf = id_token.verify_oauth2_token(token, requests.Request(), client_id)
    except Exception as e:
        print(e)
        return {"status": "failed"}
    user_id = id_inf["sub"]
    email = id_inf["email"]
    name = id_inf["name"]
    await db.execute(
        """
        INSERT INTO users (sub, email, name, balance, free_limit, expire)
        SELECT ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (
        SELECT 1 FROM users WHERE sub = ?) 
        """, (user_id, email, name, 0.0, 10, (datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(hours=3)).isoformat(), user_id)
    )

    await db.commit()
    payload = {
        "exp": datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(hours=720),
        "sub": user_id
    }
    if secret:
        bytes_s = base64.b64decode(secret)
    else:
        return {"error": "jwt secret required"}
    token = jwt.encode(payload, bytes_s, algorithm="HS256")
    return {"status": "successful", "token": token}

@app.get("/track", response_model=TrackResponse)
async def get_track(id:str, db=Depends(get_db), user=Depends(get_user)):
    cursor = await db.execute("""SELECT id,
               name,
               cover_url,
               user_id,
               genre,
               is_liked,
               color,
               optimized_prompt,
               track_url FROM tracks WHERE id = ? AND user_id = ?""", (id, user))
    track = await cursor.fetchone()
    if track:
        id, name, cover_url, user_id, genre, is_liked, color, optimized_prompt, track_url = track
        return {
            "id": id,
            "name": name,
            "cover_url": cover_url,
            "user_id": user_id,
            "genre": genre,
            "is_liked": is_liked,
            "color": color,
            "optimized_prompt": optimized_prompt,
            "track_url": track_url
        }
    raise HTTPException(status_code=404)
@app.get("/tracks", response_model=list[TrackResponse])
async def get_tracks(user=Depends(get_user), db=Depends(get_db)):
    cursor = await db.execute("""SELECT id,
               name,
               cover_url,
               user_id,
               is_liked,
               genre,
               color,
               optimized_prompt,
               track_url FROM tracks WHERE user_id = ?""", (user,))
    tracks = await cursor.fetchall()
    resp = []
    for track in tracks:
        id, name, cover_url, user_id, is_liked, genre, color, optimized_prompt, track_url = track
        resp.append({
            "id": id,
            "name": name,
            "cover_url": cover_url,
            "user_id": user_id,
            "is_liked": is_liked,
            "genre": genre,
            "color": color,
            "optimized_prompt": optimized_prompt,
            "track_url": track_url
        })
    return resp
@app.post("/upd/history")
async def upd_hi(track_id: str, user=Depends(get_user), db=Depends(get_db)):
    await db.execute("""
        INSERT INTO history (id, user_id, track_id) VALUES (?, ?, ?)
    """, (str(uuid.uuid4()), user, track_id))
    await db.commit()
    cursor = await db.execute("""
        SELECT track_id FROM history WHERE user_id = ?
    """, (user,))
    id_list = []
    rows = await cursor.fetchall()
    for id in rows:
        id_list.append(id[0])
    return {"history": id_list}
@app.get("/history")
async def get_hi(user=Depends(get_user), db=Depends(get_db)):
    cursor = await db.execute("""
        SELECT track_id FROM history WHERE user_id = ?
    """, (user,))
    id_list = []
    rows = await cursor.fetchall()
    for id in rows:
        id_list.append(id[0])
    return {"history": id_list}
@app.patch("/like")
async def like(track_id: str, db=Depends(get_db), user=Depends(get_user)):
    await db.execute("""
        UPDATE tracks SET is_liked = CASE
                     WHEN is_liked = false THEN true
                     ELSE false
                END
                WHERE id = ? AND user_id = ?
    """, (track_id, user))
    await db.commit()

@app.delete("/del/track")
async def delete_track(track_id: str, user=Depends(get_user), db=Depends(get_db)):
    await db.execute("""DELETE FROM history WHERE track_id = ?
            """, (track_id,))
    await db.execute("""DELETE FROM tracks WHERE id = ? AND user_id = ?
            """, (track_id, user))
    await db.commit()

@app.get("/user/info")
async def get_info(user=Depends(get_user), db=Depends(get_db)):
    cursor = await db.execute("""SELECT sub, name, email, balance, free_limit, expire FROM users WHERE users.sub = ?""", (user,))
    row = await cursor.fetchone()
    if row:
        sub,name,email,balance, free_limit, expire = row
        expire = datetime.datetime.fromisoformat(expire)
        return {"sub": sub, "email": email, "balance": balance, "name": name, "free-limit": free_limit, "expire": expire}
    raise HTTPException(status_code=404)

@app.get("/pay")
async def subscribe(sum: int, user=Depends(get_user), redis: Redis=Depends(get_redis)):
    proxy_req = f"http://{proxy}@196.19.120.153:8000"
    async with httpx.AsyncClient(proxy=proxy_req) as http_client:
        headers = {
            "Crypto-Pay-Api-Token": os.getenv("C_TOKEN"),
            "Content-Type": "application/json"
        }
        payload = {
            "asset": "USDT",
            "amount": str(sum),
            "description": "payment",
            "payload": user
        }
        url = "https://testnet-pay.crypt.bot/api/createInvoice"
        result = await http_client.post(url, headers=headers, json=payload)
        result = result.json()
        if result.get("ok") == True:
            await redis.set(user, "processing", 900)
            return {"url": result.get("result")["bot_invoice_url"]}
        raise HTTPException(status_code=500)

@app.post("/cryptobot")
async def webhook(request: Request, db=Depends(get_db), redis=Depends(get_redis)):
    body_bytes = await request.body()
    signature = request.headers.get("crypto-pay-api-signature")
    import hashlib, hmac 
    secret = hashlib.sha256(os.getenv("C_TOKEN").encode()).digest()
    check_hash = hmac.new(secret, body_bytes, hashlib.sha256).hexdigest()
    if not signature or check_hash != signature:
        raise HTTPException(status_code=403)
    body = json.loads(body_bytes)
    if body.get("update_type") == "invoice_paid":
        await db.execute("""UPDATE users SET balance = balance + ? WHERE sub = ?""", (body.get("payload")["amount"], body.get("payload")["payload"],))
        await db.commit()
        await redis.set(body.get("payload")["payload"], "paid")
    else:
        await redis.set(body.get("payload")["payload"], "not_paid")
@app.get("/payment/status")
async def status(user=Depends(get_user), redis=Depends(get_redis)):
    status = await redis.get(user)
    return status