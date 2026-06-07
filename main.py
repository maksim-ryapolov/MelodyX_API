from fastapi import FastAPI, Depends, BackgroundTasks
import dotenv, os, uuid, httpx
from pydantic import BaseModel
from openai import AsyncOpenAI
from ddgs import DDGS
from fastapi.staticfiles import StaticFiles
import aiofiles, asyncio

dotenv.load_dotenv()
deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
minimax_api_key = os.getenv("MINIMAX_API_KEY")

client = AsyncOpenAI(
    api_key=deepseek_api_key,
    base_url="https://api.deepseek.com"
)
TASKS_DB = {}
app = FastAPI()
ddgs_client = DDGS()
app.mount("/static", StaticFiles(directory="static"), name="static")
async def get_client()->AsyncOpenAI:
    return client
class Prompt(BaseModel):
    content: str
class DeepseekGenResponse(BaseModel):
    optimized_prompt: str 
    color: str 
    lyrics: str
    genre: str
    track_name: str
    cover_url_request: str 
class TaskStatusResponse(BaseModel):
    audio_url: None | str 
    status: str 
    cover_url: str | None
    meta: dict | None
def search(deepseek_response):
    try:
        ddg = ddgs_client
        results = ddg.images(deepseek_response["cover_url_request"], max_results=3)
        if results and len(results) > 0:
            deepseek_response["cover_url"] = results[0]["image"]
        else:
            deepseek_response["cover_url"] = None
    except Exception as e:
        print(e)
        deepseek_response["cover_url"] = None
    return deepseek_response
async def gen_music(deepseek_response, id):
    headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {minimax_api_key}"
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
    try:
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            request = await http_client.post(url, headers=headers, json=payload)
            minimax_response = request.json()
            print(request.status_code)
            print(minimax_response)
            
            if request.status_code != 200:
                TASKS_DB[id]["status"] = "failed"
                TASKS_DB[id]["audio_url"] = None
                return
            else:
                mm_task_id = minimax_response.get("request_id")
                url_status = f"https://api.gen-api.ru/api/v1/request/get/{mm_task_id}"
                for _ in range(20):
                    await asyncio.sleep(5)
                    request = await http_client.get(url_status, headers=headers)
                    minimax_response = request.json()
                    if minimax_response.get("status") == "success":
                        if minimax_response.get("result") and isinstance(minimax_response.get("result"), list):
                            TASKS_DB[id]["audio_url"] = minimax_response.get("result")[0]
                            TASKS_DB[id]["status"] = "success"
                            print(minimax_response)
                            break
                    elif request.status_code != 200:
                        print(request.status_code)
                        print(minimax_response)
                        TASKS_DB[id]["status"] = "failed"
                        TASKS_DB[id]["audio_url"] = None
                        return
                else:
                    TASKS_DB[id]["status"] = "failed"
                    return
                final_gen = TASKS_DB[id]["audio_url"]
                if final_gen:
                    audio_file = await http_client.get(final_gen, follow_redirects=True)
                    if audio_file.status_code == 200:
                        fname = f"{id}.mp3"
                        fpath = os.path.join("static", fname)
                        async with aiofiles.open(fpath, "wb") as f:
                            await f.write(audio_file.content)
                        TASKS_DB[id]["audio_url"] = f"/static/{fname}"
                            
                        
    except Exception as e:
        TASKS_DB[id]["status"] = "failed"
        print(e)
    
    
async def get_response(input_prompt: Prompt, duration: str="15", client=Depends(get_client)):
    prompt = input_prompt.model_dump()
    prompt["role"] = "user"
    prompt["content"] += f" длительность{duration}"
    messages = [
        {"role": "system", "content": (
            "Ты — ассистент для генерации музыки. Пользователь описывает желаемую музыку. "
            "Твоя задача — создать строгий JSON со следующими полями:\n"
            "- optimized_prompt: краткое описание музыки на **английском языке**, "
            "подходящее для подачи в модель генерации (например, 'synthwave, retro, driving beat'). "
            "Опиши жанр, настроение, инструменты, темп.\n"
            "- lyrics: если пользователь явно указал слова для песни, напиши их с добавлением "
            "структурных тегов [intro], [verse], [chorus], [bridge], [outro] на английском. "
            "Если пользователь не дал текст (только инструментальное описание), "
            "строго напиши '[Instrumental]'. Поле не должно быть пустым.\n"
            "- color: HEX-цвет, ассоциирующийся с музыкой (например, '#FF5733').\n"
            "- genre: жанр трека на русском или английском.\n"
            "- track_name: оригинальное название трека на английском.\n"
            "- cover_url_request: краткий поисковый запрос на английском для картинки-обложки "
            "(например, 'synthwave night city cover art').\n\n"
            "Ответь **только** JSON-объектом с ключами: "
            "optimized_prompt, lyrics, color, genre, track_name, cover_url_request."
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
    deepseek_response = await asyncio.to_thread(search, deepseek_response)
    return deepseek_response




@app.post("/api/generate")
async def generate(backgroundtasks: BackgroundTasks, response=Depends(get_response)):
    task_id: str = str(uuid.uuid4())
    TASKS_DB[task_id] = {
        "status": "processing",
        "audio_url": None,
        "cover_url": response.get("cover_url"),
        "meta": {
            "track_name": response.get("track_name"),
            "genre": response.get("genre"),
            "color": response.get("color"),
            "optimized_prompt": response.get("optimized_prompt")
        }
    }
    backgroundtasks.add_task(gen_music, response, task_id)
    return {
        "task_id": task_id,
        "status": "processing",
        "message": "Генерация музыки успешно запущена в фоновом режиме."
    }
    
    
@app.get("/api/status/{task_id}")
async def return_status(task_id):
    task = TaskStatusResponse.model_validate(TASKS_DB[task_id])
    return task.model_dump()
