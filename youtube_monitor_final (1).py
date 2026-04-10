import os
import json
import logging
import requests
from datetime import datetime, timedelta
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN
# ============================================================
YOUTUBE_API_KEY = "AIzaSyC-Ic_jfNNk431RWylwbgrk1ZjmZ5RMqHc"
GROQ_API_KEY = "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"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1492222494936010822/yWf0VqhO3AILcDXd-WgOLQn1eOC-0vOoeqWAr2Gp2tJyyOAN5MDQBVJNXRSkLHrKMzGD"

DAYS_BACK = 7
MAX_WORKERS = 5
CACHE_FILE = "channel_cache.json"
PROCESSED_FILE = "processed_videos.json"

# ============================================================
# SESSION CON RETRY
# ============================================================
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount("https://", HTTPAdapter(max_retries=retries))

# ============================================================
# 100 CANALES
# ============================================================
CHANNELS = [
    # Grupo 1: OpenClaw, Vibe Coding y Agentes
    "@KManuS88", "@AlexFinnOfficial", "@JulianGoldieSEO", "@BartSlodyczka",
    "@James-Layne", "@StanleyBishop", "@MatthewBerman", "@MckayWrigley",
    "@LiamOttley", "@WesRoth", "@TheRealSupportLaunchpad", "@AIVantage",
    "@SkillLeapAI", "@WorldofAI", "@PromptEngineering", "@IndieHackers",
    "@RabbitHole", "@DaveShapiro", "@Tyler-Ames", "@EchoHive",
    # Grupo 2: Noticias e Innovacion Global
    "@MattWolfe", "@AIExplained", "@DotCSV", "@Fireship",
    "@TheAIDailyBrief", "@ColdFusion", "@YannicKilcher", "@TwoMinutePapers",
    "@TheRealAiNews", "@TheNeuron", "@RowanCheung", "@ZaneSH",
    "@AllieKMiller", "@PeteHuang", "@MinChoi", "@LinusEkenstam",
    "@NathanLands", "@BenTossell", "@TheRundownAI", "@AIPioneers",
    # Grupo 3: Programacion y Desarrollo
    "@ThePrimeagen", "@JSMastery", "@ByteGrad", "@HuggingFace",
    "@Vercel", "@LangChain", "@Deeplearningai", "@AndrejKarpathy",
    "@Sentdex", "@TraversyMedia", "@FreeCodeCamp", "@CodingWithLewis",
    "@TechWithTim", "@WebDevSimplified", "@KevinPowell", "@DevelopedByEd",
    "@Academind", "@ProgrammingWithMosh", "@Computerphile", "@SirajRaval",
    # Grupo 4: Negocios y ROI
    "@JustinFineberg", "@RubenHassid", "@ConnorGrennan", "@RachelWoods",
    "@TheAIExchange", "@MarketingAIInstitute", "@TheNextWaveAI", "@SuperhumanAI",
    "@AIForBusiness", "@FutureTools", "@PracticalAI", "@ArtificialIntelligenceToday",
    "@Every", "@StrategyAI", "@AutomationMastery",
    # Grupo 5: Visionarios y Ciencia de Datos
    "@LexFridman", "@SamAltman", "@OpenAI", "@Anthropic",
    "@GoogleDeepMind", "@MetaAI", "@MicrosoftResearch", "@StanfordOnline",
    "@MITCBMM", "@NVIDIA", "@DwarkeshPatel", "@HubermanLab",
    "@TheInsideView", "@MachineLearningStreetTalk", "@AIModels",
    # Grupo 6: Especialistas Internacionales
    "@LucJuliaAI", "@TheAIEntrepreneur", "@DataSciGuy", "@AIBuilder",
    "@AgenticFuture", "@ClawDevs", "@AutonomousAI", "@TheCodeReport",
    "@FutureAcademy", "@AI_Revolution",
]

# ============================================================
# CACHE Y DEDUPLICACIÓN
# ============================================================

def load_json_file(filepath, default=None):
    if default is None:
        default = {}
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json_file(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def load_channel_cache():
    return load_json_file(CACHE_FILE)


def save_channel_cache(cache):
    save_json_file(CACHE_FILE, cache)


def load_processed_videos():
    return load_json_file(PROCESSED_FILE, default=[])


def save_processed_videos(processed):
    save_json_file(PROCESSED_FILE, processed)


# ============================================================
# FUNCIONES
# ============================================================

def get_channel_id(handle, cache):
    """Usa channels.list con forHandle (1 unidad) en vez de search.list (100 unidades)."""
    if handle in cache:
        return cache[handle]

    clean = handle.lstrip("@")
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "id", "forHandle": clean, "key": YOUTUBE_API_KEY}

    try:
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
        items = r.json().get("items", [])
        if items:
            channel_id = items[0]["id"]
            cache[handle] = channel_id
            return channel_id
    except requests.exceptions.RequestException as e:
        log.warning(f"Error resolviendo {handle}: {e}")

    return None


def get_upload_playlist_id(channel_id):
    """Convierte channel ID a upload playlist ID (UC... → UU...)."""
    if channel_id.startswith("UC"):
        return "UU" + channel_id[2:]
    return None


def get_recent_videos(channel_id):
    """Usa playlistItems.list (1 unidad) en vez de search.list (100 unidades)."""
    playlist_id = get_upload_playlist_id(channel_id)
    if not playlist_id:
        return []

    published_after = datetime.utcnow() - timedelta(days=DAYS_BACK)
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {
        "part": "snippet",
        "playlistId": playlist_id,
        "maxResults": 5,
        "key": YOUTUBE_API_KEY
    }

    try:
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
        items = r.json().get("items", [])

        recent = []
        for item in items:
            published = item["snippet"].get("publishedAt", "")
            try:
                pub_date = datetime.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue
            if pub_date >= published_after:
                video_id = item["snippet"]["resourceId"].get("videoId")
                if video_id:
                    recent.append({
                        "id": video_id,
                        "title": item["snippet"]["title"],
                        "publishedAt": published
                    })
        return recent[:3]

    except requests.exceptions.RequestException as e:
        log.warning(f"Error obteniendo videos de {channel_id}: {e}")
        return []


def get_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["es", "en"])
        text = " ".join([t["text"] for t in transcript])
        return text[:4000]
    except (TranscriptsDisabled, NoTranscriptFound) as e:
        log.debug(f"Sin transcript para {video_id}: {type(e).__name__}")
        return None
    except Exception as e:
        log.warning(f"Error transcript {video_id}: {e}")
        return None


def sanitize_for_prompt(text, max_len=3500):
    """Limpia texto para evitar prompt injection básica."""
    # Eliminar posibles delimitadores que confundan al LLM
    cleaned = text.replace("```", "").replace("---", "")
    return cleaned[:max_len]


def analyze_with_groq(channel_name, video_title, transcript):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    safe_title = sanitize_for_prompt(video_title, max_len=200)
    safe_transcript = sanitize_for_prompt(transcript)

    prompt = f"""Eres un asistente que analiza contenido de YouTube sobre IA y bots de Discord.

Canal: {channel_name}
Video: {safe_title}
Transcript: {safe_transcript}

Responde SOLO con este JSON sin texto extra:
{{
  "relevancia": "Alta | Media | Baja",
  "resumen": "2-3 oraciones en español",
  "idea_para_bot": "Accion concreta implementable en un bot de Discord con OpenClaw",
  "implementar": true
}}

Solo marca implementar=true si hay algo concreto y accionable."""

    body = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "temperature": 0.3
    }

    try:
        r = session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers, json=body, timeout=30
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        # Limpiar posible markdown wrapping
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1].rsplit("```", 1)[0]
        return json.loads(content)
    except requests.exceptions.RequestException as e:
        log.warning(f"Error Groq para {channel_name}: {e}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        log.warning(f"Error parsing respuesta Groq para {channel_name}: {e}")
        return None


def send_to_discord(message):
    try:
        r = session.post(DISCORD_WEBHOOK, json={"content": message}, timeout=10)
        r.raise_for_status()
        sleep(0.5)  # Rate limit de Discord
    except requests.exceptions.RequestException as e:
        log.error(f"Error enviando a Discord: {e}")


def relevancia_emoji(r):
    return {"Alta": "🔴", "Media": "🟡", "Baja": "🟢"}.get(r, "⚪")


def process_channel(handle, cache, processed_ids):
    """Procesa un canal completo. Retorna lista de resultados."""
    resultados = []

    channel_id = get_channel_id(handle, cache)
    if not channel_id:
        log.debug(f"No se encontró channel ID para {handle}")
        return resultados

    videos = get_recent_videos(channel_id)
    if not videos:
        return resultados

    for video in videos:
        video_id = video["id"]

        # Deduplicación
        if video_id in processed_ids:
            log.debug(f"Video ya procesado: {video_id}")
            continue

        title = video["title"]
        url = f"https://youtube.com/watch?v={video_id}"

        transcript = get_transcript(video_id)
        if not transcript:
            continue

        analysis = analyze_with_groq(handle, title, transcript)
        if not analysis:
            continue

        processed_ids.add(video_id)

        if analysis.get("relevancia") in ["Alta", "Media"]:
            resultados.append({
                "handle": handle,
                "title": title,
                "url": url,
                "analysis": analysis
            })

    return resultados


def build_discord_messages(resultados):
    """Construye mensajes para Discord cortando por entrada completa (no a mitad)."""
    if not resultados:
        return ["✅ Análisis completado. No hay contenido relevante esta semana."]

    header = "━━━━━━━━━━━━━━━━━━━━━━━━\n📊 **RESUMEN SEMANAL DE IA**\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    entries = []
    for i, item in enumerate(resultados, 1):
        a = item["analysis"]
        emoji = relevancia_emoji(a.get("relevancia", ""))
        entry = f"{emoji} **{item['handle']}** — [{item['title']}]({item['url']})\n"
        entry += f"📝 {a.get('resumen', 'Sin resumen')}\n"
        if a.get("implementar"):
            entry += f"💡 **Idea para tu bot:** {a.get('idea_para_bot', '')}\n"
        entry += "\n"
        entries.append(entry)

    # Construir chunks respetando límites de Discord (2000 chars)
    # sin cortar entradas a la mitad
    messages = []
    current = header
    for entry in entries:
        if len(current) + len(entry) > 1900:
            messages.append(current)
            current = ""
        current += entry

    if current.strip():
        messages.append(current)

    return messages


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("Iniciando análisis semanal...")
    send_to_discord("🔍 Analizando 100 canales de IA esta semana... dame unos minutos.")

    # Cargar cache y videos procesados
    cache = load_channel_cache()
    processed_list = load_processed_videos()
    processed_ids = set(processed_list)
    initial_count = len(processed_ids)

    resultados = []

    # Procesar canales con paralelismo controlado
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_channel, handle, cache, processed_ids): handle
            for handle in CHANNELS
        }
        for future in as_completed(futures):
            handle = futures[future]
            try:
                results = future.result()
                if results:
                    resultados.extend(results)
                    log.info(f"✓ {handle}: {len(results)} resultado(s)")
            except Exception as e:
                log.error(f"✗ Error procesando {handle}: {e}")

    # Guardar cache y processed
    save_channel_cache(cache)
    new_ids = processed_ids - set(processed_list)
    save_processed_videos(list(processed_ids))

    log.info(f"Nuevos videos procesados: {len(new_ids)}")
    log.info(f"Resultados relevantes: {len(resultados)}")

    # Enviar a Discord
    messages = build_discord_messages(resultados)
    for msg in messages:
        send_to_discord(msg)

    log.info("Resumen enviado a Discord.")


if __name__ == "__main__":
    main()
