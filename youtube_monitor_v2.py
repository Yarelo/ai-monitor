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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN
# ============================================================
YOUTUBE_API_KEY = "AIzaSyC-Ic_jfNNk431RWylwbgrk1ZjmZ5RMqHc"
GROQ_API_KEY = "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"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1492222494936010822/yWf0VqhO3AILcDXd-WgOLQn1eOC-0vOoeqWAr2Gp2tJyyOAN5MDQBVJNXRSkLHrKMzGD"

DAYS_BACK = 30
MAX_WORKERS = 5
CACHE_FILE = "channel_cache.json"
PROCESSED_FILE = "processed_videos.json"

# ============================================================
# 100 CANALES VERIFICADOS
# ============================================================
CHANNELS = [
    # === AGENTES / OPENCLAW / VIBE CODING ===
    "@AlexFinnOfficial",        # Agentes no-code, vibe coding
    "@MatthewBerman",           # Testing modelos y agentes
    "@MckayWrigley",            # Herramientas agénticas
    "@LiamOttley",              # Agencias de IA
    "@WesRoth",                 # Noticias agentes autónomos
    "@DaveShapiro",             # Arquitectura cognitiva de agentes
    "@AIFoundations",           # Vibe coding y agentes (Drake Surach)
    "@NateHerk",                # n8n, agentes no-code
    "@SabrinaRamonov",          # Agentes, prompts, automatización
    "@corbin_brown",            # Builds técnicos de agentes
    "@JackRobertsAI",           # Agentes de negocio no-code
    "@AICodeKing",              # Herramientas AI dev, alternativas gratis
    "@AutomataLearningLab",     # LangChain, LLM agents técnicos
    "@SkillLeapAI",             # Tutoriales de implementación
    "@WorldofAI",               # Repositorios GitHub nuevos
    "@PromptEngineering",       # Lógica de sistemas y prompts
    "@AIVantage",               # Flujos de trabajo con agentes
    "@DavidOndrej",             # IA para negocios, agentes
    "@AstroKJoseph",            # Apps con IA, negocio
    "@BartSlodyczka",           # Automatización avanzada y n8n

    # === NOTICIAS E IA GENERAL ===
    "@mreflow",                 # Matt Wolfe - noticias AI
    "@AIExplained",             # Explicaciones de IA
    "@Fireship",                # Noticias código ultra-rápidas
    "@TheAIDailyBrief",         # Brief diario de IA
    "@YannicKilcher",           # Papers de IA y modelos
    "@TwoMinutePapers",         # Resúmenes de papers
    "@ColdFusion",              # Tech y futuro
    "@DotCSV",                  # IA en español (Carlos Santana)
    "@howardjeremyp",           # Jeremy Howard - fast.ai
    "@WhatsAIbyLouisBouchard",  # IA explicada para no expertos
    "@RowanCheung",             # Noticias IA rápidas
    "@ThePrimeCast",            # ThePrimeagen - dev opinión
    "@ibm",                     # IBM Technology - IA empresarial
    "@GoogleDeepMind",          # DeepMind oficial
    "@OpenAI",                  # OpenAI oficial
    "@Anthropic",               # Anthropic oficial
    "@MetaAI",                  # Meta AI oficial
    "@MicrosoftResearch",       # Microsoft Research
    "@NVIDIA",                  # NVIDIA AI y hardware
    "@GoogleCloudTech",         # GenAI con cloud

    # === DESARROLLO / CÓDIGO / LLMs ===
    "@AndrejKarpathy",          # Deep dives LLMs fundamentales
    "@3blue1brown",             # Visualizaciones redes neuronales
    "@krishnaik06",             # Krish Naik - GenAI hands-on
    "@HuggingFace",             # Open source models
    "@LangChain",               # Frameworks de agentes
    "@Deeplearningai",          # Andrew Ng - cursos IA
    "@FreeCodeCamp",            # Cursos largos gratuitos
    "@TechWithTim",             # Python e IA práctica
    "@Sentdex",                 # IA en Python
    "@StatQuestwithJoshStarmer", # Estadística para ML
    "@CodeEmporium",            # ML desde código
    "@iNeuronIntelligence",     # Cursos técnicos GenAI
    "@SamWitteveen",            # 11 años deep learning, LLMs
    "@1littlecoder",            # IA práctica y demos rápidas
    "@AIJasonZ",                # Builds de agentes técnicos
    "@AssemblyAI",              # APIs de IA y tutoriales
    "@Vercel",                  # Infraestructura web para IA
    "@Docker",                  # Containers para agentes
    "@GitHubTraining",          # GitHub Actions y automatización
    "@TraversyMedia",           # Tutoriales full-stack con IA

    # === AUTOMATIZACIÓN / n8n / MAKE ===
    "@n8n-io",                  # n8n oficial - automatización
    "@Make",                    # Make.com oficial
    "@BenNaderi",               # Workflows de automatización IA
    "@AutoGPTOfficial",         # AutoGPT agentes
    "@LucasAutomata",           # LangChain + automatización
    "@AIAutomationAgency",      # Agencias de automatización IA
    "@RubenHassid",             # No-code IA workflows
    "@GraceLeungAI",            # Marketing + agentes IA
    "@RyanDoserAI",             # Claude Code, marketing IA
    "@JustinFineberg",          # IA para negocios

    # === VISIONARIOS / INVESTIGACIÓN / AGI ===
    "@LexFridman",              # Entrevistas técnicas profundas
    "@DwarkeshPatel",           # Podcast con líderes de IA
    "@StanfordOnline",          # Cursos de IA Stanford
    "@MachineLearningStreetTalk", # Debates técnicos ML
    "@TheInsideView",           # Perspectivas sobre AGI
    "@BrainInspired",           # Neurociencia + IA
    "@TheAIEdge",               # IA empresarial aplicada
    "@VoxAgent",                # Podcast IA para ejecutivos
    "@BuildFastWithAI",         # Builds rápidos con IA
    "@AgentZeroChannel",        # Agent Zero - agentes open source

    # === ESPAÑOL / PORTUGUÉS / OTROS IDIOMAS ===
    "@DotCSV",                  # Carlos Santana Vega - España
    "@Datahack",                # IA en español técnico
    "@DescubreIA",              # IA en español - noticias
    "@IALatam",                 # IA para Latinoamérica
    "@TechConTino",             # Tech e IA en español
    "@MundoIA",                 # Noticias IA en español
    "@Turing",                  # IA en portugués Brasil
    "@AIPorBR",                 # IA noticias Brasil
    "@KI_Tutorials",            # IA en alemán (KI = AI)
    "@VibeVenture",             # Vibe coding en alemán

    # === IMPLEMENTACIÓN PRÁCTICA / NEGOCIO ===
    "@TheNextWaveAI",           # Trends de IA para negocios
    "@SuperhumanAI",            # Productividad con IA
    "@MarketingAIInstitute",    # Marketing + IA
    "@FutureTools",             # Reviews de herramientas IA
    "@PracticalAI",             # IA práctica para empresas
    "@ConnorGrennan",           # IA para ejecutivos
    "@RachelWoodsAI",           # Automatización negocio
    "@Masynctech",              # Vibe coding full-stack
    "@TotalTechZonne",          # Agentic AI y data
    "@HelloWorldIndia",         # IA en Hindi/Inglés - India
]

# ============================================================
# SESSION CON RETRY
# ============================================================
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# ============================================================
# CACHE
# ============================================================
def load_json_file(filepath, default=None):
    if default is None:
        default = {}
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except:
        return default

def save_json_file(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

# ============================================================
# FUNCIONES
# ============================================================
def get_channel_id(handle, cache):
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
    except Exception as e:
        log.warning(f"Error resolviendo {handle}: {e}")
    return None


def get_recent_videos(channel_id):
    published_after = datetime.utcnow() - timedelta(days=DAYS_BACK)
    if channel_id.startswith("UC"):
        playlist_id = "UU" + channel_id[2:]
    else:
        return []
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {"part": "snippet", "playlistId": playlist_id, "maxResults": 5, "key": YOUTUBE_API_KEY}
    try:
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
        items = r.json().get("items", [])
        recent = []
        for item in items:
            published = item["snippet"].get("publishedAt", "")
            try:
                pub_date = datetime.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
            except:
                continue
            if pub_date >= published_after:
                video_id = item["snippet"]["resourceId"].get("videoId")
                if video_id:
                    recent.append({"id": video_id, "title": item["snippet"]["title"]})
        return recent[:3]
    except Exception as e:
        log.warning(f"Error videos {channel_id}: {e}")
        return []


def get_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["es", "en", "pt", "de", "fr"])
        text = " ".join([t["text"] for t in transcript])
        return text[:4000]
    except (TranscriptsDisabled, NoTranscriptFound):
        return None
    except Exception as e:
        log.warning(f"Error transcript {video_id}: {e}")
        return None


def analyze_with_groq(channel_name, video_title, transcript):
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = f"""Eres un asistente que analiza contenido de YouTube sobre IA, agentes, OpenClaw, automatización y tech.

Canal: {channel_name}
Video: {video_title[:200]}
Transcript: {transcript[:3500]}

Responde SOLO con este JSON sin texto extra:
{{
  "relevancia": "Alta | Media | Baja",
  "resumen": "2-3 oraciones en español",
  "idea_para_bot": "Accion concreta implementable en OpenClaw o bot de Discord",
  "implementar": true
}}

Alta = comandos nuevos, skills, modelos, agentes, OpenClaw updates, automatización.
Solo implementar=true si es algo concreto y accionable."""

    body = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "max_tokens": 500, "temperature": 0.3}
    try:
        r = session.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=body, timeout=30)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1].rsplit("```", 1)[0]
        return json.loads(content)
    except Exception as e:
        log.warning(f"Error Groq {channel_name}: {e}")
        return None


def send_to_discord(message):
    try:
        session.post(DISCORD_WEBHOOK, json={"content": message}, timeout=10)
        sleep(0.5)
    except Exception as e:
        log.error(f"Error Discord: {e}")


def relevancia_emoji(r):
    return {"Alta": "🔴", "Media": "🟡", "Baja": "🟢"}.get(r, "⚪")


def process_channel(handle, cache, processed_ids):
    resultados = []
    channel_id = get_channel_id(handle, cache)
    if not channel_id:
        return resultados
    videos = get_recent_videos(channel_id)
    for video in videos:
        video_id = video["id"]
        if video_id in processed_ids:
            continue
        transcript = get_transcript(video_id)
        if not transcript:
            continue
        analysis = analyze_with_groq(handle, video["title"], transcript)
        if not analysis:
            continue
        processed_ids.add(video_id)
        if analysis.get("relevancia") in ["Alta", "Media"]:
            resultados.append({
                "handle": handle,
                "title": video["title"],
                "url": f"https://youtube.com/watch?v={video_id}",
                "analysis": analysis
            })
    return resultados


# ============================================================
# MAIN
# ============================================================
def main():
    log.info("Iniciando análisis...")
    send_to_discord("🔍 Analizando 100 canales de IA... dame unos minutos.")

    cache = load_json_file(CACHE_FILE)
    processed_list = load_json_file(PROCESSED_FILE, default=[])
    processed_ids = set(processed_list)
    resultados = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_channel, h, cache, processed_ids): h for h in CHANNELS}
        for future in as_completed(futures):
            handle = futures[future]
            try:
                results = future.result()
                if results:
                    resultados.extend(results)
                    log.info(f"✓ {handle}: {len(results)} resultado(s)")
            except Exception as e:
                log.error(f"✗ {handle}: {e}")

    save_json_file(CACHE_FILE, cache)
    save_json_file(PROCESSED_FILE, list(processed_ids))

    if not resultados:
        send_to_discord("✅ Análisis completado. No hay contenido relevante este período.")
        return

    header = "━━━━━━━━━━━━━━━━━━━━━━━━\n📊 **RESUMEN DE IA — ÚLTIMOS 30 DÍAS**\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    entries = []
    for i, item in enumerate(resultados, 1):
        a = item["analysis"]
        emoji = relevancia_emoji(a.get("relevancia", ""))
        entry = f"{emoji} **{item['handle']}** — [{item['title']}]({item['url']})\n"
        entry += f"📝 {a.get('resumen', '')}\n"
        if a.get("implementar"):
            entry += f"💡 **Idea:** {a.get('idea_para_bot', '')}\n"
        entry += "\n"
        entries.append(entry)

    messages = []
    current = header
    for entry in entries:
        if len(current) + len(entry) > 1900:
            messages.append(current)
            current = ""
        current += entry
    if current.strip():
        messages.append(current)

    for msg in messages:
        send_to_discord(msg)

    log.info("Resumen enviado.")

if __name__ == "__main__":
    main()
