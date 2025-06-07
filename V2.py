import asyncio
import json
import urllib.parse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import os
import time
from collections import Counter
from datetime import datetime
import pickle
from pathlib import Path
import sys
from typing import Dict, Set
import logging
import argparse
import shutil

# Can be overridden by command-line argument
CLEAR_PROFILE_ON_START = False

class LiveMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.last_update = self.start_time
        self.update_interval = 1  # segundos
        self.total_requests = 0
        self.valid_requests = 0
        self.ads_found = 0
        self.ads_processed = 0
        self.current_scroll = 0
        self.max_scrolls = 0
        self.last_speed = 0
        self.speed_history = []
        self.advertisers_count = Counter()
        self.last_ad_time = time.time()
        self.errors_count = 0
        
    def update_progress(self, ads_data: Dict, scroll: int = None):
        """Actualiza y muestra el progreso en tiempo real"""
        current_time = time.time()
        if current_time - self.last_update < self.update_interval:
            return
            
        self.last_update = current_time
        elapsed_time = current_time - self.start_time
        time_since_last_ad = current_time - self.last_ad_time
        
        if scroll is not None:
            self.current_scroll = scroll
            
        # Calcular velocidad de recopilación
        self.last_speed = len(ads_data) / (elapsed_time / 60) if elapsed_time > 0 else 0
        self.speed_history.append(self.last_speed)
        if len(self.speed_history) > 10:  # Mantener solo las últimas 10 mediciones
            self.speed_history.pop(0)
        
        # Limpiar líneas anteriores (5 líneas)
        for _ in range(6):
            sys.stdout.write("\033[K")  # Limpiar línea
            sys.stdout.write("\033[A")  # Mover cursor arriba
        
        # Calcular barra de progreso
        progress = self.current_scroll / self.max_scrolls if self.max_scrolls > 0 else 0
        bar_width = 40
        filled = int(bar_width * progress)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        # Formatear tiempos
        def format_time(seconds):
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            s = int(seconds % 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
        
        # Calcular velocidad promedio y estimación de tiempo restante
        avg_speed = sum(self.speed_history) / len(self.speed_history) if self.speed_history else 0
        ads_per_scroll = self.ads_processed / max(self.current_scroll, 1)
        remaining_scrolls = self.max_scrolls - self.current_scroll
        estimated_remaining_ads = remaining_scrolls * ads_per_scroll
        estimated_time_remaining = (estimated_remaining_ads / avg_speed * 60) if avg_speed > 0 else 0
        
        # Calcular tasa de éxito
        success_rate = (self.valid_requests / self.total_requests * 100) if self.total_requests > 0 else 0
        processing_rate = (self.ads_processed / self.ads_found * 100) if self.ads_found > 0 else 0
        
        # Mostrar estadísticas en tiempo real con colores
        print("\n\033[1;94m═══ MONITOREO EN TIEMPO REAL ═══\033[0m")
        print(f"\033[1mProgreso: \033[0m[{bar}] \033[1;93m{progress*100:.1f}%\033[0m ({self.current_scroll}/{self.max_scrolls})")
        print(f"\033[1mTiempo: \033[0m{format_time(elapsed_time)} transcurrido | \033[1;96mETC: {format_time(estimated_time_remaining)}\033[0m")
        print(f"\033[1mAnuncios: \033[0m{len(ads_data)} totales | \033[92m{self.ads_processed}\033[0m procesados | "
              f"\033[1;93m{self.last_speed:.1f}\033[0m/min (\033[96m{avg_speed:.1f}\033[0m prom)")
        print(f"\033[1mPeticiones: \033[0m{self.total_requests} totales | {self.valid_requests} válidas | "
              f"Éxito: \033[1;93m{success_rate:.1f}%\033[0m")
        print(f"\033[1mEstado: \033[0m{self.ads_found} encontrados | {self.ads_processed} procesados | "
              f"Tasa: \033[1;92m{processing_rate:.1f}%\033[0m | "
              f"Errores: \033[91m{self.errors_count}\033[0m")
        
        # Restaurar cursor
        sys.stdout.write("\n")
        sys.stdout.flush()
        
    def update_request_stats(self, is_valid: bool = False, ads_found: int = 0, ads_processed: int = 0):
        """Actualiza estadísticas de peticiones"""
        self.total_requests += 1
        if is_valid:
            self.valid_requests += 1
        self.ads_found += ads_found
        self.ads_processed += ads_processed
        if ads_processed > 0:
            self.last_ad_time = time.time()
            
    def increment_errors(self):
        """Incrementa el contador de errores"""
        self.errors_count += 1
        
    def set_max_scrolls(self, max_scrolls: int):
        """Establece el número máximo de scrolls"""
        self.max_scrolls = max_scrolls

class CheckpointManager:
    def __init__(self, checkpoint_dir="./checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_file = self.checkpoint_dir / "checkpoint.pkl"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.retry_count = 0
        self.max_retries = 3
        self.retry_delay = 5  # segundos
        
    async def save_checkpoint(self, state_data: dict):
        """Guarda el estado actual en un archivo de checkpoint"""
        try:
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'wb') as f:
                pickle.dump(state_data, f)
            temp_file.replace(self.checkpoint_file)
            logging.info(f"Estado guardado: {len(state_data['ads_data'])} anuncios")
        except Exception as e:
            logging.exception(f"Error saving checkpoint: {e}") # Use .exception
    
    async def load_checkpoint(self) -> dict:
        """Carga el último checkpoint guardado"""
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'rb') as f:
                    state = pickle.load(f)
                logging.info(f"Estado recuperado: {len(state['ads_data'])} anuncios")
                return state
        except Exception as e:
            logging.exception(f"Error loading checkpoint: {e}") # Use .exception
        return {'ads_data': {}, 'processed_ad_ids': set(), 'last_scroll': 0}
    
    def should_retry(self, error) -> bool:
        """Determina si se debe reintentar una operación basado en el tipo de error"""
        if self.retry_count >= self.max_retries:
            return False
            
        # Errores que merecen reintento
        retry_errors = (
            TimeoutError,
            PlaywrightTimeoutError,
            ConnectionError,
            ConnectionResetError,
        )
        
        if isinstance(error, retry_errors):
            self.retry_count += 1
            logging.warning(f"Reintento {self.retry_count}/{self.max_retries} después de error: {repr(error)}")
            return True
        return False
    
    async def handle_error(self, error, state_data: dict):
        """Maneja errores guardando el estado y determinando si reintentar"""
        await self.save_checkpoint(state_data)
        if self.should_retry(error):
            logging.info(f"Esperando {self.retry_delay}s antes de reintentar...")
            await asyncio.sleep(self.retry_delay)
            return True
        return False

class StatsManager:
    def __init__(self):
        self.start_time = time.time()
        self.advertisers = Counter()
        self.ad_types = Counter()
        self.total_ads = 0
        self.last_update_time = time.time()
        self.collection_speeds = []
        
    def update_stats(self, ad_info: dict):
        self.total_ads += 1
        self.advertisers[ad_info.get("Nombre Anunciante", "Desconocido")] += 1
        self.ad_types[ad_info.get("Formato Anuncio", "Desconocido")] += 1
        
        # Calcular velocidad de recopilación
        current_time = time.time()
        time_diff = current_time - self.last_update_time
        if time_diff >= 60:  # Actualizar estadísticas cada minuto
            ads_per_minute = self.total_ads / ((current_time - self.start_time) / 60)
            self.collection_speeds.append(ads_per_minute)
            self.last_update_time = current_time
    
    def get_summary(self) -> str:
        runtime = time.time() - self.start_time
        hours = int(runtime // 3600)
        minutes = int((runtime % 3600) // 60)
        seconds = int(runtime % 60)
        
        # Calcular velocidad promedio considerando el tiempo total
        runtime_minutes = runtime / 60
        avg_speed = self.total_ads / runtime_minutes if runtime_minutes > 0 else 0
        
        summary = [
            "\n=== RESUMEN DE LA EJECUCIÓN ===",
            f"Tiempo total de ejecución: {hours:02d}:{minutes:02d}:{seconds:02d}",
            f"Total de anuncios recopilados: {self.total_ads}",
            f"Velocidad promedio: {avg_speed:.1f} anuncios/minuto",
            "\nTop 5 Anunciantes:",
        ]
        
        for advertiser, count in self.advertisers.most_common(5):
            summary.append(f"  - {advertiser}: {count} anuncios")
            
        summary.extend([
            "\nDistribución por tipo de anuncio:",
        ])
        
        for ad_type, count in self.ad_types.most_common():
            percentage = (count / self.total_ads) * 100
            summary.append(f"  - {ad_type}: {count} ({percentage:.1f}%)")
            
        return "\n".join(summary)

# Variables globales para estadísticas
stats_manager = StatsManager()

# --- Configuración ---
TARGET_URL = "https://web.facebook.com/ads/library/?active_status=active&ad_type=all&country=MX&media_type=video&q=paga%20en%20casa&search_type=keyword_unordered&start_date[min]=2025-05-22&start_date[max]=2025-06-06"
OUTPUT_FILE = "meta_ads_data_v4.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
PERSISTENT_CONTEXT_DIR = "./fb_ad_library_profile_v4"
MANUAL_SCROLL = False  # Cambiar a False para scroll automático
MAX_SCROLLS = 2 # Temporarily set to 2 for testing
SCROLL_PAUSE_TIME = 10  # segundos entre scrolls en modo manual
SAVE_INTERVAL = 20  # Guardar cada 20 anuncios nuevos
BROWSER_HEADLESS = True

if MANUAL_SCROLL:
    BROWSER_HEADLESS = False
else: # Ensure headless is true if manual scroll is false
    BROWSER_HEADLESS = True

# Variables globales
last_save_count = 0
start_time = None
ads_data = {}
processed_ad_ids = set()
checkpoint_manager = CheckpointManager()
live_monitor = LiveMonitor()

async def response_handler(response):
    """Manejador global de respuestas para procesar peticiones GraphQL"""
    if response.url.startswith("https://www.facebook.com/api/graphql/"):
        logging.debug(f"GraphQL detectado: {response.url}")
        logging.debug(f"Método: {response.request.method}")
        logging.debug(f"Es petición válida?: {response.request.method == 'POST' and response.ok}")
        
        if not response.ok:
            # Log the full response URL if response.ok is false
            logging.warning(f"Respuesta no OK para {response.url}. Estado: {response.status}")
            live_monitor.update_request_stats()
            return
            
        try:
            # Leer la respuesta una sola vez
            json_data = await response.json()
            # Ensure pretty printing for better readability if it's a complex object
            json_preview = json.dumps(json_data, indent=2, ensure_ascii=False)[:1000] # Log more for inspection
            logging.debug(f"GraphQL response content preview: {json_preview}...") # Keep as debug, can be verbose

            edges = json_data.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {}).get("edges", [])
            if edges:
                logging.info(f"Found {len(edges)} ad edges in GraphQL response.")
                # Obtener referencias globales y procesar anuncios
                global ads_data, processed_ad_ids
                live_monitor.update_request_stats(is_valid=True, ads_found=len(edges))
                await handle_response(response, ads_data, processed_ad_ids, json_data)
            else:
                logging.info("No 'edges' found in GraphQL response. Full data structure might have changed or it's not an ad-related response.")
                # Consider logging more details from json_data if edges are consistently missing
                if "data" in json_data and "ad_library_main" not in json_data["data"]:
                    logging.debug(f"GraphQL 'data' field does not contain 'ad_library_main'. Keys: {list(json_data['data'].keys())}")
                elif "data" in json_data and "search_results_connection" not in json_data["data"].get("ad_library_main", {}):
                    logging.debug(f"GraphQL 'ad_library_main' does not contain 'search_results_connection'. Keys: {list(json_data['data'].get('ad_library_main', {}).keys())}")
                live_monitor.update_request_stats(is_valid=True) # Still a valid response, just no ads
        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON from GraphQL response: {response.url}")
            try:
                responseText = await response.text()
                logging.debug(f"Response text (first 500 chars): {responseText[:500]}")
            except Exception as e_text:
                logging.error(f"Failed to get text from response after JSON decode error: {e_text}")
            live_monitor.update_request_stats() # Count as an error / invalid request attempt
            return # Cannot proceed if JSON is invalid
        except Exception as e:
            logging.exception(f"Error processing GraphQL response: {response.url}") # Use .exception for stack trace
            live_monitor.update_request_stats() # Ensure this is called on error
            return # Return if there was an error processing the response

async def handle_response(response, ads_data: dict, processed_ad_ids: set, json_data: dict):
    """Procesa una respuesta GraphQL que contiene anuncios"""
    global last_save_count
    
    try:
        # Extraer edges directamente del json_data pasado
        edges = json_data.get("data", {}).get("ad_library_main", {}).get("search_results_connection", {}).get("edges", [])
        
        if not edges:
            return
            
        logging.debug(f"Procesando {len(edges)} edges")
        
        # 3. Procesar cada edge
        new_ads_count_in_batch = 0
        for edge in edges:
            ad_id_for_error_log = "UNKNOWN_AD_ID" # Default if ad_id cannot be retrieved
            snapshot = {} # Initialize snapshot and collated for error logging scope
            collated = {}
            try:
                node = edge.get("node", {})
                if not node:
                    logging.debug("Edge sin nodo, saltando.")
                    continue

                collated_results = node.get("collated_results", [])
                if not collated_results:
                    logging.debug("Nodo sin collated_results, saltando.")
                    continue
                
                collated = collated_results[0] # Asumimos el primer resultado
                snapshot = collated.get("snapshot", {})
                if not snapshot:
                    logging.debug("Collated result sin snapshot, saltando.")
                    continue
                
                ad_id = collated.get("ad_archive_id")
                if ad_id:
                    ad_id_for_error_log = ad_id
                else: # No ad_id, definitely skip
                    logging.debug("Collated result sin ad_archive_id, saltando.")
                    continue

                # Log the ad_id being processed
                logging.debug(f"Procesando ad_id: {ad_id}")

                if ad_id in processed_ad_ids:
                    # Log if an ad_id is skipped
                    logging.debug(f"Ad_id {ad_id} ya procesado, saltando.")
                    continue

                # --- Start of critical extraction block ---
                try:
                    processed_ad_ids.add(ad_id) # Add to processed only if we attempt full extraction
                    new_ads_count_in_batch +=1

                    # --- Extracción de datos con manejo seguro de caracteres especiales ---
                    def safe_get(obj, key, default=""): # Keep safe_get for individual field robustness
                        try:
                            value = obj.get(key, default)
                            if isinstance(value, str):
                                return value.encode('ascii', 'ignore').decode('ascii')
                            return value
                        except:
                            return default

                    page_name = safe_get(snapshot, "page_name")
                    page_like_count = snapshot.get("page_like_count")
                    is_active_status = collated.get("is_active")
                    total_active_time_value = collated.get("total_active_time")
                    publisher_platforms = collated.get("publisher_platform", [])
                    start_date_ts = collated.get("start_date")
                    page_profile_uri = safe_get(snapshot, "page_profile_uri")
                    page_profile_picture_url = safe_get(snapshot, "page_profile_picture_url")

                    body_data = snapshot.get("body", {})
                    primary_caption = safe_get(body_data, "text") if isinstance(body_data, dict) else None
                    secondary_caption = safe_get(snapshot, "link_description") or safe_get(snapshot, "caption")

                    thumbnail_url = ""
                    videos_list = snapshot.get("videos", [])
                    images_list = snapshot.get("images", [])
                    video_hd_url = ""
                    video_sd_url = ""

                    if videos_list and isinstance(videos_list, list) and videos_list:
                        first_video = videos_list[0]
                        if isinstance(first_video, dict):
                            thumbnail_url = safe_get(first_video, "video_preview_image_url")
                            video_hd_url = safe_get(first_video, "video_hd_url")
                            video_sd_url = safe_get(first_video, "video_sd_url")

                    image_url_main = ""
                    if images_list and isinstance(images_list, list) and images_list:
                        first_image = images_list[0]
                        if isinstance(first_image, dict):
                            image_url_main = safe_get(first_image, "resized_image_url") or safe_get(first_image, "original_image_url")
                            if not thumbnail_url: thumbnail_url = image_url_main

                    display_format = safe_get(snapshot, "display_format")
                    cta_text = safe_get(snapshot, "cta_text")
                    cta_type = safe_get(snapshot, "cta_type")
                    raw_cta_link = safe_get(snapshot, "link_url", "")
                    cleaned_cta_link = clean_facebook_redirect_url(raw_cta_link)

                    ad_info = {
                        "ID Anuncio (Library ID)": ad_id,
                        "Nombre Anunciante": page_name,
                        "Page Like Count": page_like_count,
                        "Is Active": is_active_status,
                        "Total Active Time": total_active_time_value,
                        "URL Perfil Anunciante": page_profile_uri,
                        "Foto Perfil Anunciante": page_profile_picture_url,
                        "Thumbnail Anuncio": thumbnail_url,
                        "Caption Principal": primary_caption,
                        "Caption Secundario": secondary_caption,
                        "URL Video HD": video_hd_url,
                        "URL Video SD": video_sd_url,
                        "URL Imagen (para anuncios de imagen)": image_url_main,
                        "Formato Anuncio": display_format,
                        "Fecha Inicio (timestamp)": start_date_ts,
                        "Texto CTA": cta_text,
                        "Tipo CTA": cta_type,
                        "URL Destino CTA (limpia)": cleaned_cta_link,
                        "Plataformas": publisher_platforms
                    }

                    stats_manager.update_stats(ad_info)
                    logging.debug(f"Ad processed successfully: {ad_id}")
                    ads_data[ad_id] = ad_info

                except (KeyError, TypeError, AttributeError) as e_extract:
                    logging.error(f"Extraction error for ad_id '{ad_id_for_error_log}': {type(e_extract).__name__}: {e_extract}. Problematic snapshot/collated data snippet below.")
                    logging.debug(f"Snapshot data for ad_id '{ad_id_for_error_log}': {str(snapshot)[:500]}...")
                    logging.debug(f"Collated data for ad_id '{ad_id_for_error_log}': {str(collated)[:500]}...")
                    live_monitor.increment_errors() # Ensure error is counted
                    # new_ads_count_in_batch was incremented, but this ad failed.
                    # It's simpler to potentially misreport slightly on batch count in monitor
                    # than to decrement here or manage complex state.
                    # The processed_ad_ids.add was also done, which is fine, it means we won't retry this problematic ad.
                    continue # Continue to the next edge
                # --- End of critical extraction block ---
                
                # Guardar incrementalmente
                if len(ads_data) - last_save_count >= SAVE_INTERVAL:
                    await save_data(ads_data)
                    last_save_count = len(ads_data)

            except Exception as e_outer: # Catch any other unexpected error for an edge
                logging.exception(f"Outer error processing edge for ad_id '{ad_id_for_error_log}': {e_outer}")
                live_monitor.increment_errors()
                continue # Continue to the next edge
        
        if new_ads_count_in_batch > 0: # This count might be slightly off if errors occurred mid-batch
            live_monitor.update_request_stats(ads_processed=new_ads_count_in_batch)
            live_monitor.update_progress(ads_data)

    except Exception as ex: # This is for errors in the overall edge processing setup, not individual ads
        logging.exception(f"Critical error in handle_response structure for URL {response.url}: {ex}")
        live_monitor.increment_errors()

async def save_data(ads_data: dict):
    try:
        temp_file = f"{OUTPUT_FILE}.temp"
        with open(temp_file, 'w', encoding='utf-8', errors='ignore') as f:
            json.dump(list(ads_data.values()), f, ensure_ascii=False, indent=4)
        os.replace(temp_file, OUTPUT_FILE)
        logging.info(f"Guardados {len(ads_data)} anuncios en {OUTPUT_FILE}")
    except Exception as e:
        logging.exception(f"Failed to save data to {OUTPUT_FILE}: {e}")

# --- Funciones Auxiliares ---
def clean_facebook_redirect_url(url: str) -> str:
    if not url: return ""
    try:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        if "u" in qs and qs["u"]: return qs["u"][0]
        return url
    except Exception: return url

# --- Función Principal ---
async def main():
    global start_time, ads_data, processed_ad_ids, last_save_count, PERSISTENT_CONTEXT_DIR
    start_time = time.time()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f"Target URL for scraping: {TARGET_URL}")

    parser = argparse.ArgumentParser(description="Scrape Facebook Ads Library.")
    parser.add_argument(
        "--clear-profile",
        action="store_true",
        help="Clear the persistent browser profile directory before starting."
    )
    args = parser.parse_args()

    if args.clear_profile:
        if os.path.exists(PERSISTENT_CONTEXT_DIR):
            logging.info(f"Command-line argument --clear-profile received. Deleting directory: {PERSISTENT_CONTEXT_DIR}")
            try:
                shutil.rmtree(PERSISTENT_CONTEXT_DIR)
                logging.info(f"Successfully deleted directory: {PERSISTENT_CONTEXT_DIR}")
                # Recreate the directory after deletion as Playwright expects it to exist
                os.makedirs(PERSISTENT_CONTEXT_DIR, exist_ok=True)
            except Exception as e:
                logging.exception(f"Failed to delete directory {PERSISTENT_CONTEXT_DIR}. Please clear it manually if issues persist.")
        else:
            logging.info(f"--clear-profile specified, but directory {PERSISTENT_CONTEXT_DIR} does not exist. Proceeding.")
    
    # Intentar cargar desde checkpoint
    state = await checkpoint_manager.load_checkpoint()
    ads_data = state['ads_data']
    processed_ad_ids = state['processed_ad_ids']
    last_scroll = state.get('last_scroll', 0)
    last_save_count = len(ads_data)
    
    # Configurar monitor
    live_monitor.set_max_scrolls(MAX_SCROLLS)
    
    # Cargar datos adicionales del archivo JSON si existen
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                for ad in existing_data:
                    ad_id = ad.get("ID Anuncio (Library ID)")
                    if ad_id and ad_id not in processed_ad_ids:
                        ads_data[ad_id] = ad
                        processed_ad_ids.add(ad_id)
            logging.info(f"Cargados {len(ads_data)} anuncios existentes de {OUTPUT_FILE}")
        except Exception as e:
            logging.warning(f"Error cargando datos existentes: {e}")
            live_monitor.increment_errors()

    if not os.path.exists(PERSISTENT_CONTEXT_DIR):
        os.makedirs(PERSISTENT_CONTEXT_DIR)

    while True:  # Loop principal para reintentos
        try:
            async with async_playwright() as p:
                logging.info(f"Lanzando navegador con perfil: {PERSISTENT_CONTEXT_DIR}")
                context = await p.firefox.launch_persistent_context(
                    PERSISTENT_CONTEXT_DIR, 
                    headless=BROWSER_HEADLESS, 
                    user_agent=USER_AGENT,
                    viewport={'width': 1920, 'height': 1080},
                    args=["--disable-blink-features=AutomationControlled"], 
                    locale="es-ES",
                )
                page = await context.new_page()
                
                # Configurar el manejador de respuestas antes de navegar
                page.on("response", lambda response: asyncio.create_task(response_handler(response)))
                
                logging.info(f"Navegando a: {TARGET_URL}")
                try:
                    await page.goto(TARGET_URL, wait_until="networkidle", timeout=90000)
                    logging.info("Página principal cargada.")
                except Exception as e:
                    logging.error(f"Navegando a la página: {e}")
                    live_monitor.increment_errors()
                    if not await checkpoint_manager.handle_error(e, {
                        'ads_data': ads_data,
                        'processed_ad_ids': processed_ad_ids,
                        'last_scroll': last_scroll
                    }):
                        await context.close()
                        break
                    continue

                # Manejar cookies si es necesario
                logging.info("Attempting to find cookie consent button with selectors: 'button:has-text(\"Permitir todas las cookies\")' or 'button:has-text(\"Allow all cookies\")'")
                try:
                    cookie_selector = 'button:has-text("Permitir todas las cookies"), button:has-text("Allow all cookies")'
                    logging.info(f"Looking for cookie button with selector: {cookie_selector}")
                    btn_cookies = page.locator(cookie_selector).first
                    if await btn_cookies.is_visible(timeout=7000):
                        logging.info("Cookie consent button found and visible. Attempting to click.")
                        await btn_cookies.click(timeout=5000)
                        logging.info("Cookie consent button clicked.")
                        await page.wait_for_timeout(2000) # Wait for any overlays to disappear
                    else:
                        logging.info("Cookie consent button not found or not visible within timeout.")
                except PlaywrightTimeoutError:
                    logging.warning("Timeout while trying to find or click the cookie consent button.")
                except Exception as e:
                    logging.warning(f"Could not click cookie consent button: {e}")

                # Iniciar proceso de scroll
                logging.info("Iniciando proceso de scroll...")
                initial_ad_count = len(ads_data)
                scroll_count = last_scroll  # Comenzar desde el último scroll guardado

                if MANUAL_SCROLL:
                    logging.info(f"Iniciando scroll manual infinito cada {SCROLL_PAUSE_TIME}s...")
                    prev_ads_count = len(ads_data)
                    no_new_scrolls = 0
                    threshold = 3
                    while True:
                        ads_before_scroll = len(ads_data)
                        logging.info(f"Manual scroll {scroll_count + 1}: Ads count before scroll: {ads_before_scroll}")
                        # mostrar progreso actual
                        live_monitor.update_progress(ads_data, scroll_count)
                        # cuenta regresiva antes del siguiente scroll
                        for sec in range(int(SCROLL_PAUSE_TIME), 0, -1):
                            sys.stdout.write(f"\rPróximo scroll en {sec}s...")
                            sys.stdout.flush()
                            await asyncio.sleep(1)
                        # limpiar línea de cuenta regresiva
                        sys.stdout.write("\r" + " " * 30 + "\r")
                        # realizar scroll
                        await page.evaluate("window.scrollBy(0, window.innerHeight * 0.9)")
                        scroll_count += 1
                        # guardar checkpoint después de cada scroll
                        await checkpoint_manager.save_checkpoint({
                            'ads_data': ads_data,
                            'processed_ad_ids': processed_ad_ids,
                            'last_scroll': scroll_count
                        })
                        logging.debug(f"Scroll {scroll_count} realizado. Esperando {SCROLL_PAUSE_TIME}s para carga de anuncios.")
                        # espera para procesar posibles respuestas
                        await asyncio.sleep(SCROLL_PAUSE_TIME) # Short wait for ads to load after scroll
                        ads_after_scroll = len(ads_data)
                        logging.info(f"Manual scroll {scroll_count}: Ads count after scroll and processing: {ads_after_scroll}. New ads from this scroll: {ads_after_scroll - ads_before_scroll}")
                        if ads_after_scroll > prev_ads_count: # Use ads_after_scroll for comparison
                            prev_ads_count = ads_after_scroll
                            no_new_scrolls = 0
                        else:
                            no_new_scrolls += 1
                        if no_new_scrolls >= threshold:
                            logging.info(f"No se encontraron anuncios nuevos tras {threshold} scrolls. Finalizando scroll manual.")
                            break
                    logging.info(f"Scroll manual infinito finalizado después de {scroll_count} scrolls.")
                else:
                    logging.info(f"Iniciando scroll automático (hasta {MAX_SCROLLS} intentos)...")
                    for i in range(scroll_count, MAX_SCROLLS):
                        ads_before_scroll = len(ads_data)
                        # Use (i + 1) for 1-based scroll attempt logging if scroll_count starts at 0
                        logging.info(f"Automatic scroll attempt {i + 1}/{MAX_SCROLLS}: Ads count before scroll: {ads_before_scroll}")
                        try:
                            await page.evaluate("window.scrollBy(0, window.innerHeight * 0.9)")
                            scroll_count += 1
                            live_monitor.update_progress(ads_data, scroll_count)
                            logging.debug(f"Scroll {scroll_count} realizado. Esperando {SCROLL_PAUSE_TIME}s para carga de anuncios.")
                            
                            if scroll_count % 5 == 0 or scroll_count == MAX_SCROLLS:
                                # Guardar checkpoint cada 5 scrolls
                                await checkpoint_manager.save_checkpoint({
                                    'ads_data': ads_data,
                                    'processed_ad_ids': processed_ad_ids,
                                    'last_scroll': scroll_count
                                })
                            await asyncio.sleep(SCROLL_PAUSE_TIME) # Short wait for ads to load
                            ads_after_scroll = len(ads_data)
                            # scroll_count is incremented after evaluate, so it's current for this log
                            logging.info(f"Automatic scroll attempt {scroll_count}/{MAX_SCROLLS}: Ads count after scroll and processing: {ads_after_scroll}. New ads from this scroll: {ads_after_scroll - ads_before_scroll}")
                        except Exception as e:
                            logging.exception(f"Error durante el scroll automático #{i + 1}:")
                            live_monitor.increment_errors()
                            if not await checkpoint_manager.handle_error(e, {
                                'ads_data': ads_data,
                                'processed_ad_ids': processed_ad_ids,
                                'last_scroll': scroll_count
                            }):
                                raise
                
                logging.info(f"Scroll finalizado después de {scroll_count} scrolls.")
                new_ads_after_scroll = len(ads_data) - initial_ad_count
                if new_ads_after_scroll > 0:
                    logging.info(f"Se encontraron {new_ads_after_scroll} anuncios adicionales durante el scroll.")
                
                logging.info("Esperando 5s para procesar peticiones finales...")
                await asyncio.sleep(5)
                
                logging.info(f"Cerrando navegador. Total anuncios únicos: {len(ads_data)}")
                await context.close()
                break  # Salir del loop de reintentos si todo fue exitoso
                
        except Exception as e:
            logging.exception("Excepción en el loop principal:")
            live_monitor.increment_errors()
            if not await checkpoint_manager.handle_error(e, {
                'ads_data': ads_data,
                'processed_ad_ids': processed_ad_ids,
                'last_scroll': scroll_count if 'scroll_count' in locals() else 0
            }):
                logging.exception(f"FATAL ERROR in main execution after failing retry attempts: {e}")
                break
    
    if ads_data:
        await save_data(ads_data)
        # The StatsManager.get_summary() uses print for a final report, which is acceptable.
        print("\n" + stats_manager.get_summary())
    else:
        logging.warning("No se recopilaron datos de anuncios.")

if __name__ == "__main__":
    asyncio.run(main())