import json
import re
import asyncio
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import os
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from time import sleep

class FacebookAdsLibraryScraper:
    def __init__(self):
        self.ads_data = []
        self.captured_requests = []
        
    def clean_url(self, url):
        """Limpia la URL removiendo parámetros innecesarios"""
        if not url:
            return ""
        
        try:
            parsed = urlparse(url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            return clean_url
        except:
            return url
    
    def get_media_info(self, cards):
        """Extrae información de medios (imagen/video) de las cards"""
        video_hd_url = ""
        video_sd_url = ""
        image_url = ""
        thumbnail_url = ""
        
        if cards and len(cards) > 0:
            first_card = cards[0]
            video_hd_url = first_card.get('video_hd_url', '')
            video_sd_url = first_card.get('video_sd_url', '')
            
            # Para imágenes
            if first_card.get('resized_image_url'):
                image_url = first_card.get('resized_image_url', '')
            elif first_card.get('original_image_url'):
                image_url = first_card.get('original_image_url', '')
                
            # Para thumbnail
            thumbnail_url = first_card.get('video_preview_image_url', '')
            if not thumbnail_url:
                thumbnail_url = image_url
                
        return video_hd_url, video_sd_url, image_url, thumbnail_url
    
    def determine_ad_format(self, cards, images, videos):
        """Determina el formato del anuncio"""
        if cards and len(cards) > 0:
            first_card = cards[0]
            if first_card.get('video_hd_url') or first_card.get('video_sd_url'):
                return "VIDEO"
            elif first_card.get('resized_image_url') or first_card.get('original_image_url'):
                return "IMAGEN"
        
        if videos and len(videos) > 0:
            return "VIDEO"
        elif images and len(images) > 0:
            return "IMAGEN"
        
        return "DESCONOCIDO"
    
    def extract_ad_data(self, response_data):
        """Extrae los datos específicos de cada anuncio"""
        ads = []
        
        try:
            data = response_data.get('data', {})
            ad_library_main = data.get('ad_library_main', {})
            search_results = ad_library_main.get('search_results_connection', {})
            edges = search_results.get('edges', [])
            
            for edge in edges:
                node = edge.get('node', {})
                collated_results = node.get('collated_results', [])
                
                for result in collated_results:
                    try:
                        snapshot = result.get('snapshot', {})
                        cards = snapshot.get('cards', [])
                        
                        # Extraer información de medios
                        video_hd_url, video_sd_url, image_url, thumbnail_url = self.get_media_info(cards)
                        
                        # Determinar formato
                        ad_format = self.determine_ad_format(
                            cards, 
                            snapshot.get('images', []), 
                            snapshot.get('videos', [])
                        )
                        
                        # Extraer caption principal y secundario
                        caption_principal = ""
                        caption_secundario = snapshot.get('caption', '')
                        
                        if cards and len(cards) > 0:
                            caption_principal = cards[0].get('body', '')
                        
                        # Si no hay caption principal, usar el body del snapshot
                        if not caption_principal and snapshot.get('body'):
                            caption_principal = snapshot['body'].get('text', '')
                        
                        ad_data = {
                            "ID Anuncio (Library ID)": result.get('ad_archive_id', ''),
                            "Nombre Anunciante": result.get('page_name', ''),
                            "Page Like Count": snapshot.get('page_like_count', 0),
                            "Is Active": result.get('is_active', False),
                            "Total Active Time": result.get('total_active_time', 0),
                            "URL Perfil Anunciante": snapshot.get('page_profile_uri', ''),
                            "Foto Perfil Anunciante": snapshot.get('page_profile_picture_url', ''),
                            "Thumbnail Anuncio": thumbnail_url,
                            "Caption Principal": caption_principal,
                            "Caption Secundario": caption_secundario,
                            "URL Video HD": video_hd_url,
                            "URL Video SD": video_sd_url,
                            "URL Imagen (para anuncios de imagen)": image_url,
                            "Formato Anuncio": ad_format,
                            "Fecha Inicio (timestamp)": result.get('start_date', 0),
                            "Texto CTA": snapshot.get('cta_text', ''),
                            "Tipo CTA": snapshot.get('cta_type', ''),
                            "URL Destino CTA (limpia)": self.clean_url(snapshot.get('link_url', '')),
                            "Plataformas": result.get('publisher_platform', [])
                        }
                        
                        ads.append(ad_data)
                        
                    except Exception as e:
                        print(f"Error procesando anuncio individual: {e}")
                        continue
                        
        except Exception as e:
            print(f"Error procesando respuesta: {e}")
            
        return ads
    
    async def setup_request_handler(self, page, output_file):
        """Configura el manejador para capturar peticiones GraphQL y guardar datos progresivamente"""

        async def handle_response(response):
            try:
                # Filtrar solo peticiones GraphQL que contengan ad_archive_id
                if 'graphql' in response.url.lower():
                    response_text = await response.text()
                    if 'ad_archive_id' in response_text:
                        print(f"📥 Capturada petición GraphQL con ads: {response.url[:100]}...")

                        try:
                            response_data = json.loads(response_text)
                            ads = self.extract_ad_data(response_data)
                            if ads:
                                self.ads_data.extend(ads)
                                print(f"   ✅ {len(ads)} anuncios extraídos de esta petición")

                                # Guardar datos progresivamente
                                self.save_to_json(output_file)
                        except json.JSONDecodeError:
                            print(f"   ⚠️ Error decodificando JSON de la respuesta")
                        except Exception as e:
                            print(f"   ⚠️ Error procesando respuesta: {e}")

            except Exception as e:
                pass  # Ignorar errores de respuestas que no podemos leer

        page.on('response', handle_response)
    async def smart_scroll(self, page, max_scrolls=50, scroll_delay=2):
        """Hace scroll inteligente hasta completar todos los scrolls programados"""
        print(f"🔄 Iniciando scroll automático ({max_scrolls} scrolls programados)...")
        
        last_ads_count = 0
        consecutive_same_count = 0
        
        for i in range(max_scrolls):
            try:
                current_ads_count = len(self.ads_data)
                
                # Hacer scroll más agresivo
                await page.evaluate("""
                    window.scrollTo({
                        top: document.documentElement.scrollHeight + 1000,
                        behavior: 'auto'
                    });
                """)
                
                # Esperar a que el contenido cargue
                await asyncio.sleep(scroll_delay)
                
                # Verificar si hay nuevos anuncios
                if current_ads_count > last_ads_count:
                    print(f"   📊 Scroll {i+1}/{max_scrolls}: {current_ads_count - last_ads_count} nuevos anuncios (Total: {current_ads_count})")
                    last_ads_count = current_ads_count
                    consecutive_same_count = 0
                else:
                    consecutive_same_count += 1
                    print(f"   ⏳ Scroll {i+1}/{max_scrolls}: Buscando nuevos anuncios... ({consecutive_same_count} intentos sin cambios)")
                    
                    # Si llevamos tres intentos sin anuncios nuevos, ejecutar scroll en sacudidas
                    if consecutive_same_count == 3:
                        print("   🔄 Ejecutando scroll en sacudidas por 9 segundos...")
                        await page.evaluate("""
                            (function() {
                                // --- Parámetros que puedes ajustar ---

                                // Píxeles para el scroll suave inicial hacia arriba
                                const scrollInicialHaciaArriba = 1200;

                                // Duración total del efecto de sacudida en milisegundos (5000ms = 5 segundos)
                                const duracionTotalSacudida = 9000;

                                // Amplitud de la sacudida en píxeles (qué tan "fuerte" es el movimiento)
                                const amplitud = 90;

                                // Frecuencia de la sacudida en milisegundos (un número más bajo es más rápido)
                                const frecuencia = 200;

                                // --- Lógica del script (no necesitas modificar esto) ---

                                // 1. Realizar el scroll inicial hacia arriba
                                console.log(`Desplazando la vista ${scrollInicialHaciaArriba}px hacia arriba...`);
                                window.scrollBy({
                                    top: -scrollInicialHaciaArriba, // Negativo para subir
                                    left: 0,
                                    behavior: 'smooth'
                                });

                                // 2. Esperar a que el scroll suave termine y luego iniciar la sacudida
                                const retrasoAntesDeSacudir = 700; // Pausa en milisegundos

                                setTimeout(() => {
                                    // Esta es la lógica de la sacudida, que ahora se ejecuta después de la pausa
                                    console.log(`Iniciando sacudida por ${duracionTotalSacudida / 1000} segundos...`);

                                    let direccion = 1; // 1 para bajar, -1 para subir
                                    const efectoSacudida = () => {
                                        window.scrollBy(0, amplitud * direccion);
                                        direccion *= -1;
                                    };

                                    const intervalo = setInterval(efectoSacudida, frecuencia);

                                    setTimeout(() => {
                                        clearInterval(intervalo);
                                        console.log("Efecto finalizado.");
                                    }, duracionTotalSacudida);

                                }, retrasoAntesDeSacudir);

                            })();
                        """)
                        sleep(5)  # Esperar 5 segundos para que la sacudida se complete
                        print("   🔄 Sacudida completada, continuando con el scroll...")
                        consecutive_same_count = 0  # Reiniciar contador después de la sacudida
                
                # Click en cualquier botón de "Ver más" o "Mostrar más"
                try:
                    for text in ["Ver más", "Mostrar más", "See More", "Show More", "Load More"]:
                        button = page.locator(f'text="{text}" >> visible=true')
                        if await button.count() > 0:
                            await button.first.click()
                            print(f"   👆 Click en botón '{text}'")
                            await asyncio.sleep(2)
                except:
                    pass
                    
            except Exception as e:
                print(f"   ⚠️ Error durante el scroll {i+1}: {e}")
                continue
        
        print(f"✅ Scrolling completado: {max_scrolls} scrolls realizados. Total de anuncios: {len(self.ads_data)}")
    
    async def scrape_facebook_ads(self, url, headless=True, max_scrolls=50):
        """Scrape Facebook Ads Library usando Playwright"""
        print("🚀 Iniciando scraping de Facebook Ads Library...")
        print(f"🌐 URL objetivo: {url}")

        # Preguntar al usuario dónde guardar los datos
        output_file = input("💾 Ingresa el nombre del archivo de salida (default: facebook_ads_data.json): ").strip()
        if not output_file:
            output_file = 'facebook_ads_data.json'
        print(f"📂 Los datos se guardarán en: {output_file}")

        async with async_playwright() as p:
            # Configurar navegador
            browser = await p.chromium.launch(
                headless=headless,
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-dev-shm-usage'
                ]
            )
            
            # Crear contexto con user agent real
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = await context.new_page()
            
            try:
                # Configurar captura de peticiones con guardado progresivo
                await self.setup_request_handler(page, output_file)

                # Navegar a la página con timeout más largo
                print("📱 Navegando a Facebook Ads Library...")
                await page.goto(url, wait_until='load', timeout=60000)
                
                # Esperar a que aparezcan los elementos principales
                print("⌛ Esperando a que cargue la página...")
                try:
                    await page.wait_for_selector('[role="main"]', timeout=30000)
                except:
                    print("⚠️ No se detectó el contenedor principal, pero continuamos...")
                
                # Esperar más tiempo para asegurar que todo cargue
                await asyncio.sleep(5)
                
                # Verificar si necesitamos aceptar cookies o hacer login
                try:
                    # Buscar y hacer clic en botón de aceptar cookies si existe
                    cookie_button = page.locator('button:has-text("Allow all cookies"), button:has-text("Accept all"), button:has-text("Aceptar todo")')
                    if await cookie_button.count() > 0:
                        await cookie_button.first.click()
                        print("🍪 Cookies aceptadas")
                        await asyncio.sleep(2)
                except:
                    pass
                
                # Hacer scroll inteligente
                await self.smart_scroll(page, max_scrolls=max_scrolls)

                # Esperar un poco más para capturar últimas peticiones
                await asyncio.sleep(3)
                
            except PlaywrightTimeoutError:
                print("⚠️ Timeout al cargar la página, pero continuamos con los datos capturados...")
            except Exception as e:
                print(f"❌ Error durante el scraping: {e}")
            finally:
                await browser.close()

    def save_to_json(self, output_file):
        """Guarda los datos extraídos en un archivo JSON progresivamente"""
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(self.ads_data, file, ensure_ascii=False, indent=2)
            print(f"💾 Datos guardados progresivamente en: {output_file}")
        except Exception as e:
            print(f"❌ Error guardando archivo: {e}")
    
    def get_summary(self):
        """Devuelve un resumen de los datos extraídos"""
        if not self.ads_data:
            return "No hay datos para mostrar"
        
        total_ads = len(self.ads_data)
        active_ads = sum(1 for ad in self.ads_data if ad['Is Active'])
        
        formats = {}
        platforms = {}
        advertisers = {}
        
        for ad in self.ads_data:
            # Contar formatos
            ad_format = ad['Formato Anuncio']
            formats[ad_format] = formats.get(ad_format, 0) + 1
            
            # Contar plataformas
            for platform in ad['Plataformas']:
                platforms[platform] = platforms.get(platform, 0) + 1
            
            # Contar anunciantes
            advertiser = ad['Nombre Anunciante']
            advertisers[advertiser] = advertisers.get(advertiser, 0) + 1
        
        summary = f"""
📊 RESUMEN DE DATOS EXTRAÍDOS:
==========================
📈 Total de anuncios: {total_ads}
✅ Anuncios activos: {active_ads}
❌ Anuncios inactivos: {total_ads - active_ads}

🎨 FORMATOS:
{json.dumps(formats, indent=2, ensure_ascii=False)}

📱 PLATAFORMAS MÁS UTILIZADAS:
{json.dumps(dict(sorted(platforms.items(), key=lambda x: x[1], reverse=True)[:5]), indent=2, ensure_ascii=False)}

🏢 TOP 5 ANUNCIANTES:
{json.dumps(dict(sorted(advertisers.items(), key=lambda x: x[1], reverse=True)[:5]), indent=2, ensure_ascii=False)}
        """
        
        return summary

# Clase para mantener compatibilidad con archivos HAR existentes
class FacebookAdsHARParser:
    def __init__(self, har_file_path):
        self.har_file_path = har_file_path
        self.ads_data = []
        
    def load_har_file(self):
        """Carga el archivo HAR"""
        try:
            with open(self.har_file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            print(f"Error al cargar el archivo HAR: {e}")
            return None
    
    def filter_graphql_requests(self, har_data):
        """Filtra peticiones que contengan 'graphql' en la URL"""
        graphql_requests = []
        
        if 'log' not in har_data or 'entries' not in har_data['log']:
            print("Estructura de HAR inválida")
            return graphql_requests
            
        for entry in har_data['log']['entries']:
            if 'request' in entry and 'url' in entry['request']:
                url = entry['request']['url']
                if 'graphql' in url.lower():
                    graphql_requests.append(entry)
                    
        print(f"Se encontraron {len(graphql_requests)} peticiones GraphQL")
        return graphql_requests
    
    def filter_ad_archive_responses(self, graphql_requests):
        """Filtra peticiones que contengan 'ad_archive_id' en la respuesta"""
        ad_requests = []
        
        for request in graphql_requests:
            if 'response' in request and 'content' in request['response']:
                try:
                    response_text = request['response']['content'].get('text', '')
                    if response_text and 'ad_archive_id' in response_text:
                        ad_requests.append(request)
                except Exception as e:
                    continue
                    
        print(f"Se encontraron {len(ad_requests)} peticiones con ad_archive_id")
        return ad_requests
    
    def clean_url(self, url):
        """Limpia la URL removiendo parámetros innecesarios"""
        if not url:
            return ""
        
        try:
            parsed = urlparse(url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            return clean_url
        except:
            return url
    
    def get_media_info(self, cards):
        """Extrae información de medios (imagen/video) de las cards"""
        video_hd_url = ""
        video_sd_url = ""
        image_url = ""
        thumbnail_url = ""
        
        if cards and len(cards) > 0:
            first_card = cards[0]
            video_hd_url = first_card.get('video_hd_url', '')
            video_sd_url = first_card.get('video_sd_url', '')
            
            # Para imágenes
            if first_card.get('resized_image_url'):
                image_url = first_card.get('resized_image_url', '')
            elif first_card.get('original_image_url'):
                image_url = first_card.get('original_image_url', '')
                
            # Para thumbnail
            thumbnail_url = first_card.get('video_preview_image_url', '')
            if not thumbnail_url:
                thumbnail_url = image_url
                
        return video_hd_url, video_sd_url, image_url, thumbnail_url
    
    def determine_ad_format(self, cards, images, videos):
        """Determina el formato del anuncio"""
        if cards and len(cards) > 0:
            first_card = cards[0]
            if first_card.get('video_hd_url') or first_card.get('video_sd_url'):
                return "VIDEO"
            elif first_card.get('resized_image_url') or first_card.get('original_image_url'):
                return "IMAGEN"
        
        if videos and len(videos) > 0:
            return "VIDEO"
        elif images and len(images) > 0:
            return "IMAGEN"
        
        return "DESCONOCIDO"
    
    def extract_ad_data(self, response_data):
        """Extrae los datos específicos de cada anuncio"""
        ads = []
        
        try:
            data = response_data.get('data', {})
            ad_library_main = data.get('ad_library_main', {})
            search_results = ad_library_main.get('search_results_connection', {})
            edges = search_results.get('edges', [])
            
            for edge in edges:
                node = edge.get('node', {})
                collated_results = node.get('collated_results', [])
                
                for result in collated_results:
                    try:
                        snapshot = result.get('snapshot', {})
                        cards = snapshot.get('cards', [])
                        
                        # Extraer información de medios
                        video_hd_url, video_sd_url, image_url, thumbnail_url = self.get_media_info(cards)
                        
                        # Determinar formato
                        ad_format = self.determine_ad_format(
                            cards, 
                            snapshot.get('images', []), 
                            snapshot.get('videos', [])
                        )
                        
                        # Extraer caption principal y secundario
                        caption_principal = ""
                        caption_secundario = snapshot.get('caption', '')
                        
                        if cards and len(cards) > 0:
                            caption_principal = cards[0].get('body', '')
                        
                        # Si no hay caption principal, usar el body del snapshot
                        if not caption_principal and snapshot.get('body'):
                            caption_principal = snapshot['body'].get('text', '')
                        
                        ad_data = {
                            "ID Anuncio (Library ID)": result.get('ad_archive_id', ''),
                            "Nombre Anunciante": result.get('page_name', ''),
                            "Page Like Count": snapshot.get('page_like_count', 0),
                            "Is Active": result.get('is_active', False),
                            "Total Active Time": result.get('total_active_time', 0),
                            "URL Perfil Anunciante": snapshot.get('page_profile_uri', ''),
                            "Foto Perfil Anunciante": snapshot.get('page_profile_picture_url', ''),
                            "Thumbnail Anuncio": thumbnail_url,
                            "Caption Principal": caption_principal,
                            "Caption Secundario": caption_secundario,
                            "URL Video HD": video_hd_url,
                            "URL Video SD": video_sd_url,
                            "URL Imagen (para anuncios de imagen)": image_url,
                            "Formato Anuncio": ad_format,
                            "Fecha Inicio (timestamp)": result.get('start_date', 0),
                            "Texto CTA": snapshot.get('cta_text', ''),
                            "Tipo CTA": snapshot.get('cta_type', ''),
                            "URL Destino CTA (limpia)": self.clean_url(snapshot.get('link_url', '')),
                            "Plataformas": result.get('publisher_platform', [])
                        }
                        
                        ads.append(ad_data)
                        
                    except Exception as e:
                        print(f"Error procesando anuncio individual: {e}")
                        continue
                        
        except Exception as e:
            print(f"Error procesando respuesta: {e}")
            
        return ads
    
    def process_har_file(self):
        """Procesa todo el archivo HAR y extrae los datos de anuncios"""
        print("Iniciando procesamiento del archivo HAR...")
        
        # Cargar archivo HAR
        har_data = self.load_har_file()
        if not har_data:
            return False
        
        # Filtrar peticiones GraphQL
        graphql_requests = self.filter_graphql_requests(har_data)
        if not graphql_requests:
            print("No se encontraron peticiones GraphQL")
            return False
        
        # Filtrar peticiones con ad_archive_id
        ad_requests = self.filter_ad_archive_responses(graphql_requests)
        if not ad_requests:
            print("No se encontraron peticiones con ad_archive_id")
            return False
        
        # Procesar cada respuesta
        total_ads = 0
        for i, request in enumerate(ad_requests):
            try:
                response_text = request['response']['content']['text']
                response_data = json.loads(response_text)
                
                ads = self.extract_ad_data(response_data)
                self.ads_data.extend(ads)
                total_ads += len(ads)
                
                print(f"Procesada petición {i+1}/{len(ad_requests)}: {len(ads)} anuncios encontrados")
                
            except Exception as e:
                print(f"Error procesando petición {i+1}: {e}")
                continue
        
        print(f"Procesamiento completado. Total de anuncios extraídos: {total_ads}")
        return True
    
    def save_to_json(self, output_file='facebook_ads_data.json'):
        """Guarda los datos extraídos en un archivo JSON"""
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(self.ads_data, file, ensure_ascii=False, indent=2)
            print(f"Datos guardados en: {output_file}")
            return True
        except Exception as e:
            print(f"Error guardando archivo: {e}")
            return False
    
    def get_summary(self):
        """Devuelve un resumen de los datos extraídos"""
        if not self.ads_data:
            return "No hay datos para mostrar"
        
        total_ads = len(self.ads_data)
        active_ads = sum(1 for ad in self.ads_data if ad['Is Active'])
        
        formats = {}
        platforms = {}
        advertisers = {}
        
        for ad in self.ads_data:
            # Contar formatos
            ad_format = ad['Formato Anuncio']
            formats[ad_format] = formats.get(ad_format, 0) + 1
            
            # Contar plataformas
            for platform in ad['Plataformas']:
                platforms[platform] = platforms.get(platform, 0) + 1
            
            # Contar anunciantes
            advertiser = ad['Nombre Anunciante']
            advertisers[advertiser] = advertisers.get(advertiser, 0) + 1
        
        summary = f"""
RESUMEN DE DATOS EXTRAÍDOS:
==========================
Total de anuncios: {total_ads}
Anuncios activos: {active_ads}
Anuncios inactivos: {total_ads - active_ads}

FORMATOS:
{json.dumps(formats, indent=2, ensure_ascii=False)}

PLATAFORMAS MÁS UTILIZADAS:
{json.dumps(dict(sorted(platforms.items(), key=lambda x: x[1], reverse=True)[:5]), indent=2, ensure_ascii=False)}

TOP 5 ANUNCIANTES:
{json.dumps(dict(sorted(advertisers.items(), key=lambda x: x[1], reverse=True)[:5]), indent=2, ensure_ascii=False)}
        """
        
        return summary

async def main():
    """Función principal"""
    print("🚀 === FACEBOOK ADS LIBRARY SCRAPER ===")
    print()
    
    print("Selecciona el modo de operación:")
    print("1. 🌐 Scraping automático con Playwright (URL de Facebook Ads Library)")
    print("2. 📁 Procesar archivo HAR existente")
    
    mode = input("\nIngresa tu opción (1 o 2): ").strip()
    
    if mode == "1":
        # Modo scraping automático
        url = input("\n🌐 Ingresa la URL de Facebook Ads Library: ").strip()
        
        if not url or 'facebook.com' not in url:
            print("❌ URL inválida. Debe ser una URL de Facebook Ads Library.")
            return
        
        # Configuraciones del scraping
        print("\n⚙️ Configuraciones del scraping:")
        headless_input = input("¿Ejecutar en modo headless? (s/N): ").strip().lower()
        headless = headless_input in ['s', 'si', 'sí', 'y', 'yes']
        
        max_scrolls_input = input("Máximo número de scrolls (default: 50): ").strip()
        try:
            max_scrolls = int(max_scrolls_input) if max_scrolls_input else 50
        except ValueError:
            max_scrolls = 50
        
        # Crear scraper y ejecutar
        scraper = FacebookAdsLibraryScraper()
        
        try:
            await scraper.scrape_facebook_ads(url, headless=headless, max_scrolls=max_scrolls)
            
            if scraper.ads_data:
                # Mostrar resumen
                print(scraper.get_summary())
                
                # Guardar datos
                output_file = input("\n💾 Nombre del archivo de salida (default: facebook_ads_data.json): ").strip()
                if not output_file:
                    output_file = 'facebook_ads_data.json'
                
                if scraper.save_to_json(output_file):
                    print(f"\n✅ Proceso completado exitosamente!")
                    print(f"📄 Archivo guardado: {output_file}")
                    print(f"📊 Total de anuncios extraídos: {len(scraper.ads_data)}")
                else:
                    print("❌ Error al guardar el archivo")
            else:
                print("⚠️ No se encontraron anuncios. Verifica que la URL sea correcta y contenga anuncios.")
                
        except Exception as e:
            print(f"❌ Error durante el scraping: {e}")
    
    elif mode == "2":
        # Modo procesamiento de HAR
        har_file_path = input("\n📁 Ingresa la ruta del archivo HAR: ").strip()
        
        if not os.path.exists(har_file_path):
            print("❌ El archivo no existe. Verifica la ruta.")
            return
        
        # Crear parser y procesar
        parser = FacebookAdsHARParser(har_file_path)
        
        if parser.process_har_file():
            # Mostrar resumen
            print(parser.get_summary())
            
            # Guardar datos
            output_file = input("\n💾 Nombre del archivo de salida (default: facebook_ads_data.json): ").strip()
            if not output_file:
                output_file = 'facebook_ads_data.json'
            
            if parser.save_to_json(output_file):
                print(f"\n✅ Proceso completado exitosamente!")
                print(f"📄 Archivo guardado: {output_file}")
                print(f"📊 Total de anuncios extraídos: {len(parser.ads_data)}")
            else:
                print("❌ Error al guardar el archivo")
        else:
            print("❌ Error procesando el archivo HAR")
    
    else:
        print("❌ Opción inválida. Por favor selecciona 1 o 2.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⛔ Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error general: {e}")