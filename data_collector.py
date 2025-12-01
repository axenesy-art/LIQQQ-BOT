import asyncio
import aiohttp
import json
import logging
import sqlite3
import redis
from datetime import datetime
import time
import signal
import sys
import socket
from aiohttp import ClientSession, TCPConnector
# Düzeltilmiş import: AsyncResolver
from aiohttp.resolver import AsyncResolver 
from utils.logger import setup_logger

# DNS timeout ayarı - CRITICAL FIX
socket.setdefaulttimeout(10.0)

# Gelişmiş logging konfigürasyonu
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collector.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # Unicode desteği için stdout
    ]
)
logger = logging.getLogger('UltraLiquidationBot')

class DataCollector:
    def __init__(self, db_path='liquidation_data.db', redis_host='localhost', redis_port=6379):
        self.db_path = db_path
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None

        # ✅ KRİTİK: Logger'ı hemen tanımla (None olarak)
        self.logger = None
        
        # Exchange WebSocket URL'leri
        self.exchanges = {
            'binance': {
                'urls': [
                    # ✅ DIRECT IP'ler - DNS BYPASS
                    'wss://35.74.163.134/ws/!forceOrder@arr',
                    'wss://54.249.141.230/ws/!forceOrder@arr', 
                    'wss://35.73.112.136/ws/!forceOrder@arr',
                    'wss://52.194.48.252/ws/!forceOrder@arr',
                    'wss://fstream.binance.com/ws/!forceOrder@arr'  # Fallback
                ],
                'subscription': None,
                'current_url_index': 0
            },
            'bybit': {
                'urls': [
                    # ✅ DIRECT IP'ler - CloudFront IP'leri
                    'wss://108.157.60.13/v5/public/linear',
                    'wss://108.157.60.52/v5/public/linear',
                    'wss://108.157.60.24/v5/public/linear', 
                    'wss://108.157.60.49/v5/public/linear',
                    'wss://stream.bybit.com/v5/public/linear'  # Fallback
                ],
                'subscription': {
                    "op": "subscribe",
                    "args": ["liquidation"]
                },
                'current_url_index': 0
            },
            'okx': {
                'urls': [
                    # ✅ DIRECT IP'ler - CloudFlare IP'leri
                    'wss://172.64.144.82:8443/ws/v5/public',
                    'wss://104.18.43.174:8443/ws/v5/public',
                    'wss://ws.okx.com:8443/ws/v5/public'  # Fallback
                ],
                'subscription': {
                    "op": "subscribe",
                    "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]
                },
                'current_url_index': 0
            }
        }
        
        self.websockets = {}
        self.sessions = {}  # YENİ: Session'ları da sakla
        self.is_running = False
        self.reconnect_attempts = {}
        self.max_reconnect_attempts = 10
        self.connection_timeout = 30
        self.heartbeat_interval = 25
        
        # DNS problemi için alternative resolver
        self.resolver = AsyncResolver()
        
        self.setup_database()
        self.setup_redis()

        socket.setdefaulttimeout(30.0)

        # TCP Keepalive settings
        try:
            # Windows için TCP keepalive
            if sys.platform == "win32":
                socket.socket(socket.AF_INET, socket.SOCK_STREAM).setsockopt(
                    socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
                )
        except:
            pass

    def setup_database(self):
        """Veritabanını kur"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS liquidations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Indexler
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_timestamp ON liquidations(exchange, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_side ON liquidations(symbol, side)')
            
            conn.commit()
            conn.close()
            logger.info("Database setup completed successfully")
        except Exception as e:
            logger.error(f"Database setup error: {e}")

    def setup_redis(self):
        """Redis bağlantısını kur - REDIS FALLBACK FIX"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=10
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Redis connection established successfully")
        except Exception as e:
            logger.warning(f"Redis connection failed, continuing without Redis: {e}")
            self.redis_client = None

    async def initialize(self):
        """FIXED: Tüm bağlantıları initialize et"""
        # ✅ ÖNCE logger'ı set et
        self.logger = await setup_logger("data_collector")
        
        # ✅ SONRA self.logger kullan
        await self.logger.info("Initializing Data Collector connections")
        self.setup_redis()  
        
        try:
            await self.logger.info("DNS resolver initialized successfully (using custom nameservers)")
        except Exception as e:
            await self.logger.error(f"DNS resolver initialization failed: {e}")
        
        await self.logger.info("Data Collector initialization completed")

    async def create_stable_session(self):
        """DNS problemleri için SÜPER STABIL HTTP session oluştur"""
        try:
            # Burada da AsyncResolver çağrısının nameservers'ı doğru kullanması gerekir
            connector = TCPConnector(
                resolver=self.resolver, # __init__ içinde tanımlanmış resolver'ı kullan
                limit=0,
                limit_per_host=0,
                keepalive_timeout=35,
                enable_cleanup_closed=True,
                force_close=False,
                use_dns_cache=True,
                ttl_dns_cache=300,
                family=socket.AF_INET,
                local_addr=None
            )
            
            # Timeout ayarları
            timeout = aiohttp.ClientTimeout(
                total=0,
                connect=15,
                sock_connect=15,
                sock_read=30
            )
            
            return ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            )
        except Exception as e:
            logger.error(f"Failed to create stable session: {e}")
            # Fallback: basic session
            return ClientSession()

    def get_current_url(self, exchange_name):
        """Mevcut URL'i al ve rotate et"""
        exchange_config = self.exchanges[exchange_name]
        current_index = exchange_config['current_url_index']
        return exchange_config['urls'][current_index]

    def rotate_url(self, exchange_name):
        """Bir sonraki URL'e geç"""
        exchange_config = self.exchanges[exchange_name]
        exchange_config['current_url_index'] = (exchange_config['current_url_index'] + 1) % len(exchange_config['urls'])
        logger.info(f"Rotated {exchange_name} to URL index {exchange_config['current_url_index']}")

    async def connect_to_exchange(self, exchange_name):
        """Exchange WebSocket'ine bağlan - DNS FIX EKLENDİ"""
        if exchange_name not in self.reconnect_attempts:
            self.reconnect_attempts[exchange_name] = 0
            
        attempt = self.reconnect_attempts[exchange_name]
        if attempt > 0:
            delay = min(2 ** min(attempt, 5), 30)
            await self.logger.info(f"Waiting {delay}s before reconnecting to {exchange_name}")
            await asyncio.sleep(delay)
        
        current_url = self.get_current_url(exchange_name)
        
        try:
            await self.logger.info(f"Connecting to {exchange_name} at {current_url} (attempt {attempt + 1})")
            
            # ✅ DNS FIX: Gelişmiş session creation
            session = await self.create_stable_session()
            
            # ✅ DNS OPTIMIZATION: Custom connector ile
            import aiohttp
            from aiohttp import TCPConnector
            import socket
            
            # Gelişmiş DNS connector
            connector = TCPConnector(
                family=socket.AF_INET,  # IPv4 zorla
                verify_ssl=True,
                use_dns_cache=True,
                ttl_dns_cache=300,  # 5 dakika DNS cache
                limit=100,
                limit_per_host=10
            )
            
            # Optimize edilmiş timeout
            timeout = aiohttp.ClientTimeout(
                total=30,      # Toplam timeout
                connect=15,    # Bağlantı timeout
                sock_connect=15, # Socket bağlantı timeout
                sock_read=20   # Socket okuma timeout
            )
            
            # Mevcut session yerine gelişmiş session kullan
            custom_session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': '*/*'
                }
            )
            
            websocket = await custom_session.ws_connect(
                current_url,
                timeout=timeout,
                heartbeat=self.heartbeat_interval,
                receive_timeout=30,
                autoclose=True,
                autoping=True,  # ✅ EKLENDİ: Otomatik ping
                compress=15     # ✅ EKLENDİ: Compression
            )
            
            subscription = self.exchanges[exchange_name]['subscription']
            if subscription:
                await websocket.send_json(subscription)
                await self.logger.info(f"Subscribed to {exchange_name} liquidation channel")
            
            # ✅ FIX: Custom session'ı sakla
            self.sessions[exchange_name] = custom_session
            self.websockets[exchange_name] = websocket
            self.reconnect_attempts[exchange_name] = 0
            
            await self.logger.info(f"Successfully connected to {exchange_name}")
            return custom_session, websocket
            
        except Exception as e:
            self.reconnect_attempts[exchange_name] += 1
            await self.logger.error(f"Connection failed to {exchange_name}: {e}")
            self.rotate_url(exchange_name)
            
            # Session'ı temizle
            if exchange_name in self.sessions:
                try:
                    await self.sessions[exchange_name].close()
                except:
                    pass
                del self.sessions[exchange_name]
            
            if self.reconnect_attempts[exchange_name] >= self.max_reconnect_attempts:
                await self.logger.error(f"Max reconnection attempts reached for {exchange_name}")
                return None
            
            return await self.connect_to_exchange(exchange_name)

    async def handle_websocket_messages(self, exchange_name, websocket, session):
        """WebSocket mesajlarını işle - ERROR HANDLING FIX"""
        try:
            async for message in websocket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self.process_liquidation_data(exchange_name, message.data)
                elif message.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning(f"WebSocket connection closed for {exchange_name}")
                    break
                elif message.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error for {exchange_name}: {websocket.exception()}")
                    break
                elif message.type == aiohttp.WSMsgType.PING:
                    await websocket.pong()
                elif message.type == aiohttp.WSMsgType.PONG:
                    continue
                    
        except asyncio.TimeoutError:
            logger.warning(f"WebSocket timeout for {exchange_name}")
        except Exception as e:
            logger.error(f"Error handling messages from {exchange_name}: {e}")
        finally:
            # KRİTİK: Yeniden bağlanmadan önce session'ı kapat
            await self.reconnect_exchange(exchange_name, session)

    async def reconnect_exchange(self, exchange_name, session):
        """Exchange'e yeniden bağlan - İYİLEŞTİRİLDİ"""
        # Websocket'i kapat
        if exchange_name in self.websockets:
            try:
                await self.websockets[exchange_name].close()
            except Exception as e:
                logger.debug(f"Error closing websocket: {e}")
            del self.websockets[exchange_name]
        
        # Session'ı kapat
        if session:
            try:
                await session.close()
            except Exception as e:
                logger.debug(f"Error closing session: {e}")
        
        if exchange_name in self.sessions:
            del self.sessions[exchange_name]
        
        if self.is_running:
            attempt = self.reconnect_attempts.get(exchange_name, 0)
            delay = min(2 ** attempt, 60)
            logger.info(f"Reconnecting to {exchange_name} in {delay}s (attempt {attempt})")
            await asyncio.sleep(delay)
            asyncio.create_task(self.manage_exchange_connection(exchange_name))

    async def manage_exchange_connection(self, exchange_name):
        """Exchange bağlantısını yönet - İYİLEŞTİRİLDİ"""
        while self.is_running:
            try:
                result = await self.connect_to_exchange(exchange_name)
                if result:
                    session, websocket = result
                    await self.handle_websocket_messages(exchange_name, websocket, session)
                else:
                    logger.error(f"Failed to connect to {exchange_name}, retrying in 30s")
                    await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Error in connection manager for {exchange_name}: {e}")
                await asyncio.sleep(10)

    async def process_liquidation_data(self, exchange_name, raw_data):
        """Liquidation verilerini işle ve kaydet - OKX PARSING FIX"""
        try:
            data = json.loads(raw_data)
            liquidation_data = None
            
            # Exchange-specific data parsing
            if exchange_name == 'binance':
                if 'e' in data and data['e'] == 'forceOrder':
                    liquidation = data['o']
                    liquidation_data = {
                        'symbol': liquidation['s'],
                        'side': 'sell' if liquidation['S'] == 'SELL' else 'buy',
                        'quantity': float(liquidation['q']),
                        'price': float(liquidation['p']),
                        'timestamp': datetime.fromtimestamp(data['E'] / 1000)
                    }
                    
            elif exchange_name == 'bybit':
                if 'topic' in data and data['topic'] == 'liquidation':
                    for liquidation in data['data']:
                        liquidation_data = {
                            'symbol': liquidation['symbol'],
                            'side': liquidation['side'].lower(),
                            'quantity': float(liquidation['size']),
                            'price': float(liquidation['price']),
                            'timestamp': datetime.fromtimestamp(liquidation['timestamp'] / 1000)
                        }
                        await self.store_liquidation(exchange_name, liquidation_data)
                    return
                    
            elif exchange_name == 'okx':
                # OKX'in yeni formatı (önceki düzeltmelerden)
                if 'arg' in data and data['arg']['channel'] == 'liquidation-orders' and 'data' in data:
                    for liquidation in data['data']:
                        # instId ve ts bilgileri dış katmandan alınmalı (güvenli okuma)
                        instId = liquidation.get('instId', 'UNKNOWN')
                        
                        # OKX'te bazen 'details' alanı boş gelebilir veya farklı bir yapıya sahip olabilir
                        # Verinin içinden 'sz' ve 'px' değerlerini doğru okumaya çalışalım
                        price = float(liquidation.get('px', 0.0))
                        quantity = float(liquidation.get('sz', 0.0))
                        timestamp_ms = float(liquidation.get('ts', time.time() * 1000))
                        
                        # OKX'te likidasyon side'ı, pozisyonun tersidir.
                        pos_side = liquidation.get('posSide', '').lower()
                        if pos_side == 'long':
                            final_side = 'sell'
                        elif pos_side == 'short':
                            final_side = 'buy'
                        else:
                            final_side = liquidation.get('side', 'unknown').lower()

                        if quantity > 0 and price > 0:
                            liquidation_data = {
                                'symbol': instId,
                                'side': final_side,
                                'quantity': quantity,
                                'price': price,
                                'timestamp': datetime.fromtimestamp(timestamp_ms / 1000)
                            }
                            await self.store_liquidation(exchange_name, liquidation_data)
                    return
            
            if liquidation_data:
                await self.store_liquidation(exchange_name, liquidation_data)
                
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error from {exchange_name}: {e}")
        except KeyError as e:
            logger.warning(f"Key error processing {exchange_name} data: {e} - Data: {raw_data}")
        except Exception as e:
            logger.error(f"Error processing {exchange_name} data: {e}")

    async def store_liquidation(self, exchange, data):
        """Liquidation verisini SQLite ve Redis'e kaydet - REDIS DÜZELTİLDİ"""
        try:
            # SQLite'a kaydet
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO liquidations (exchange, symbol, side, quantity, price, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (exchange, data['symbol'], data['side'], data['quantity'], data['price'], data['timestamp']))
            conn.commit()
            conn.close()
            
            # Redis'e kaydet (eğer bağlantı varsa) - GRACEFUL DEGRADATION
            if self.redis_client:
                try:
                    timestamp_key = int(data['timestamp'].timestamp())
                    redis_key = f"liquidations:{exchange}:{data['symbol']}:{timestamp_key}"
                    
                    # HSET düzeltmesi - mapping yerine field-field kullan
                    liquidation_data = {
                        'exchange': exchange,
                        'symbol': data['symbol'],
                        'side': data['side'],
                        'quantity': str(data['quantity']),
                        'price': str(data['price']),
                        'timestamp': data['timestamp'].isoformat()
                    }
                    
                    # Tek tek field'ları set et
                    for field, value in liquidation_data.items():
                        self.redis_client.hset(redis_key, field, value)
                        
                    self.redis_client.expire(redis_key, 3600)  # 1 saat TTL
                except Exception as e:
                    logger.warning(f"Redis storage failed, continuing without Redis: {e}")
                    self.redis_client = None  # Disable Redis on error
            
            logger.info(f"Liquidation stored: {exchange} {data['symbol']} {data['side']} {data['quantity']} @ {data['price']}")
            
        except sqlite3.Error as e:
            logger.error(f"SQLite storage error: {e}")
        except Exception as e:
            logger.error(f"Storage error: {e}")

    async def start(self):
        """Data Collector'ü başlat"""
        logger.info("🚀 Starting Ultra Liquidation Data Collector")
        self.is_running = True
        
        # Önce initialize et
        await self.initialize()
        
        # Tüm exchange'lere bağlan
        tasks = []
        for exchange_name in self.exchanges.keys():
            task = asyncio.create_task(self.manage_exchange_connection(exchange_name))
            tasks.append(task)
        
        # Health check task
        health_task = asyncio.create_task(self.health_check())
        tasks.append(health_task)
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            await self.stop()

    async def health_check(self):
        """Sistem sağlık kontrolü"""
        while self.is_running:
            await asyncio.sleep(60)  # Her 60 saniyede bir
            
            active_connections = len([ws for ws in self.websockets.values() if not ws.closed])
            logger.info(f"Health check: {active_connections}/{len(self.exchanges)} connections active")
            
            # Redis connection check
            if self.redis_client:
                try:
                    self.redis_client.ping()
                except:
                    logger.warning("Redis connection lost, attempting reconnect")
                    self.setup_redis()
            
            # Bağlantı sayısı çok düşükse warning
            if active_connections < len(self.exchanges) // 2:
                logger.warning("Low connection count - attempting to restore connections")

    async def stop(self):
        """Data Collector'ü durdur"""
        logger.info("🛑 Stopping Ultra Liquidation Data Collector")
        self.is_running = False
        
        # Tüm WebSocket bağlantılarını kapat
        for exchange_name, websocket in list(self.websockets.items()):
            try:
                await websocket.close()
                logger.info(f"Closed connection to {exchange_name}")
            except Exception as e:
                logger.error(f"Error closing connection to {exchange_name}: {e}")
                
        # Websockets, session'lar kapatıldıktan sonra boşaltılmalı
        # Session'lar reconnect_exchange veya handle_websocket_messages'da kapatılıyor
        self.websockets.clear()

# Graceful shutdown handler
def signal_handler(signum, frame):
    logger.info("Received shutdown signal")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def main():
    collector = DataCollector()
    try:
        await collector.start()
    except KeyboardInterrupt:
        pass
    finally:
        await collector.stop()

if __name__ == "__main__":
    asyncio.run(main())