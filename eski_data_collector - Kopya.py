import asyncio
import json
from typing import Dict, List, Any, Optional
import aiohttp
from datetime import datetime
import numpy as np  # ✅ EKSİK IMPORT EKLENDİ

from config import CONFIG
from constants import Exchange, OrderSide
from data.data_storage import DataStorage
from utils.logger import setup_logger
from utils.helpers import format_quantity, calculate_usd_value

class DataParser:
    """Tüm exchange verilerini standart formata çevirir"""
    
    def __init__(self):
        self.logger = None
        
    async def initialize(self):
        self.logger = await setup_logger("data_parser")
    
    @staticmethod
    def standardize_liquidation_data(data: Dict[str, Any], exchange: str) -> Dict[str, Any]:
        """Liquidation verilerini standart formata dönüştür"""
        standardized = {
            "exchange": exchange,
            "symbol": "",
            "type": "liquidation",
            "side": "",
            "quantity": 0.0,
            "price": 0.0,
            "timestamp": ""
        }
        
        try:
            if exchange == Exchange.BINANCE.value:
                standardized.update({
                    "symbol": data['o']['s'],
                    "side": data['o']['S'].lower(),
                    "quantity": float(data['o']['q']),
                    "price": float(data['o']['p']),
                    "timestamp": datetime.utcfromtimestamp(data['E']/1000).isoformat() + 'Z'
                })
            elif exchange == Exchange.BYBIT.value:
                standardized.update({
                    "symbol": data['data']['symbol'],
                    "side": data['data']['side'].lower(),
                    "quantity": float(data['data']['size']),
                    "price": float(data['data']['price']),
                    "timestamp": datetime.utcnow().isoformat() + 'Z'
                })
            elif exchange == Exchange.OKX.value:
                standardized.update({
                    "symbol": data['arg']['instId'].replace('-', ''),
                    "side": data['data'][0]['side'].lower(),
                    "quantity": float(data['data'][0]['sz']),
                    "price": float(data['data'][0]['px']),
                    "timestamp": datetime.utcfromtimestamp(int(data['data'][0]['ts'])/1000).isoformat() + 'Z'
                })
        except KeyError as e:
            return None
            
        return standardized if DataParser.validate_data(standardized) else None
    
    @staticmethod
    def validate_data(data: Dict[str, Any]) -> bool:
        """Veriyi validate et"""
        required = ['exchange', 'symbol', 'side', 'quantity', 'price', 'timestamp']
        return all(data.get(field) for field in required)

class BinanceWebSocket:
    """Binance WebSocket bağlantısı"""
    
    def __init__(self, manager):
        self.manager = manager
        self.ws = None
        self.is_connected = False
        self.reconnect_delay = 5
        self.logger = None
        
    async def initialize(self):
        self.logger = await setup_logger("binance_ws")
        
    async def connect(self):
        """WebSocket'e bağlan - improved error handling"""
        endpoint = "wss://fstream.binance.com/ws"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        endpoint, 
                        timeout=aiohttp.ClientTimeout(total=30),
                        heartbeat=30
                    ) as websocket:
                        self.ws = websocket
                        self.is_connected = True
                        await self.logger.info("WebSocket connected", exchange="binance", status="connected")
                        
                        await self.subscribe()
                        await self.listen()
                        break  # Başarılı oldu, döngüden çık
                        
            except Exception as e:
                await self.logger.error(f"Connection error (attempt {attempt + 1}/{max_retries})", exchange="binance", error=str(e))
                self.is_connected = False
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(5 * (attempt + 1))  # Artan gecikme
                else:
                    await self.logger.error("Max connection attempts reached", exchange="binance")
                    break
            
    async def subscribe(self):
        """Binance channel'larına subscribe ol"""
        subscription = {
            "method": "SUBSCRIBE",
            "params": [
                "btcusdt@trade", "btcusdt@forceOrder", "btcusdt@bookTicker",
                "ethusdt@trade", "ethusdt@forceOrder", "ethusdt@bookTicker"
            ],
            "id": 1
        }
        await self.ws.send_json(subscription)
        await self.logger.info("Subscription sent", exchange="binance", channels=len(subscription["params"]))
        
    async def listen(self):
        """WebSocket mesajlarını dinle"""
        async for message in self.ws:
            if not self.manager.is_running:
                break
                
            if message.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(message.data)
                await self.handle_message(data)
            elif message.type == aiohttp.WSMsgType.ERROR:
                await self.logger.error("WebSocket error", exchange="binance", error=message.data)
                break
                
    async def handle_message(self, data: Dict[str, Any]):
        """Binance mesajlarını işle"""
        try:
            if data.get('e') == 'forceOrder':
                standardized_data = DataParser.standardize_liquidation_data(data, Exchange.BINANCE.value)
                if standardized_data:
                    await self.manager.on_liquidation_data(standardized_data)
        except Exception as e:
            await self.logger.error("Message handling error", exchange="binance", error=str(e))
            
    async def reconnect(self):
        """Yeniden bağlanmayı dene"""
        await self.logger.warning("Reconnecting", exchange="binance", delay=self.reconnect_delay)
        await asyncio.sleep(self.reconnect_delay)
        await self.connect()

class BybitWebSocket:
    """Bybit WebSocket bağlantısı"""
    
    def __init__(self, manager):
        self.manager = manager
        self.ws = None
        self.is_connected = False
        self.reconnect_delay = 5
        self.logger = None
        
    async def initialize(self):
        self.logger = await setup_logger("bybit_ws")
        
    async def connect(self):
        """Bybit WebSocket'e bağlan"""
        endpoint = "wss://stream.bybit.com/v5/public/linear"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(endpoint, timeout=aiohttp.ClientTimeout(total=30)) as websocket:
                    self.ws = websocket
                    self.is_connected = True
                    await self.logger.info("WebSocket connected", exchange="bybit", status="connected")
                    
                    await self.subscribe()
                    await self.listen()
                    
        except Exception as e:
            await self.logger.error("Connection error", exchange="bybit", error=str(e))
            self.is_connected = False
            await self.reconnect()
            
    async def subscribe(self):
        """Bybit channel'larına subscribe ol"""
        subscription = {
            "op": "subscribe",
            "args": [
                "publicTrade.BTCUSDT", "liquidation.BTCUSDT", "orderbook.50.BTCUSDT",
                "publicTrade.ETHUSDT", "liquidation.ETHUSDT", "orderbook.50.ETHUSDT"
            ]
        }
        await self.ws.send_json(subscription)
        await self.logger.info("Subscription sent", exchange="bybit", channels=len(subscription["args"]))
        
    async def listen(self):
        """WebSocket mesajlarını dinle"""
        async for message in self.ws:
            if not self.manager.is_running:
                break
                
            if message.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(message.data)
                await self.handle_message(data)
            elif message.type == aiohttp.WSMsgType.ERROR:
                await self.logger.error("WebSocket error", exchange="bybit", error=message.data)
                break
                
    async def handle_message(self, data: Dict[str, Any]):
        """Bybit mesajlarını işle"""
        try:
            if data.get('topic', '').startswith('liquidation'):
                standardized_data = DataParser.standardize_liquidation_data(data, Exchange.BYBIT.value)
                if standardized_data:
                    await self.manager.on_liquidation_data(standardized_data)
        except Exception as e:
            await self.logger.error("Message handling error", exchange="bybit", error=str(e))
            
    async def reconnect(self):
        """Yeniden bağlanmayı dene"""
        await self.logger.warning("Reconnecting", exchange="bybit", delay=self.reconnect_delay)
        await asyncio.sleep(self.reconnect_delay)
        await self.connect()

class OKXWebSocket:
    """OKX WebSocket bağlantısı"""
    
    def __init__(self, manager):
        self.manager = manager
        self.ws = None
        self.is_connected = False
        self.reconnect_delay = 5
        self.logger = None
        
    async def initialize(self):
        self.logger = await setup_logger("okx_ws")
        
    async def connect(self):
        """OKX WebSocket'e bağlan"""
        endpoint = "wss://ws.okx.com:8443/ws/v5/public"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(endpoint, timeout=aiohttp.ClientTimeout(total=30)) as websocket:
                    self.ws = websocket
                    self.is_connected = True
                    await self.logger.info("WebSocket connected", exchange="okx", status="connected")
                    
                    await self.subscribe()
                    await self.listen()
                    
        except Exception as e:
            await self.logger.error("Connection error", exchange="okx", error=str(e))
            self.is_connected = False
            await self.reconnect()
            
    async def subscribe(self):
        """OKX channel'larına subscribe ol"""
        subscription = {
            "op": "subscribe",
            "args": [
                {"channel": "trades", "instId": "BTC-USDT"},
                {"channel": "liquidation-orders", "instId": "BTC-USDT"},
                {"channel": "books50", "instId": "BTC-USDT"},
                {"channel": "trades", "instId": "ETH-USDT"},
                {"channel": "liquidation-orders", "instId": "ETH-USDT"},
                {"channel": "books50", "instId": "ETH-USDT"}
            ]
        }
        await self.ws.send_json(subscription)
        await self.logger.info("Subscription sent", exchange="okx", channels=len(subscription["args"]))
        
    async def listen(self):
        """WebSocket mesajlarını dinle"""
        async for message in self.ws:
            if not self.manager.is_running:
                break
                
            if message.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(message.data)
                await self.handle_message(data)
            elif message.type == aiohttp.WSMsgType.ERROR:
                await self.logger.error("WebSocket error", exchange="okx", error=message.data)
                break
                
    async def handle_message(self, data: Dict[str, Any]):
        """OKX mesajlarını işle"""
        try:
            if data.get('arg', {}).get('channel') == 'liquidation-orders':
                standardized_data = DataParser.standardize_liquidation_data(data, Exchange.OKX.value)
                if standardized_data:
                    await self.manager.on_liquidation_data(standardized_data)
        except Exception as e:
            await self.logger.error("Message handling error", exchange="okx", error=str(e))
            
    async def reconnect(self):
        """Yeniden bağlanmayı dene"""
        await self.logger.warning("Reconnecting", exchange="okx", delay=self.reconnect_delay)
        await asyncio.sleep(self.reconnect_delay)
        await self.connect()

class WebSocketManager:
    """Tüm exchange WebSocket bağlantılarını yönetir"""
    
    def __init__(self):
        self.is_running = False
        self.storage = DataStorage()
        self.connections = {'binance': None, 'bybit': None, 'okx': None}
        self.health_check_interval = 60
        self.logger = None
        
    async def initialize(self):
        """WebSocket manager'ı başlat"""
        self.logger = await setup_logger("ws_manager")
        await self.storage.initialize()
        self.is_running = True
        await self.logger.info("WebSocket Manager initialized", status="ready")
        
    async def start_all_connections(self):
        """Tüm exchange bağlantılarını başlat"""
        binance_ws = BinanceWebSocket(self)
        bybit_ws = BybitWebSocket(self)
        okx_ws = OKXWebSocket(self)
        
        await binance_ws.initialize()
        await bybit_ws.initialize()
        await okx_ws.initialize()
        
        self.connections['binance'] = binance_ws
        self.connections['bybit'] = bybit_ws
        self.connections['okx'] = okx_ws
        
        tasks = []
        tasks.append(asyncio.create_task(binance_ws.connect()))
        tasks.append(asyncio.create_task(bybit_ws.connect()))
        tasks.append(asyncio.create_task(okx_ws.connect()))
        tasks.append(asyncio.create_task(self.health_check()))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def on_liquidation_data(self, data: Dict[str, Any]):
        """Liquidation verisi geldiğinde çağrılır"""
        try:
            await self.storage.save_liquidation(data)
            
            usd_value = calculate_usd_value(data['quantity'], data['price'])
            side_emoji = "📉" if data['side'] == 'sell' else "📈"
            
            await self.logger.info(
                "Liquidation detected",
                exchange=data['exchange'],
                symbol=data['symbol'],
                side=data['side'],
                quantity=format_quantity(data['quantity']),
                price=data['price'],
                usd_value=usd_value,
                emoji=side_emoji
            )
            
        except Exception as e:
            await self.logger.error("Liquidation data processing error", 
                                  error=str(e), 
                                  exchange=data.get('exchange'),
                                  symbol=data.get('symbol'))
            
    async def health_check(self):
        """Connection health check"""
        while self.is_running:
            await asyncio.sleep(self.health_check_interval)
            
            for exchange, connection in self.connections.items():
                if connection and not connection.is_connected:
                    await self.logger.warning("Connection lost", exchange=exchange, status="disconnected")
                    asyncio.create_task(connection.connect())
                    
    async def stop(self):
        """Tüm bağlantıları durdur"""
        self.is_running = False
        await self.logger.info("WebSocket Manager stopping", status="shutting_down")
        
        for exchange, connection in self.connections.items():
            if connection and connection.ws:
                try:
                    await connection.ws.close()
                    await self.logger.info("Connection closed", exchange=exchange)
                except Exception as e:
                    await self.logger.error("Error closing connection", exchange=exchange, error=str(e))

class DataCollector:
    """Ana veri toplayıcı sınıfı"""
    
    def __init__(self):
        self.ws_manager = WebSocketManager()
        self.logger = None
        
    async def initialize(self):
        """Data collector'ı başlat"""
        self.logger = await setup_logger("data_collector")
        await self.ws_manager.initialize()
        await self.logger.info("Data Collector initialized", status="ready")
        
    async def start(self):
        """Veri toplamayı başlat"""
        await self.logger.info("Starting data collection", status="starting")
        await self.ws_manager.start_all_connections()
        
    async def stop(self):
        """Veri toplamayı durdur"""
        await self.ws_manager.stop()
        await self.logger.info("Data Collector stopped", status="stopped")
