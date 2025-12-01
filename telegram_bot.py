import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import aiohttp

from config import CONFIG
from utils.logger import setup_logger
from utils.helpers import format_quantity, calculate_usd_value, format_price

class AlertType(Enum):
    WHALE_MOVEMENT = "WHALE_MOVEMENT"
    LIQUIDATION_CASCADE = "LIQUIDATION_CASCADE"
    AI_PREDICTION = "AI_PREDICTION"
    SYSTEM_HEALTH = "SYSTEM_HEALTH"
    CRITICAL_EVENT = "CRITICAL_EVENT"

@dataclass
class AlertMessage:
    """Alert mesajı için data class"""
    
    alert_type: AlertType
    title: str
    message: str
    symbol: Optional[str] = None
    priority: int = 1
    timestamp: str = None
    data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

class TelegramBot:
    """Telegram alert bot for real-time notifications"""
    
    def __init__(self):
        self.bot_token = CONFIG.TELEGRAM_BOT_TOKEN
        self.chat_ids = self._parse_chat_ids(CONFIG.TELEGRAM_CHAT_ID)
        self.is_running = False
        self.message_queue = asyncio.Queue()
        self.rate_limits = {}
        self.session = None
        self.max_messages_per_minute = 10
        self.message_timestamps = []
        self.logger = None
        
    def _parse_chat_ids(self, chat_id_config: str) -> List[str]:
        """Chat ID'leri parse et"""
        if not chat_id_config:
            return []
        return [cid.strip() for cid in chat_id_config.split(',')]
    
    async def initialize(self) -> None:
        """Telegram bot'u başlat"""
        self.logger = await setup_logger("telegram_bot")
        
        if not self.bot_token or not self.chat_ids:
            await self.logger.warning("Telegram bot configuration missing", 
                                    has_token=bool(self.bot_token),
                                    has_chats=bool(self.chat_ids))
            return
            
        self.session = aiohttp.ClientSession()
        self.is_running = True
        
        asyncio.create_task(self._message_consumer())
        asyncio.create_task(self._health_monitor())
        
        await self.logger.info("Telegram Bot initialized", status="ready", chats=len(self.chat_ids))
        
    async def _health_monitor(self) -> None:
        """Bot health monitoring"""
        while self.is_running:
            await asyncio.sleep(60)
            
            recent_messages = [ts for ts in self.message_timestamps 
                             if datetime.now() - ts < timedelta(minutes=1)]
            if len(recent_messages) >= self.max_messages_per_minute:
                await self.logger.warning("Rate limit approaching", 
                                        messages_last_minute=len(recent_messages),
                                        limit=self.max_messages_per_minute)
                
    async def _message_consumer(self) -> None:
        """Message queue consumer"""
        while self.is_running:
            try:
                alert_message = await self.message_queue.get()
                await self._check_rate_limit()
                sent_count = await self._send_alert_to_all_chats(alert_message)
                
                if sent_count > 0:
                    await self.logger.info("Alert sent successfully", 
                                         alert_type=alert_message.alert_type.value,
                                         title=alert_message.title,
                                         chats_sent=sent_count)
                else:
                    await self.logger.warning("Alert failed to send", 
                                            alert_type=alert_message.alert_type.value)
                    
                self.message_queue.task_done()
                
            except Exception as e:
                await self.logger.error("Message consumer error", error=str(e))
                await asyncio.sleep(5)
                
    async def _check_rate_limit(self) -> None:
        """Rate limit kontrolü ve bekleme"""
        now = datetime.now()
        self.message_timestamps = [
            ts for ts in self.message_timestamps 
            if now - ts < timedelta(minutes=1)
        ]
        
        if len(self.message_timestamps) >= self.max_messages_per_minute:
            oldest_timestamp = min(self.message_timestamps)
            wait_time = 60 - (now - oldest_timestamp).total_seconds()
            if wait_time > 0:
                await self.logger.warning("Rate limit wait", wait_time=wait_time)
                await asyncio.sleep(wait_time)
                
        self.message_timestamps.append(datetime.now())
        
    async def _send_alert_to_all_chats(self, alert: AlertMessage) -> int:
        """Alert'ı tüm chat'lere gönder"""
        sent_count = 0
        
        for chat_id in self.chat_ids:
            try:
                success = await self._send_telegram_message(chat_id, alert)
                if success:
                    sent_count += 1
                else:
                    await self.logger.error("Failed to send to chat", chat_id=chat_id)
                    
            except Exception as e:
                await self.logger.error("Chat send error", chat_id=chat_id, error=str(e))
                
        return sent_count
        
    async def _send_telegram_message(self, chat_id: str, alert: AlertMessage) -> bool:
        """Tekil Telegram mesajı gönder"""
        try:
            message_text = self._format_message(alert)
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message_text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            async with self.session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    return True
                else:
                    error_text = await response.text()
                    await self.logger.error("Telegram API error", 
                                          status=response.status,
                                          error=error_text)
                    return False
                    
        except asyncio.TimeoutError:
            await self.logger.error("Telegram API timeout")
            return False
        except Exception as e:
            await self.logger.error("Telegram send error", error=str(e))
            return False
            
    def _format_message(self, alert: AlertMessage) -> str:
        """Alert mesajını formatla"""
        base_template = """
<b>{emoji} {title}</b>
{message}
        
⏰ <i>{timestamp}</i>
        """.strip()
        
        if alert.alert_type == AlertType.WHALE_MOVEMENT:
            return self._format_whale_alert(alert, base_template)
        elif alert.alert_type == AlertType.LIQUIDATION_CASCADE:
            return self._format_liquidation_alert(alert, base_template)
        elif alert.alert_type == AlertType.AI_PREDICTION:
            return self._format_ai_alert(alert, base_template)
        elif alert.alert_type == AlertType.SYSTEM_HEALTH:
            return self._format_system_alert(alert, base_template)
        elif alert.alert_type == AlertType.CRITICAL_EVENT:
            return self._format_critical_alert(alert, base_template)
        else:
            return base_template.format(
                emoji="📢",
                title=alert.title,
                message=alert.message,
                timestamp=alert.timestamp
            )
            
    def _format_whale_alert(self, alert: AlertMessage, template: str) -> str:
        """Whale alert formatı"""
        data = alert.data or {}
        emoji = "🐋" if data.get('size', 'WHALE') == 'WHALE' else "🐳"
        
        message_lines = [
            f"<b>Symbol:</b> {alert.symbol}",
            f"<b>Direction:</b> {data.get('direction', 'N/A')}",
            f"<b>Size:</b> ${data.get('usd_value', 0):,.0f}",
            f"<b>Impact:</b> {data.get('impact_score', 0):.2f}",
        ]
        
        if data.get('liquidation_risk'):
            message_lines.append(f"<b>Liquidation Risk:</b> {data.get('liquidation_risk'):.2f}")
            
        message = "\n".join(message_lines)
        
        return template.format(
            emoji=emoji,
            title=alert.title,
            message=message,
            timestamp=alert.timestamp
        )
        
    def _format_liquidation_alert(self, alert: AlertMessage, template: str) -> str:
        """Liquidation alert formatı"""
        data = alert.data or {}
        emoji = "💀" if data.get('cascade', False) else "📉"
        
        message_lines = [
            f"<b>Symbol:</b> {alert.symbol}",
            f"<b>Side:</b> {data.get('side', 'N/A')}",
            f"<b>Quantity:</b> {format_quantity(data.get('quantity', 0))}",
            f"<b>Price:</b> ${format_price(data.get('price', 0))}",
            f"<b>USD Value:</b> ${data.get('usd_value', 0):,.0f}",
        ]
        
        if data.get('cascade'):
            message_lines.append(f"<b>Cascade Events:</b> {data.get('cascade_events', 0)}")
            
        if data.get('pressure_index'):
            message_lines.append(f"<b>Pressure Index:</b> {data.get('pressure_index'):.3f}")
            
        message = "\n".join(message_lines)
        
        return template.format(
            emoji=emoji,
            title=alert.title,
            message=message,
            timestamp=alert.timestamp
        )
        
    def _format_ai_alert(self, alert: AlertMessage, template: str) -> str:
        """AI prediction alert formatı"""
        data = alert.data or {}
        emoji = "🤖"
        
        message_lines = [
            f"<b>Symbol:</b> {alert.symbol}",
            f"<b>Prediction:</b> {data.get('prediction_type', 'N/A')}",
            f"<b>Value:</b> {data.get('predicted_value', 0):.3f}",
            f"<b>Confidence:</b> {data.get('confidence', 0):.3f}",
            f"<b>Timeframe:</b> {data.get('timeframe', 'N/A')}",
        ]
        
        if data.get('recommendation'):
            message_lines.append(f"<b>Recommendation:</b> {data.get('recommendation')}")
            
        message = "\n".join(message_lines)
        
        return template.format(
            emoji=emoji,
            title=alert.title,
            message=message,
            timestamp=alert.timestamp
        )
        
    def _format_system_alert(self, alert: AlertMessage, template: str) -> str:
        """System alert formatı"""
        data = alert.data or {}
        status = data.get('status', 'INFO')
        emoji = "✅" if status == "HEALTHY" else "⚠️" if status == "WARNING" else "❌"
        
        message_lines = [
            f"<b>Status:</b> {status}",
            f"<b>Component:</b> {data.get('component', 'N/A')}",
            f"<b>Message:</b> {data.get('details', 'N/A')}",
        ]
        
        if data.get('performance'):
            message_lines.append(f"<b>Performance:</b> {data.get('performance')}%")
            
        message = "\n".join(message_lines)
        
        return template.format(
            emoji=emoji,
            title=alert.title,
            message=message,
            timestamp=alert.timestamp
        )
        
    def _format_critical_alert(self, alert: AlertMessage, template: str) -> str:
        """Critical event alert formatı"""
        data = alert.data or {}
        emoji = "🚨"
        
        message_lines = [
            f"<b>Event:</b> {alert.title}",
            f"<b>Severity:</b> {data.get('severity', 'HIGH')}",
            f"<b>Symbol:</b> {alert.symbol or 'MULTIPLE'}",
            f"<b>Description:</b> {data.get('description', 'N/A')}",
        ]
        
        if data.get('action_required'):
            message_lines.append(f"<b>Action Required:</b> {data.get('action_required')}")
            
        message = "\n".join(message_lines)
        
        return template.format(
            emoji=emoji,
            title="🚨 CRITICAL ALERT",
            message=message,
            timestamp=alert.timestamp
        )
        
    async def send_whale_alert(self, symbol: str, whale_data: Dict[str, Any]) -> None:
        """Whale movement alert gönder"""
        alert = AlertMessage(
            alert_type=AlertType.WHALE_MOVEMENT,
            title=f"WHALE ALERT - {symbol}",
            message=f"Whale activity detected for {symbol}",
            symbol=symbol,
            priority=3,
            data=whale_data
        )
        
        await self.message_queue.put(alert)
        await self.logger.debug("Whale alert queued", symbol=symbol)
        
    async def send_liquidation_alert(self, symbol: str, liquidation_data: Dict[str, Any]) -> None:
        """Liquidation alert gönder"""
        is_cascade = liquidation_data.get('cascade', False)
        alert_type = AlertType.LIQUIDATION_CASCADE if is_cascade else AlertType.CRITICAL_EVENT
        title = "CASCADE LIQUIDATION" if is_cascade else "LIQUIDATION ALERT"
        
        alert = AlertMessage(
            alert_type=alert_type,
            title=f"{title} - {symbol}",
            message=f"Liquidation event detected for {symbol}",
            symbol=symbol,
            priority=4 if is_cascade else 2,
            data=liquidation_data
        )
        
        await self.message_queue.put(alert)
        await self.logger.debug("Liquidation alert queued", symbol=symbol, is_cascade=is_cascade)
        
    async def send_ai_prediction_alert(self, symbol: str, prediction_data: Dict[str, Any]) -> None:
        """AI prediction alert gönder"""
        alert = AlertMessage(
            alert_type=AlertType.AI_PREDICTION,
            title=f"AI PREDICTION - {symbol}",
            message=f"AI prediction update for {symbol}",
            symbol=symbol,
            priority=2,
            data=prediction_data
        )
        
        await self.message_queue.put(alert)
        await self.logger.debug("AI prediction alert queued", symbol=symbol)
        
    async def send_system_alert(self, component: str, status: str, details: str, 
                              performance: Optional[float] = None) -> None:
        """System health alert gönder"""
        alert = AlertMessage(
            alert_type=AlertType.SYSTEM_HEALTH,
            title=f"SYSTEM STATUS - {component}",
            message=f"System status update for {component}",
            priority=1,
            data={
                'status': status,
                'component': component,
                'details': details,
                'performance': performance
            }
        )
        
        await self.message_queue.put(alert)
        await self.logger.debug("System alert queued", component=component, status=status)
        
    async def send_critical_alert(self, title: str, description: str, 
                                severity: str = "HIGH", symbol: Optional[str] = None,
                                action_required: Optional[str] = None) -> None:
        """Critical event alert gönder"""
        alert = AlertMessage(
            alert_type=AlertType.CRITICAL_EVENT,
            title=title,
            message=description,
            symbol=symbol,
            priority=5,
            data={
                'severity': severity,
                'description': description,
                'action_required': action_required
            }
        )
        
        await self.message_queue.put(alert)
        await self.logger.debug("Critical alert queued", title=title, severity=severity)
        
    async def get_bot_status(self) -> Dict[str, Any]:
        """Bot status bilgisini getir"""
        status = {
            'is_running': self.is_running,
            'chat_count': len(self.chat_ids),
            'queue_size': self.message_queue.qsize(),
            'messages_last_minute': len(self.message_timestamps),
            'rate_limit': self.max_messages_per_minute
        }
        
        await self.logger.debug("Bot status checked", status=status)
        return status
        
    async def stop(self) -> None:
        """Bot'u durdur"""
        self.is_running = False
        await self.message_queue.join()
        
        if self.session:
            await self.session.close()
            
        await self.logger.info("Telegram Bot stopped", status="stopped")
