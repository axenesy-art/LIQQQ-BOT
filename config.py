import os
from typing import Dict, Any

class Config:
    """Ultra Liquidation Bot Configuration"""
    
    # Exchange API Keys
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
    BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
    
    # Telegram Bot
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # Trading Parameters - LOWERED FOR TESTING
    RISK_PER_TRADE = 0.02
    MAX_DAILY_RISK = 0.10
    MIN_CONFIDENCE = 0.3  # 🔽 Lowered from 0.8 to 0.3 for testing
    MIN_RISK_SCORE = 0.2   # 🔽 Added missing parameter

    # WebSocket Settings with better timeouts
    WEBSOCKET_TIMEOUT = 60
    RECONNECT_DELAY = 10
    HEARTBEAT_INTERVAL = 25
    
    # Data Collection
    LIQUIDATION_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT",
        "BNBUSDT", "XRPUSDT", "SOLUSDT", "MATICUSDT", "AVAXUSDT"
    ]
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate critical configuration settings"""
        required_settings = [
            cls.BINANCE_API_KEY,
            cls.BINANCE_SECRET_KEY,
            cls.TELEGRAM_BOT_TOKEN
        ]
        
        if not all(required_settings):
            missing = [name for name, value in zip(
                ['BINANCE_API_KEY', 'BINANCE_SECRET_KEY', 'TELEGRAM_BOT_TOKEN'],
                required_settings
            ) if not value]
            raise ValueError(f"Missing required settings: {', '.join(missing)}")
        
        return True

# Global config instance
CONFIG = Config()
