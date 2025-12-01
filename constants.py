from enum import Enum

class Exchange(Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    HUOBI = "huobi"
    KUCOIN = "kucoin"

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"

# WebSocket URLs
WEBSOCKET_URLS = {
    Exchange.BINANCE: "wss://fstream.binance.com/ws",
    Exchange.BYBIT: "wss://stream.bybit.com/v5/public/linear",
    Exchange.OKX: "wss://ws.okx.com:8443/ws/v5/public"
}

# Database Constants
DATABASE_NAME = "liquidation_bot.db"

# Trading Constants
MAX_LEVERAGE = 20
MIN_POSITION_SIZE = 10  # USD
MAX_POSITION_SIZE = 1000  # USD

# Alert Constants
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
ALERT_COOLDOWN = 60  # seconds

# Analysis Constants
PREDICTION_TIME_HORIZON = 15  # minutes
LIQUIDATION_THRESHOLD = 100000  # USD
