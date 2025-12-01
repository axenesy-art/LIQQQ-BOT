import logging
import asyncio
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Config dosyasını import etmeniz gerekecek (muhtemelen main.py'den geliyor)
# Burada varsayılan olarak tanımlayalım, main.py dosyanızda CONFIG import edilmeli.
class DummyConfig:
    def get(self, key, default):
        return default
CONFIG = DummyConfig()

class AsyncLogHandler(logging.Handler):
    """Async log handler for non-blocking file operations"""
    
    def __init__(self, log_file: str, max_bytes: int = 10 * 1024 * 1024, backup_count: int = 5):
        super().__init__()
        self.log_file = log_file
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.queue = asyncio.Queue()
        self.writer_task = None
        self._setup_file_handler()
        
    def _setup_file_handler(self):
        """Setup rotating file handler"""
        # Ensure log directory exists
        log_dir = Path(self.log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        self.file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]'
        )
        self.file_handler.setFormatter(formatter)
    
    def emit(self, record: logging.LogRecord):
        """Put record in queue for async writing"""
        if self.writer_task and not self.writer_task.done():
            # Try to put without blocking. If queue is full, skip (lossy but safe)
            try:
                self.queue.put_nowait(record)
            except asyncio.QueueFull:
                pass # Drop message if the queue is full
        
    async def writer(self):
        """Asynchronously write logs from the queue to the file"""
        while True:
            record = await self.queue.get()
            if record is None: # Sentinel to stop
                break
            
            # Format and write the record using the underlying file handler
            try:
                self.file_handler.emit(record)
            except Exception as e:
                # Fallback print to ensure we catch major logging errors
                print(f"Async log writing failed: {e}")
                
            self.queue.task_done()

    async def initialize(self):
        """Start the async writer task"""
        if self.writer_task is None:
            self.writer_task = asyncio.create_task(self.writer())

    async def close(self):
        """Stop the async writer task gracefully"""
        if self.writer_task:
            await self.queue.put(None) # Send sentinel to stop the writer
            await self.writer_task

class BotLogger:
    """Async wrapper for Python's logging module"""
    
    _instances: Dict[str, 'BotLogger'] = {}
    
    def __init__(self, name: str, log_level: str = "INFO", enable_file_logging: bool = True):
        self.name = name
        self.log_level_str = log_level
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.enable_file_logging = enable_file_logging
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.log_level)
        self.logger.propagate = False # Prevent logs from going to root logger initially
        self.is_initialized = False
        self.async_handler: Optional[AsyncLogHandler] = None
        
        self.formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
        )
    
    @classmethod
    def get_logger(cls, name: str, log_level: str = "INFO", enable_file_logging: bool = True) -> 'BotLogger':
        if name not in cls._instances:
            cls._instances[name] = cls(name, log_level, enable_file_logging)
        return cls._instances[name]

    async def initialize(self):
        if self.is_initialized:
            return

        # Clear existing handlers to prevent duplicate output
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # KRİTİK DÜZELTME: Unicode Hatası için basit StreamHandler
        try:
            # Basit ve güvenli StreamHandler - Unicode sorunlarını çözer
            stream_handler = logging.StreamHandler(sys.stdout)
            # Encoding sorunları için güvenli formatter
            safe_formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
            )
            stream_handler.setFormatter(safe_formatter)
        except Exception as e:
            # Fallback: çok basit handler
            stream_handler = logging.StreamHandler()
            print(f"StreamHandler setup error: {e}")
            
        stream_handler.setLevel(self.log_level)
        self.logger.addHandler(stream_handler)
        
        # Async File Handler
        if self.enable_file_logging:
            try:
                # CONFIG yerine DummyConfig kullanıldı. Gerçek implementasyonda CONFIG import edilmeli.
                log_file = os.path.join(CONFIG.get('LOG_DIR', 'logs'), f"{self.name}.log")
                self.async_handler = AsyncLogHandler(log_file)
                self.async_handler.file_handler.setFormatter(self.formatter)
                self.async_handler.setLevel(self.log_level)
                
                self.logger.addHandler(self.async_handler)
                await self.async_handler.initialize()
            except Exception as e:
                print(f"File handler setup error: {e}")
                
        self.is_initialized = True

    async def close(self):
        """Close loggers gracefully"""
        if self.async_handler:
            await self.async_handler.close()
            self.logger.removeHandler(self.async_handler)
        self.is_initialized = False
        
        # Diğer handler'ları da kapat
        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception as e:
                print(f"Handler close error: {e}")

    def _log_sync(self, level: int, message: str, **kwargs: Any):
         """FIXED: Senkron loglama - kwargs doğru şekilde işleniyor"""
    try:
        full_message = self._format_message(message, kwargs)
        self.logger.log(level, full_message)
    except Exception as e:
        # Fallback senkron loglama
        try:
            print(f"LOG ERROR: {e} - {message} - {kwargs}")
        except:
            pass  # Son çare

    async def _log_async(self, level: int, message: str, **kwargs: Any):
        """FIXED: Asenkron loglama - kwargs doğru şekilde iletilir"""
        try:
            # Senkron loglama işlemini executor'da çalıştır
            # DÜZELTME: kwargs'ı ayrı ayrı parametre olarak iletiyoruz
            loop = asyncio.get_event_loop()
            
            # _log_sync'i doğrudan çağır (executor kullanmadan - daha basit)
            self._log_sync(level, message, **kwargs)
            
        except Exception as e:
            # Fallback: doğrudan senkron loglama
            try:
                self._log_sync(level, f"ASYNC LOG ERROR: {e} - {message}", **kwargs)
            except:
                # Ultimate fallback
                print(f"ULTIMATE LOG FALLBACK: {level} - {message} - {e}")

    def _format_message(self, message: str, kwargs: Dict[str, Any]) -> str:
        """Format message with extra key=value pairs"""
        if not kwargs:
            return message
            
        extra_parts = []
        for key, value in kwargs.items():
            # Tüm değerleri string'e çevirerek hatalı formatlamayı önle
            try:
                extra_parts.append(f"{key}={str(value)}")
            except Exception:
                extra_parts.append(f"{key}=[unserializable]")
        
        return f"{message} | {' | '.join(extra_parts)}"

    async def debug(self, message: str, **kwargs: Any) -> None:
        """Debug log"""
        await self._log_async(logging.DEBUG, message, **kwargs)

    async def info(self, message: str, **kwargs: Any) -> None:
        """Info log"""
        await self._log_async(logging.INFO, message, **kwargs)

    async def warning(self, message: str, **kwargs: Any) -> None:
        """Warning log"""
        await self._log_async(logging.WARNING, message, **kwargs)

    async def error(self, message: str, **kwargs: Any) -> None:
        """Error log"""
        await self._log_async(logging.ERROR, message, **kwargs)

    async def critical(self, message: str, **kwargs: Any) -> None:
        """Critical log"""
        await self._log_async(logging.CRITICAL, message, **kwargs)

async def setup_logger(name: str, log_level: str = "INFO", enable_file_logging: bool = True) -> BotLogger:
    """Setup and return a logger instance"""
    logger = BotLogger.get_logger(name, log_level=log_level, enable_file_logging=enable_file_logging)
    await logger.initialize()
    return logger

# Default logger for quick use
_default_logger = None

async def get_default_logger() -> BotLogger:
    """Get default logger instance"""
    global _default_logger
    if _default_logger is None:
        _default_logger = await setup_logger("ultra_liquidation_bot")
    return _default_logger

# Quick async logging functions
async def log_debug(message: str, **kwargs: Any) -> None:
    """Quick debug log"""
    logger = await get_default_logger()
    await logger.debug(message, **kwargs)
    
async def log_info(message: str, **kwargs: Any) -> None:
    """Quick info log"""
    logger = await get_default_logger()
    await logger.info(message, **kwargs)
    
async def log_warning(message: str, **kwargs: Any) -> None:
    """Quick warning log"""
    logger = await get_default_logger()
    await logger.warning(message, **kwargs)
    
async def log_error(message: str, **kwargs: Any) -> None:
    """Quick error log"""
    logger = await get_default_logger()
    await logger.error(message, **kwargs)
    
async def log_critical(message: str, **kwargs: Any) -> None:
    """Quick critical log"""
    logger = await get_default_logger()
    await logger.critical(message, **kwargs)
