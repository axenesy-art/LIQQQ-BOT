#!/usr/bin/env python3
"""
Ultra Liquidation Bot - Main Entry Point
Advanced liquidation detection and AI-powered trading bot
"""

import asyncio
import signal
import sys
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import numpy as np  # EKSİK IMPORT EKLENDİ


from config import CONFIG
from data.data_collector import DataCollector
from data.data_storage import DataStorage
from analysis.whale_tracker import WhaleTracker
from analysis.liquidation_analyzer import LiquidationAnalyzer
from analysis.ai_predictor import AIPredictor
from trading.signal_generator import SignalGenerator
from trading.risk_manager import RiskManager
from alerts.telegram_bot import TelegramBot
from utils.logger import setup_logger, BotLogger
from utils.helpers import format_quantity, calculate_usd_value

# Basit logger import - kompleks logger'dan kaçın
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s')


# ==================== WINDOWS UNICODE FIX ====================
if sys.platform == "win32":
    try:
        import io
        # Force UTF-8 encoding for Windows console
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception as e:
        print(f"Windows encoding fix failed: {e}")

# ==================== BASIC LOGGING SETUP ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True  # Override any existing config
)

# ==================== GLOBAL CONFIG FIX ====================
# Eğer CONFIG yoksa, basit bir config oluştur
try:
    from config import CONFIG
except ImportError:
    class DummyConfig:
        def get(self, key, default=None):
            return default
        def __getattr__(self, name):
            return None
    CONFIG = DummyConfig()

print("🚀 ULTRA LIQUIDATION BOT - FIXED VERSION STARTING")
print("==================================================")

class UltraLiquidationBot:
    """Main bot class that integrates all modules"""
    
    def __init__(self):
        self.is_running = False
        self.start_time = None
        self.performance_metrics = {}
        
        self.data_storage = None
        self.data_collector = None
        self.whale_tracker = None
        self.liquidation_analyzer = None
        self.ai_predictor = None
        self.signal_generator = None
        self.risk_manager = None
        self.telegram_bot = None
        
        self.module_status = {
            'data_collector': False,
            'data_storage': False,
            'whale_tracker': False,
            'liquidation_analyzer': False,
            'ai_predictor': False,
            'signal_generator': False,
            'risk_manager': False,
            'telegram_bot': False
        }
        
        self.logger = None
        
    async def initialize(self) -> None:
        """Initialize all bot modules in correct order"""
        self.logger = await setup_logger("main")
            
        await self.logger.info("ULTRA LIQUIDATION BOT INITIALIZATION STARTED", phase="starting")
            
        self.start_time = datetime.now()
        self.is_running = True
            
        # Phase 1: Data Storage
        await self.logger.info("Initializing Data Storage", phase="1")
        self.data_storage = DataStorage()
        await self.data_storage.initialize()
        self.module_status['data_storage'] = True
        
        # Phase 2: Data Collector
        await self.logger.info("Initializing Data Collector", phase="2")
        self.data_collector = DataCollector()
        await self.data_collector.initialize()
        self.module_status['data_collector'] = True
        
        # Phase 3: Analysis Modules
        await self.logger.info("Initializing Analysis Modules", phase="3")
        
        self.whale_tracker = WhaleTracker(self.data_storage)
        await self.whale_tracker.initialize()
        self.module_status['whale_tracker'] = True
        
        self.liquidation_analyzer = LiquidationAnalyzer(self.data_storage)
        await self.liquidation_analyzer.initialize()
        self.module_status['liquidation_analyzer'] = True
        
        self.ai_predictor = AIPredictor(self.data_storage)
        await self.ai_predictor.initialize()
        self.module_status['ai_predictor'] = True
        
        # Phase 4: Trading Modules
        await self.logger.info("Initializing Trading Modules", phase="4")
        
        self.signal_generator = SignalGenerator(
            self.whale_tracker,
            self.liquidation_analyzer,
            self.ai_predictor
        )
        await self.signal_generator.initialize()
        self.module_status['signal_generator'] = True
        
        self.risk_manager = RiskManager()
        await self.risk_manager.initialize()
        self.module_status['risk_manager'] = True
        
        # Phase 5: Alert System
        await self.logger.info("Initializing Alert System", phase="5")
        
        self.telegram_bot = TelegramBot()
        await self.telegram_bot.initialize()
        self.module_status['telegram_bot'] = True
        
        await self._send_startup_notification()
        
        await self.logger.info("ALL MODULES INITIALIZED SUCCESSFULLY", 
                              modules_ready=sum(self.module_status.values()),
                              total_modules=len(self.module_status))
        
    async def start(self) -> None:
        """Start the main bot loop"""
        await self.logger.info("STARTING ULTRA LIQUIDATION BOT", status="starting")
        
        tasks = []
        
        tasks.append(asyncio.create_task(self._start_data_pipeline()))
        tasks.append(asyncio.create_task(self._start_analysis_pipeline()))
        tasks.append(asyncio.create_task(self._start_trading_pipeline()))
        tasks.append(asyncio.create_task(self._start_monitoring()))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def _start_data_pipeline(self) -> None:
        """Start data collection pipeline"""
        await self.logger.info("Starting data pipeline", pipeline="data")
        
        try:
            await self.data_collector.start()
        except Exception as e:
            await self.logger.error("Data pipeline error", error=str(e))
            await self._handle_module_error('data_collector', e)
            
    async def _start_analysis_pipeline(self) -> None:
        """Start analysis pipeline"""
        await self.logger.info("Starting analysis pipeline", pipeline="analysis")
        
        try:
            await self.whale_tracker.start_tracking()
            await self.liquidation_analyzer.start_analysis()
            
            while self.is_running:
                await self._run_periodic_analysis()
                await asyncio.sleep(60)
                
        except Exception as e:
            await self.logger.error("Analysis pipeline error", error=str(e))
            await self._handle_module_error('analysis_modules', e)
            
    async def _start_trading_pipeline(self) -> None:
        """Start trading pipeline - COMPLETELY FIXED VERSION"""
        await self.logger.info("🚀 Starting trading pipeline", pipeline="trading")
        
        try:
            # Wait for initialization
            await asyncio.sleep(5)
            
            if not hasattr(self, 'signal_generator') or not self.signal_generator:
                await self.logger.error("❌ Signal generator not available!")
                return
            
            await self.logger.info("✅ Signal generator available, starting continuous loop")
            
            # Create dedicated signal generation task
            signal_task = asyncio.create_task(self._run_signal_generator_loop())
            
            # Monitor and restart if needed
            while self.is_running:
                if signal_task.done():
                    try:
                        # Check if task completed normally or with error
                        result = signal_task.result()
                        await self.logger.info("📊 Signal generator task completed normally")
                    except Exception as e:
                        await self.logger.error(f"❌ Signal generator task failed: {e}")
                    
                    # Restart the task
                    await self.logger.info("🔄 Restarting signal generator task...")
                    signal_task = asyncio.create_task(self._run_signal_generator_loop())
                
                await asyncio.sleep(10)  # Check every 10 seconds
            
        except Exception as e:
            await self.logger.error(f"❌ Trading pipeline error: {e}")        

    # ✅ BU YENİ METODU EKLE (aynı dosyaya):
    async def _run_signal_generator_loop(self):
        """Run signal generator in a dedicated loop - FIXED"""
        try:
            await self.logger.info("🚀 SIGNAL GENERATOR LOOP STARTED")
            
            while self.is_running:
                try:
                    # Generate signals
                    signals = await self.signal_generator.generate_signals()
                    
                    if signals:
                        await self.logger.info(f"🎯 Generated {len(signals)} signals")
                        
                        for signal in signals:
                            await self._process_signal(signal)
                    else:
                        await self.logger.debug("No signals generated this cycle")
                    
                    # Wait before next cycle
                    await asyncio.sleep(30)  # Check every 30 seconds
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    await self.logger.error(f"Signal generation error: {e}")
                    await asyncio.sleep(10)  # Shorter wait on error
                    
        except Exception as e:
            await self.logger.error(f"Signal generator loop crashed: {e}")

    async def _process_signal(self, signal):
        """Process generated trading signal"""
        await self.logger.info(f"📢 Processing signal: {signal}")
        # Buraya sinyal işleme kodu ekleyebilirsin

            
    async def _start_monitoring(self) -> None:
        """Start system monitoring"""
        await self.logger.info("Starting system monitoring", pipeline="monitoring")
        
        while self.is_running:
            await self._monitor_system_health()
            await self._update_performance_metrics()
            await asyncio.sleep(30)
            
    async def _run_periodic_analysis(self) -> None:
        """Run periodic analysis tasks"""
        try:
            symbols_to_analyze = CONFIG.LIQUIDATION_SYMBOLS[:3]
            
            for symbol in symbols_to_analyze:
                await self.liquidation_analyzer.generate_heatmap(symbol)
                pressure_data = await self.liquidation_analyzer.calculate_pressure_index(symbol)
                cascade_data = await self.liquidation_analyzer.detect_cascade_pattern(symbol)
                
                if pressure_data.get('pressure_index', 0) > 0.7:
                    await self._send_pressure_alert(symbol, pressure_data)
                    
                if cascade_data.get('cascade_detected', False):
                    await self._send_cascade_alert(symbol, cascade_data)
                    
            await self.logger.debug("Periodic analysis completed", symbols=len(symbols_to_analyze))
                    
        except Exception as e:
            await self.logger.error("Periodic analysis error", error=str(e))
            
    async def _monitor_system_health(self) -> None:
        """Monitor system health and module status"""
        try:
            health_status = {
                'timestamp': datetime.now().isoformat(),
                'uptime': (datetime.now() - self.start_time).total_seconds(),
                'modules_healthy': all(self.module_status.values()),
                'module_status': self.module_status.copy(),
                'performance': self.performance_metrics.copy()
            }
            
            healthy_modules = sum(self.module_status.values())
            total_modules = len(self.module_status)
            
            if healthy_modules < total_modules:
                health_status['status'] = 'DEGRADED'
                await self.logger.warning("System degraded", 
                                        healthy_modules=healthy_modules,
                                        total_modules=total_modules)
                
                await self._send_health_alert(health_status)
            else:
                health_status['status'] = 'HEALTHY'
                
            if int(time.time()) % 300 == 0:
                await self.logger.info("System health check", status=health_status['status'])
                
        except Exception as e:
            await self.logger.error("Health monitoring error", error=str(e))
            
    async def _update_performance_metrics(self) -> None:
        """Update performance metrics"""
        try:
            storage_metrics = await self._get_storage_metrics()
            analysis_metrics = await self._get_analysis_metrics()
            trading_metrics = await self._get_trading_metrics()
            
            self.performance_metrics = {
                'timestamp': datetime.now().isoformat(),
                'storage': storage_metrics,
                'analysis': analysis_metrics,
                'trading': trading_metrics,
                'system': {
                    'uptime': (datetime.now() - self.start_time).total_seconds(),
                    'memory_usage': await self._get_memory_usage(),
                    'cpu_usage': await self._get_cpu_usage()
                }
            }
            
            await self.logger.debug("Performance metrics updated")
            
        except Exception as e:
            await self.logger.error("Performance metrics update error", error=str(e))
            
    async def _get_storage_metrics(self) -> Dict[str, Any]:
        """Get data storage metrics"""
        try:
            stats = await self.data_storage.get_liquidation_stats(1)
            
            return {
                'liquidations_last_hour': stats.get('total_count', 0),
                'total_volume': stats.get('side_stats', {}).get('total_quantity', 0),
                'symbol_ranking': stats.get('symbol_ranking', [])
            }
        except Exception as e:
            await self.logger.error("Storage metrics error", error=str(e))
            return {}
            
    async def _get_analysis_metrics(self) -> Dict[str, Any]:
        """Get analysis metrics"""
        try:
            return {
                'whales_tracked': len(getattr(self.whale_tracker, 'whale_positions', {})),
                'heatmaps_generated': len(getattr(self.liquidation_analyzer, 'heatmap_data', {})),
                'predictions_made': len(getattr(self.ai_predictor, 'prediction_history', [])),
            }
        except Exception as e:
            await self.logger.error("Analysis metrics error", error=str(e))
            return {}
            
    async def _get_trading_metrics(self) -> Dict[str, Any]:
        """Get trading metrics"""
        try:
            active_signals = await self.signal_generator.get_active_signals()
            signal_history = await self.signal_generator.get_signal_history(10)
            
            return {
                'active_signals': len(active_signals),
                'signals_last_10': len(signal_history),
                'avg_confidence': np.mean([s.confidence for s in signal_history]) if signal_history else 0
            }
        except Exception as e:
            await self.logger.error("Trading metrics error", error=str(e))
            return {}
            
    async def _get_memory_usage(self) -> float:
        """Get memory usage"""
        return 0.0
        
    async def _get_cpu_usage(self) -> float:
        """Get CPU usage"""
        return 0.0
        
    async def _send_startup_notification(self) -> None:
        """Send startup notification"""
        try:
            if self.telegram_bot and self.module_status['telegram_bot']:
                await self.telegram_bot.send_system_alert(
                    "Ultra Liquidation Bot",
                    "HEALTHY",
                    "All modules initialized successfully",
                    performance=100
                )
                await self.logger.info("Startup notification sent")
        except Exception as e:
            await self.logger.error("Startup notification error", error=str(e))
            
    async def _send_pressure_alert(self, symbol: str, pressure_data: Dict[str, Any]) -> None:
        """Send liquidation pressure alert"""
        try:
            if self.telegram_bot:
                await self.telegram_bot.send_liquidation_alert(symbol, {
                    'side': 'UNKNOWN',
                    'quantity': 0,
                    'price': 0,
                    'usd_value': pressure_data.get('total_volume', 0),
                    'pressure_index': pressure_data.get('pressure_index', 0),
                })
        except Exception as e:
            await self.logger.error("Pressure alert error", error=str(e), symbol=symbol)
            
    async def _send_cascade_alert(self, symbol: str, cascade_data: Dict[str, Any]) -> None:
        """Send cascade detection alert"""
        try:
            if self.telegram_bot:
                await self.telegram_bot.send_liquidation_alert(symbol, {
                    'side': 'UNKNOWN',
                    'quantity': 0,
                    'price': 0,
                    'cascade': True,
                    'cascade_events': cascade_data.get('total_cascade_events', 0),
                })
        except Exception as e:
            await self.logger.error("Cascade alert error", error=str(e), symbol=symbol)
            
    async def _send_health_alert(self, health_status: Dict[str, Any]) -> None:
        """Send system health alert"""
        try:
            if self.telegram_bot:
                healthy_modules = sum(health_status['module_status'].values())
                total_modules = len(health_status['module_status'])
                
                await self.telegram_bot.send_system_alert(
                    "System Health",
                    "DEGRADED",
                    f"Modules healthy: {healthy_modules}/{total_modules}",
                    performance=(healthy_modules / total_modules) * 100
                )
        except Exception as e:
            await self.logger.error("Health alert error", error=str(e))
            
    async def _handle_initialization_error(self, error: Exception) -> None:
        """Handle initialization errors"""
        await self.logger.critical("Bot initialization failed", error=str(error))
        
        try:
            if self.telegram_bot:
                await self.telegram_bot.send_critical_alert(
                    "BOT INITIALIZATION FAILED",
                    f"Initialization error: {str(error)}",
                    "CRITICAL",
                    action_required="Check logs and restart bot"
                )
        except Exception as e:
            await self.logger.error("Error sending initialization alert", error=str(e))
    
    # ✅ ADD THIS METHOD TO PROCESS SIGNALS
    async def _process_trading_signal(self, signal: Dict):
        """Process individual trading signal"""
        try:
            symbol = signal.get('symbol', 'UNKNOWN')
            side = signal.get('side', 'unknown')
            confidence = signal.get('confidence', 0)
            
            await self.logger.info(
                f"📊 Processing signal: {symbol} {side.upper()} "
                f"(Confidence: {confidence:.2f})"
            )
            
            # Send to risk manager
            if hasattr(self, 'risk_manager') and self.risk_manager:
                risk_assessment = await self.risk_manager.assess_risk(signal)
                
                if risk_assessment.get('approved', False):
                    await self._execute_trade(signal, risk_assessment)
                else:
                    await self.logger.warning(f"🚫 Trade rejected: {risk_assessment.get('reason', 'Unknown')}")
            
            # Send alert
            if hasattr(self, 'telegram_bot') and self.telegram_bot:
                await self.telegram_bot.send_signal_alert(signal)
                
        except Exception as e:
            await self.logger.error(f"❌ Error processing signal: {e}")

    # ✅ ADD THIS METHOD FOR TRADE EXECUTION
    async def _execute_trade(self, signal: Dict, risk_assessment: Dict):
        """Execute approved trade"""
        try:
            symbol = signal['symbol']
            side = signal['side']
            
            await self.logger.info(f"🎯 EXECUTING TRADE: {symbol} {side.upper()}")
            
            # Here you would integrate with your exchange API
            # For now, just log the execution
            
            # Simulate trade execution
            execution_data = {
                'symbol': symbol,
                'side': side,
                'quantity': risk_assessment.get('position_size', 0),
                'price': risk_assessment.get('entry_price', 0),
                'timestamp': datetime.now().isoformat(),
                'status': 'SIMULATED'
            }
            
            await self.logger.info(f"✅ Trade executed: {execution_data}")
            
        except Exception as e:
            await self.logger.error(f"❌ Trade execution error: {e}")

    
    
    
    
            
    async def _handle_module_error(self, module_name: str, error: Exception) -> None:
        """Handle module errors"""
        await self.logger.error("Module error", module=module_name, error=str(error))
        self.module_status[module_name] = False
        
        try:
            if self.telegram_bot:
                await self.telegram_bot.send_system_alert(
                    f"Module {module_name}",
                    "ERROR",
                    f"Module error: {str(error)}",
                    performance=0
                )
        except Exception as e:
            await self.logger.error("Error sending module alert", module=module_name, error=str(e))
            
    async def stop(self) -> None:
        """Stop the bot gracefully"""
        self.is_running = False
        await self.logger.info("STOPPING ULTRA LIQUIDATION BOT", status="stopping")
        
        try:
            if self.signal_generator:
                await self.signal_generator.stop_generation()
                
            if self.risk_manager:
                await self.risk_manager.stop_monitoring()
                
            if self.liquidation_analyzer:
                await self.liquidation_analyzer.stop_analysis()
                
            if self.whale_tracker:
                await self.whale_tracker.stop_tracking()
                
            if self.data_collector:
                await self.data_collector.stop()
                
            if self.telegram_bot:
                await self.telegram_bot.stop()
                
            uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            
            await self.logger.info("Bot stopped gracefully", uptime=uptime)
            
            if self.telegram_bot:
                await self.telegram_bot.send_system_alert(
                    "Ultra Liquidation Bot",
                    "SHUTDOWN",
                    f"Bot stopped. Uptime: {uptime:.2f}s",
                    performance=0
                )
                
        except Exception as e:
            await self.logger.error("Error during shutdown", error=str(e))
            
    async def get_status(self) -> Dict[str, Any]:
        """Get current bot status"""
        status = {
            'is_running': self.is_running,
            'uptime': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'module_status': self.module_status,
            'performance_metrics': self.performance_metrics,
            'timestamp': datetime.now().isoformat()
        }
        
        await self.logger.debug("Status report generated")
        return status

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

async def main():
    """Ana uygulama"""
    bot = UltraLiquidationBot()
    
    try:
        await bot.initialize()
        await bot.start()
    except Exception as e:
        print(f"Critical error: {e}")
        return 1  # sys.exit yerine return

if __name__ == "__main__":
    # Basit sinyal handler
    def signal_handler(signum, frame):
        print("Shutting down...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("ULTRA LIQUIDATION BOT STARTING")
    print("=" * 50)
    
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
