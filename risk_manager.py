import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import statistics

from config import CONFIG
from trading.signal_generator import TradingSignal, Direction
from utils.logger import setup_logger
from utils.helpers import calculate_usd_value, format_quantity, format_price

class RiskLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

@dataclass
class RiskMetrics:
    """Risk metrikleri için data class"""
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    volatility: float
    exposure: float
    daily_pnl: float
    timestamp: str

@dataclass
class PortfolioState:
    """Portfolio durumu"""
    total_balance: float
    available_balance: float
    total_exposure: float
    open_positions: int
    daily_pnl: float
    unrealized_pnl: float
    risk_score: float

class RiskManager:
    """Advanced risk management system for Ultra Liquidation Bot"""
    
    # Risk parametreleri
    MAX_RISK_PER_TRADE = 0.02  # %2
    MAX_DAILY_RISK = 0.10      # %10
    MAX_DRAWDOWN = 0.15        # %15
    MIN_CONFIDENCE = 0.7       # 70%
    MIN_POSITION_SIZE = 10.0   # $10
    MAX_POSITION_SIZE_RATIO = 0.1  # %10
    
    def __init__(self, initial_balance: float = 10000.0):
        self.initial_balance = initial_balance
        self.current_balance = initial_balance
        self.available_balance = initial_balance
        self.total_exposure = 0.0
        self.open_positions: Dict[str, Dict] = {}
        self.trade_history: List[Dict] = []
        self.daily_metrics: Dict = {}
        self.is_monitoring = False
        self.portfolio_value_history: List[float] = [initial_balance]
        self.daily_trades: List[Dict] = []
        self.logger = None
        
        # Risk metrik cache
        self.risk_metrics_cache = {}
        self.last_calculation_time = None
        
    async def initialize(self) -> None:
        """Risk manager'ı başlat"""
        self.logger = await setup_logger("risk_manager")
        self.is_monitoring = True
        
        # Risk monitoring task'ını başlat
        asyncio.create_task(self.monitor_risk())
        
        await self.logger.info(
            "Risk Manager initialized",
            initial_balance=self.initial_balance,
            max_risk_per_trade=self.MAX_RISK_PER_TRADE,
            max_daily_risk=self.MAX_DAILY_RISK
        )
        
    async def monitor_risk(self) -> None:
        """Real-time risk takibi"""
        await self.logger.info("Starting risk monitoring")
        
        while self.is_monitoring:
            try:
                portfolio_risk = await self.analyze_portfolio_risk()
                risk_level = self._determine_risk_level(portfolio_risk)
                
                if risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
                    await self.execute_risk_controls(risk_level, portfolio_risk)
                    
                # Her 30 saniyede bir risk metriklerini güncelle
                await self._update_risk_metrics()
                
                await asyncio.sleep(30)
                
            except Exception as e:
                await self.logger.error("Risk monitoring error", error=str(e))
                await asyncio.sleep(60)  # Hata durumunda 1 dakika bekle
                
    async def calculate_position_size(self, signal: TradingSignal, 
                                   current_price: float) -> Dict[str, Any]:
        """Pozisyon büyüklüğünü hesapla - Kelly criterion + volatility adjustment"""
        try:
            # 1. Kelly criterion ile temel pozisyon büyüklüğü
            kelly_fraction = await self._calculate_kelly_position(signal)
            
            # 2. Volatility adjustment
            volatility_factor = await self._calculate_volatility_adjusted_size(signal.symbol)
            
            # 3. Portfolio risk adjustment
            portfolio_risk_factor = await self._calculate_portfolio_risk_adjustment()
            
            # 4. Correlation risk adjustment
            correlation_factor = await self._calculate_correlation_risk(signal.symbol)
            
            # 5. Temel pozisyon büyüklüğü (risk bazlı)
            risk_amount = self.current_balance * self.MAX_RISK_PER_TRADE
            
            # Stop loss distance hesapla
            stop_loss_distance = await self._calculate_stop_loss_distance(signal, current_price)
            if stop_loss_distance <= 0:
                await self.logger.warning("Invalid stop loss distance", symbol=signal.symbol)
                return await self._get_default_position_size(current_price)
                
            base_size = risk_amount / stop_loss_distance
            
            # 6. Tüm faktörleri uygula
            adjusted_size = base_size * kelly_fraction * volatility_factor * portfolio_risk_factor * correlation_factor
            
            # 7. Limitleri uygula
            max_size = self.current_balance * self.MAX_POSITION_SIZE_RATIO
            final_size = max(self.MIN_POSITION_SIZE, min(adjusted_size, max_size))
            
            # 8. Risk detaylarını hazırla
            position_info = {
                'size': final_size,
                'usd_value': final_size * current_price,
                'risk_percentage': (final_size * current_price / self.current_balance) * 100,
                'risk_amount': risk_amount,
                'stop_loss_distance': stop_loss_distance,
                'components': {
                    'kelly_fraction': kelly_fraction,
                    'volatility_factor': volatility_factor,
                    'portfolio_risk_factor': portfolio_risk_factor,
                    'correlation_factor': correlation_factor,
                    'base_size': base_size
                }
            }
            
            await self.logger.info(
                "Position size calculated",
                symbol=signal.symbol,
                size=final_size,
                usd_value=position_info['usd_value'],
                risk_percent=position_info['risk_percentage']
            )
            
            return position_info
            
        except Exception as e:
            await self.logger.error("Position size calculation failed", error=str(e), symbol=signal.symbol)
            return await self._get_default_position_size(current_price)
            
    async def validate_signal_risk(self, signal: TradingSignal, 
                                 position_info: Dict[str, Any]) -> Dict[str, Any]:
        """Sinyal risk doğrulaması yap"""
        try:
            validation_results = {}
            
            # 1. Trade risk kontrolü (%2 limit)
            trade_risk_ok = position_info['risk_percentage'] <= (self.MAX_RISK_PER_TRADE * 100)
            validation_results['trade_risk'] = trade_risk_ok
            
            # 2. Daily risk kontrolü (%10 limit)
            daily_risk_ok = await self._check_daily_risk_limit(position_info['risk_amount'])
            validation_results['daily_risk'] = daily_risk_ok
            
            # 3. Exposure limit kontrolü
            exposure_ok = await self._check_exposure_limit(signal.symbol, position_info['usd_value'])
            validation_results['exposure'] = exposure_ok
            
            # 4. Drawdown limit kontrolü
            drawdown_ok = await self._check_max_drawdown()
            validation_results['drawdown'] = drawdown_ok
            
            # 5. Position limit kontrolü
            position_ok = await self._check_position_limit(signal.symbol, position_info['size'])
            validation_results['position_limit'] = position_ok
            
            # 6. Confidence threshold kontrolü
            confidence_ok = signal.confidence >= self.MIN_CONFIDENCE
            validation_results['confidence'] = confidence_ok
            
            # 7. Risk/reward ratio kontrolü (min 1:1)
            risk_reward_ok = signal.risk_reward_ratio >= 1.0 if signal.risk_reward_ratio else False
            validation_results['risk_reward'] = risk_reward_ok
            
            # Toplam validation
            all_checks_passed = all(validation_results.values())
            validation_results['overall'] = all_checks_passed
            
            if not all_checks_passed:
                failed_checks = [k for k, v in validation_results.items() if not v and k != 'overall']
                await self.logger.warning(
                    "Risk validation failed",
                    symbol=signal.symbol,
                    failed_checks=failed_checks,
                    confidence=signal.confidence
                )
            else:
                await self.logger.info(
                    "Risk validation passed",
                    symbol=signal.symbol,
                    risk_percent=position_info['risk_percentage']
                )
                
            return validation_results
            
        except Exception as e:
            await self.logger.error("Signal risk validation failed", error=str(e), symbol=signal.symbol)
            return {'overall': False, 'error': str(e)}
            
    async def monitor_portfolio_risk(self) -> PortfolioState:
        """Portfolio risk takibi"""
        try:
            # Total exposure hesapla
            total_exposure = sum(pos.get('exposure', 0) for pos in self.open_positions.values())
            
            # Unrealized PnL hesapla
            unrealized_pnl = await self._calculate_unrealized_pnl()
            
            # Daily PnL hesapla
            daily_pnl = await self._calculate_daily_pnl()
            
            # Risk score hesapla
            risk_score = await self._calculate_portfolio_risk_score()
            
            portfolio_state = PortfolioState(
                total_balance=self.current_balance,
                available_balance=self.available_balance,
                total_exposure=total_exposure,
                open_positions=len(self.open_positions),
                daily_pnl=daily_pnl,
                unrealized_pnl=unrealized_pnl,
                risk_score=risk_score
            )
            
            return portfolio_state
            
        except Exception as e:
            await self.logger.error("Portfolio risk monitoring failed", error=str(e))
            return PortfolioState(0, 0, 0, 0, 0, 0, 0.0)
            
    async def execute_risk_controls(self, risk_level: RiskLevel, 
                                 portfolio_risk: PortfolioState) -> None:
        """Risk kontrollerini uygula"""
        try:
            if risk_level == RiskLevel.HIGH:
                await self.logger.warning(
                    "High risk level detected - reducing exposure by 50%",
                    risk_score=portfolio_risk.risk_score
                )
                await self._reduce_exposure(0.5)
                
            elif risk_level == RiskLevel.CRITICAL:
                await self.logger.error(
                    "Critical risk level detected - emergency measures",
                    risk_score=portfolio_risk.risk_score
                )
                await self._close_all_positions()
                await self._suspend_trading()
                
        except Exception as e:
            await self.logger.error("Risk controls execution failed", error=str(e))
            

    async def _calculate_simple_risk_score(self, total_exposure: float) -> float:
        """Recursive olmayan basit risk skoru"""
        try:
            exposure_ratio = total_exposure / self.current_balance if self.current_balance > 0 else 0
            drawdown = await self.calculate_max_drawdown()
            
            # Basit weighted risk score
            risk_score = (exposure_ratio * 0.4) + (drawdown * 0.3) + (0.3 * 0.3)
            return min(1.0, max(0.0, risk_score))
        except Exception as e:
            await self.logger.error("Simple risk score calculation failed", error=str(e))
            return 0.5

    async def analyze_portfolio_risk(self) -> Dict[str, Any]:
        """Portfolio risk analizi - recursive call fix"""
        try:
            # Direkt hesapla, monitor_portfolio_risk'ü çağırma
            total_exposure = sum(pos.get('exposure', 0) for pos in self.open_positions.values())
            unrealized_pnl = await self._calculate_unrealized_pnl()
            daily_pnl = await self._calculate_daily_pnl()
            
            # Risk score'u direkt hesapla
            risk_score = await self._calculate_simple_risk_score(total_exposure)
            
            risk_analysis = {
                'current_balance': self.current_balance,
                'exposure_ratio': total_exposure / self.current_balance if self.current_balance > 0 else 0,
                'drawdown': await self.calculate_max_drawdown(),
                'daily_risk_utilization': await self._calculate_daily_risk_utilization(),
                'position_concentration': await self._calculate_position_concentration(),
                'correlation_risk': await self._calculate_correlation_risk(),
                'liquidity_risk': await self._calculate_liquidity_risk(),
                'volatility_risk': await self._calculate_volatility_risk(),
                'overall_risk_score': risk_score,
                'timestamp': datetime.now().isoformat()
            }
            
            return risk_analysis
            
        except Exception as e:
            await self.logger.error("Portfolio risk analysis failed", error=str(e))
            return {}
            
    async def calculate_risk_metrics(self) -> RiskMetrics:
        """Risk metriklerini hesapla"""
        try:
            if len(self.trade_history) < 2:
                return RiskMetrics(0, 0, 0, 0, 0, 0, 0, datetime.now().isoformat())
                
            # Trade returns hesapla
            returns = [trade.get('pnl_percent', 0) for trade in self.trade_history if 'pnl_percent' in trade]
            
            if len(returns) < 2:
                return RiskMetrics(0, 0, 0, 0, 0, 0, 0, datetime.now().isoformat())
                
            returns_series = pd.Series(returns)
            
            # Sharpe Ratio (basit versiyon - risk-free rate = 0)
            sharpe = returns_series.mean() / returns_series.std() if returns_series.std() > 0 else 0
            
            # Max Drawdown
            cumulative = (1 + returns_series).cumprod()
            running_max = cumulative.expanding().max()
            drawdown = (cumulative - running_max) / running_max
            max_drawdown = drawdown.min()
            
            # Win Rate
            winning_trades = len([r for r in returns if r > 0])
            win_rate = winning_trades / len(returns)
            
            # Profit Factor
            gross_profit = sum([r for r in returns if r > 0])
            gross_loss = abs(sum([r for r in returns if r < 0]))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
            
            # Volatility (annualized)
            volatility = returns_series.std() * np.sqrt(365)
            
            # Current exposure
            current_exposure = sum(pos.get('exposure', 0) for pos in self.open_positions.values())
            exposure_ratio = current_exposure / self.current_balance
            
            # Daily PnL
            daily_pnl = await self._calculate_daily_pnl()
            
            metrics = RiskMetrics(
                sharpe_ratio=float(sharpe),
                max_drawdown=float(max_drawdown),
                win_rate=float(win_rate),
                profit_factor=float(profit_factor) if profit_factor != float('inf') else 10.0,
                volatility=float(volatility),
                exposure=float(exposure_ratio),
                daily_pnl=float(daily_pnl),
                timestamp=datetime.now().isoformat()
            )
            
            await self.logger.info(
                "Risk metrics calculated",
                sharpe_ratio=metrics.sharpe_ratio,
                max_drawdown=metrics.max_drawdown,
                win_rate=metrics.win_rate
            )
            
            return metrics
            
        except Exception as e:
            await self.logger.error("Risk metrics calculation failed", error=str(e))
            return RiskMetrics(0, 0, 0, 0, 0, 0, 0, datetime.now().isoformat())
    
    # 🎯 RISK CALCULATION METHODS - TAM IMPLEMENTASYON
    
    async def _calculate_kelly_position(self, signal: TradingSignal) -> float:
        """Kelly criterion ile pozisyon büyüklüğü hesapla"""
        try:
            win_probability = signal.confidence
            loss_probability = 1 - win_probability
            risk_reward = signal.risk_reward_ratio or 2.0
            
            if risk_reward <= 0:
                return 0.1  # Default minimum
                
            # Kelly formülü: f = (bp - q) / b
            kelly_fraction = (win_probability * risk_reward - loss_probability) / risk_reward
            
            # Conservative Kelly: fractional (1/4)
            conservative_fraction = kelly_fraction * 0.25
            
            # Limit between 10% and 25%
            return max(0.1, min(conservative_fraction, 0.25))
            
        except Exception as e:
            await self.logger.error("Kelly position calculation failed", error=str(e))
            return 0.1  # Fallback
            
    async def _calculate_volatility_adjusted_size(self, symbol: str) -> float:
        """Volatility adjustment factor hesapla"""
        try:
            # Basit volatility hesaplama (gerçekte historical data kullanılır)
            # BTC için ~2%, altcoinler için ~5% varsayalım
            base_volatilities = {
                'BTCUSDT': 0.02,
                'ETHUSDT': 0.03,
                'ADAUSDT': 0.05,
                'DOTUSDT': 0.05,
                'LINKUSDT': 0.04
            }
            
            current_volatility = base_volatilities.get(symbol, 0.04)  # Default 4%
            
            # Yüksek volatility → küçük pozisyon
            # 2% volatility = 1.0 factor, 5% volatility = 0.5 factor
            volatility_factor = 0.02 / current_volatility
            
            return max(0.3, min(volatility_factor, 1.5))  # Min 30%, max 150%
            
        except Exception as e:
            await self.logger.error("Volatility adjustment calculation failed", error=str(e))
            return 1.0  # Fallback
            
    async def _calculate_portfolio_risk_adjustment(self) -> float:
        """Portfolio risk adjustment factor hesapla"""
        try:
            current_drawdown = await self.calculate_max_drawdown()
            
            # Drawdown arttıkça pozisyon büyüklüğünü azalt
            if current_drawdown <= 0.05:  # %5'ten az drawdown
                return 1.0
            elif current_drawdown <= 0.10:  # %10'dan az drawdown
                return 0.8
            elif current_drawdown <= 0.15:  # %15'ten az drawdown
                return 0.5
            else:  # %15'ten fazla drawdown
                return 0.3
                
        except Exception as e:
            await self.logger.error("Portfolio risk adjustment calculation failed", error=str(e))
            return 1.0
            
    async def _calculate_correlation_risk(self, symbol: str) -> float:
        """Correlation risk adjustment factor hesapla"""
        try:
            # Basit correlation hesaplama
            # Aynı asset class'taki symbol'ler için correlation yüksek
            bitcoin_symbols = ['BTCUSDT']
            ethereum_symbols = ['ETHUSDT']
            altcoin_symbols = ['ADAUSDT', 'DOTUSDT', 'LINKUSDT']
            
            # Mevcut pozisyonlardaki symbol'leri kontrol et
            current_symbols = list(self.open_positions.keys())
            
            if symbol in bitcoin_symbols and any(s in bitcoin_symbols for s in current_symbols):
                return 0.7  # Yüksek correlation
            elif symbol in ethereum_symbols and any(s in ethereum_symbols for s in current_symbols):
                return 0.8
            elif symbol in altcoin_symbols and any(s in altcoin_symbols for s in current_symbols):
                return 0.6  # Çok yüksek correlation
            else:
                return 1.0  # Düşük correlation
                
        except Exception as e:
            await self.logger.error("Correlation risk calculation failed", error=str(e))
            return 1.0
            
    async def _calculate_stop_loss_distance(self, signal: TradingSignal, current_price: float) -> float:
        """Stop loss distance hesapla"""
        try:
            if signal.direction == Direction.LONG:
                return current_price - signal.stop_loss
            else:
                return signal.stop_loss - current_price
        except Exception as e:
            await self.logger.error("Stop loss distance calculation failed", error=str(e))
            return current_price * 0.01  %1
    
    # ⚠️ RISK LIMIT METHODS - TAM IMPLEMENTASYON
    
    async def _check_daily_risk_limit(self, risk_amount: float) -> bool:
        """Günlük risk limiti kontrolü"""
        try:
            daily_risk_limit = self.current_balance * self.MAX_DAILY_RISK
            today_risk = await self._get_today_risk()
            return (today_risk + risk_amount) <= daily_risk_limit
        except Exception as e:
            await self.logger.error("Daily risk limit check failed", error=str(e))
            return False
            
    async def _check_exposure_limit(self) -> bool:
        """Toplam maruziyet limiti kontrolü"""
        try:
            total_exposure = sum(pos.get('exposure', 0) for pos in self.open_positions.values())
            exposure_limit = self.current_balance * 0.3  # %30 exposure limit
            return total_exposure <= exposure_limit
        except Exception as e:
            await self.logger.error("Exposure limit check failed", error=str(e))
            return False
            
    async def _check_max_drawdown(self) -> bool:
        """Maksimum drawdown limiti kontrolü"""
        try:
            current_drawdown = await self.calculate_max_drawdown()
            return current_drawdown <= self.MAX_DRAWDOWN
        except Exception as e:
            await self.logger.error("Max drawdown check failed", error=str(e))
            return False
            
    async def _check_position_limit(self, symbol: str, position_size: float) -> bool:
        """Symbol bazlı pozisyon limiti kontrolü"""
        try:
            symbol_exposure = sum(
                pos.get('exposure', 0) for pos in self.open_positions.values() 
                if pos.get('symbol') == symbol
            )
            max_symbol_exposure = self.current_balance * 0.05  # %5 per symbol
            return (symbol_exposure + position_size) <= max_symbol_exposure
        except Exception as e:
            await self.logger.error("Position limit check failed", error=str(e))
            return False
    
    # 📈 RISK METRIC METHODS - TAM IMPLEMENTASYON
    
    async def calculate_sharpe_ratio(self) -> float:
        """Sharpe oranı hesapla"""
        try:
            metrics = await self.calculate_risk_metrics()
            return metrics.sharpe_ratio
        except Exception as e:
            await self.logger.error("Sharpe ratio calculation failed", error=str(e))
            return 0.0
            
    async def calculate_max_drawdown(self) -> float:
        """Maksimum drawdown hesapla"""
        try:
            if len(self.portfolio_value_history) < 2:
                return 0.0
                
            peak = max(self.portfolio_value_history)
            current = self.portfolio_value_history[-1]
            drawdown = (peak - current) / peak if peak > 0 else 0.0
            
            return max(0.0, drawdown)
        except Exception as e:
            await self.logger.error("Max drawdown calculation failed", error=str(e))
            return 0.0
            
    async def calculate_win_rate(self) -> float:
        """Win rate hesapla"""
        try:
            metrics = await self.calculate_risk_metrics()
            return metrics.win_rate
        except Exception as e:
            await self.logger.error("Win rate calculation failed", error=str(e))
            return 0.0
            
    async def calculate_exposure_ratio(self) -> float:
        """Maruziyet oranı hesapla"""
        try:
            total_exposure = sum(pos.get('exposure', 0) for pos in self.open_positions.values())
            return total_exposure / self.current_balance if self.current_balance > 0 else 0.0
        except Exception as e:
            await self.logger.error("Exposure ratio calculation failed", error=str(e))
            return 0.0
    
    # 🔧 HELPER METHODS - TAM IMPLEMENTASYON
    
    async def _get_portfolio_value(self) -> float:
        """Portfolio değerini getir"""
        return self.current_balance
        
    async def _get_current_exposure(self) -> float:
        """Mevcut maruziyeti getir"""
        return sum(pos.get('exposure', 0) for pos in self.open_positions.values())
        
    async def _get_daily_pnl(self) -> float:
        """Günlük P&L hesapla"""
        try:
            today = datetime.now().date()
            daily_trades = [
                trade for trade in self.trade_history 
                if trade.get('timestamp', datetime.now()).date() == today
            ]
            return sum(trade.get('pnl', 0) for trade in daily_trades)
        except Exception as e:
            await self.logger.error("Daily PnL calculation failed", error=str(e))
            return 0.0
            
    async def _update_risk_metrics(self) -> None:
        """Risk metriklerini güncelle"""
        try:
            self.portfolio_value_history.append(self.current_balance)
            # Son 100 değeri tut
            if len(self.portfolio_value_history) > 100:
                self.portfolio_value_history.pop(0)
        except Exception as e:
            await self.logger.error("Risk metrics update failed", error=str(e))
    
    async def _get_today_risk(self) -> float:
        """Bugünkü toplam risk"""
        try:
            today = datetime.now().date()
            today_risk = 0.0
            
            for trade in self.trade_history:
                trade_time = trade.get('timestamp')
                if isinstance(trade_time, datetime) and trade_time.date() == today:
                    today_risk += trade.get('risk_amount', 0)
                elif isinstance(trade_time, str):
                    try:
                        trade_datetime = datetime.fromisoformat(trade_time.replace('Z', '+00:00'))
                        if trade_datetime.date() == today:
                            today_risk += trade.get('risk_amount', 0)
                    except ValueError:
                        continue
            
            return today_risk
            
        except Exception as e:
            await self.logger.error("Today's risk calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_unrealized_pnl(self) -> float:
        """Açık pozisyonların unrealized PnL'si"""
        try:
            unrealized_pnl = 0.0
            current_prices = {'BTCUSDT': 45000.0, 'ETHUSDT': 2500.0}  # Mock prices
            
            for symbol, position in self.open_positions.items():
                entry_price = position.get('entry_price', 0)
                current_price = current_prices.get(symbol, entry_price)
                quantity = position.get('quantity', 0)
                side = position.get('side', 'LONG')
                
                if side == 'LONG':
                    unrealized_pnl += (current_price - entry_price) * quantity
                else:
                    unrealized_pnl += (entry_price - current_price) * quantity
                    
            return unrealized_pnl
            
        except Exception as e:
            await self.logger.error("Unrealized PnL calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_daily_risk_utilization(self) -> float:
        """Günlük risk kullanım oranı"""
        try:
            daily_risk_limit = self.current_balance * self.MAX_DAILY_RISK
            today_risk = await self._get_today_risk()
            return today_risk / daily_risk_limit if daily_risk_limit > 0 else 0.0
        except Exception as e:
            await self.logger.error("Daily risk utilization calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_position_concentration(self) -> float:
        """Pozisyon konsantrasyon riski"""
        try:
            if not self.open_positions:
                return 0.0
                
            symbol_exposures = {}
            for position in self.open_positions.values():
                symbol = position.get('symbol', 'UNKNOWN')
                symbol_exposures[symbol] = symbol_exposures.get(symbol, 0) + position.get('exposure', 0)
                
            if not symbol_exposures:
                return 0.0
                
            # Herfindahl-Hirschman Index (HHI)
            total_exposure = sum(symbol_exposures.values())
            concentrations = [(exp / total_exposure) ** 2 for exp in symbol_exposures.values()]
            hhi = sum(concentrations)
            
            return hhi
        except Exception as e:
            await self.logger.error("Position concentration calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_correlation_risk(self) -> float:
        """Korelasyon riski"""
        try:
            # Basit correlation risk hesaplama
            symbol_count = len(set(pos.get('symbol') for pos in self.open_positions.values()))
            total_positions = len(self.open_positions)
            
            if total_positions == 0:
                return 0.0
                
            # Symbol çeşitliliği azaldıkça correlation riski artar
            diversification_ratio = symbol_count / total_positions
            correlation_risk = 1.0 - diversification_ratio
            
            return max(0.0, min(correlation_risk, 1.0))
        except Exception as e:
            await self.logger.error("Correlation risk calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_liquidity_risk(self) -> float:
        """Likidite riski"""
        try:
            # Basit liquidity risk hesaplama
            total_exposure = await self._get_current_exposure()
            liquidity_ratio = self.available_balance / self.current_balance if self.current_balance > 0 else 0.0
            
            # Available balance azaldıkça liquidity risk artar
            liquidity_risk = 1.0 - liquidity_ratio
            
            return max(0.0, min(liquidity_risk, 1.0))
        except Exception as e:
            await self.logger.error("Liquidity risk calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_volatility_risk(self) -> float:
        """Volatilite riski"""
        try:
            # Basit volatility risk hesaplama
            if len(self.portfolio_value_history) < 2:
                return 0.0
                
            returns = []
            for i in range(1, len(self.portfolio_value_history)):
                prev_value = self.portfolio_value_history[i-1]
                curr_value = self.portfolio_value_history[i]
                if prev_value > 0:
                    returns.append((curr_value - prev_value) / prev_value)
                    
            if len(returns) < 2:
                return 0.0
                
            volatility = statistics.stdev(returns)
            return min(volatility * 10, 1.0)  # Scale to 0-1 range
        except Exception as e:
            await self.logger.error("Volatility risk calculation failed", error=str(e))
            return 0.0
            
    async def _calculate_portfolio_risk_score(self) -> float:
        """Portfolio risk skoru hesapla"""
        try:
            risk_factors = await self.analyze_portfolio_risk()
            
            weights = {
                'exposure_ratio': 0.25,
                'drawdown': 0.20,
                'daily_risk_utilization': 0.15,
                'position_concentration': 0.15,
                'volatility_risk': 0.10,
                'liquidity_risk': 0.10,
                'correlation_risk': 0.05
            }
            
            risk_score = 0.0
            for factor, weight in weights.items():
                value = risk_factors.get(factor, 0)
                risk_score += value * weight
                
            return min(1.0, max(0.0, risk_score))
        except Exception as e:
            await self.logger.error("Portfolio risk score calculation failed", error=str(e))
            return 0.5  # Neutral risk
            
    def _determine_risk_level(self, risk_analysis: Dict[str, Any]) -> RiskLevel:
        """Risk seviyesini belirle"""
        risk_score = risk_analysis.get('overall_risk_score', 0)
        
        if risk_score >= 0.8:
            return RiskLevel.CRITICAL
        elif risk_score >= 0.6:
            return RiskLevel.HIGH
        elif risk_score >= 0.4:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
            
    async def _reduce_exposure(self, reduction_factor: float) -> None:
        """Exposure'ı azalt"""
        await self.logger.warning(f"Reducing exposure by {reduction_factor:.0%}")
        # Gerçek implementasyon için trading engine entegrasyonu gerekli
        
    async def _close_all_positions(self) -> None:
        """Tüm pozisyonları kapat"""
        await self.logger.error("Closing all positions due to critical risk")
        # Gerçek implementasyon için trading engine entegrasyonu gerekli
        
    async def _suspend_trading(self) -> None:
        """Trading'i askıya al"""
        await self.logger.critical("Trading suspended due to critical risk")
        self.is_monitoring = False
        
    async def _get_default_position_size(self, current_price: float) -> Dict[str, Any]:
        """Default pozisyon büyüklüğü"""
        default_size = self.current_balance * 0.01 / current_price  # %1 risk
        return {
            'size': default_size,
            'usd_value': default_size * current_price,
            'risk_percentage': 1.0,
            'risk_amount': self.current_balance * 0.01,
            'stop_loss_distance': current_price * 0.01,
            'components': {'fallback': True}
        }
        
    async def record_trade(self, trade_data: Dict[str, Any]) -> None:
        """Trade kaydı ekle"""
        self.trade_history.append(trade_data)
        
        if 'pnl' in trade_data:
            self.current_balance += trade_data['pnl']
            self.available_balance = self.current_balance - self.total_exposure
            
        await self.logger.info(
            "Trade recorded",
            symbol=trade_data.get('symbol'),
            pnl=trade_data.get('pnl', 0),
            new_balance=self.current_balance
        )
        
    async def update_position(self, symbol: str, position_data: Dict[str, Any]) -> None:
        """Pozisyon güncelle"""
        self.open_positions[symbol] = position_data
        await self.logger.debug("Position updated", symbol=symbol)
        
    async def get_risk_report(self) -> Dict[str, Any]:
        """Risk raporu oluştur"""
        portfolio_state = await self.monitor_portfolio_risk()
        risk_metrics = await self.calculate_risk_metrics()
        risk_analysis = await self.analyze_portfolio_risk()
        
        report = {
            'portfolio_state': portfolio_state.__dict__,
            'risk_metrics': risk_metrics.__dict__,
            'risk_analysis': risk_analysis,
            'open_positions': len(self.open_positions),
            'trade_count': len(self.trade_history),
            'timestamp': datetime.now().isoformat()
        }
        
        await self.logger.info("Risk report generated", open_positions=len(self.open_positions))
        
        return report
        

    async def _calculate_daily_pnl(self) -> float:
        """Günlük P&L hesapla"""
        try:
            today = datetime.now().date()
            daily_trades = []
            
            for trade in self.trade_history:
                trade_time = trade.get('timestamp')
                if isinstance(trade_time, datetime) and trade_time.date() == today:
                    daily_trades.append(trade)
                elif isinstance(trade_time, str):
                    # String timestamp'i datetime'a çevir
                    try:
                        trade_datetime = datetime.fromisoformat(trade_time.replace('Z', '+00:00'))
                        if trade_datetime.date() == today:
                            daily_trades.append(trade)
                    except ValueError:
                        continue
            
            daily_pnl = sum(trade.get('pnl', 0) for trade in daily_trades)
            return daily_pnl
            
        except Exception as e:
            await self.logger.error("Daily PnL calculation failed", error=str(e))
            return 0.0



    async def stop_monitoring(self) -> None:
        """Risk monitoring'ı durdur"""
        self.is_monitoring = False
        await self.logger.info("Risk Manager stopped", status="stopped")
