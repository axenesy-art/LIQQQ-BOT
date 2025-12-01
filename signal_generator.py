import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import numpy as np

from config import CONFIG
from analysis.whale_tracker import WhaleTracker
from analysis.liquidation_analyzer import LiquidationAnalyzer
from analysis.ai_predictor import AIPredictor
from utils.logger import setup_logger
from utils.helpers import calculate_usd_value, format_quantity

class SignalType(Enum):
    WHALE_LIQUIDATION_SIGNAL = "WHALE_LIQUIDATION_SIGNAL"
    CASCADE_PREDICTION_SIGNAL = "CASCADE_PREDICTION_SIGNAL"
    AI_CONFIRMATION_SIGNAL = "AI_CONFIRMATION_SIGNAL"
    RISK_ADJUSTED_SIGNAL = "RISK_ADJUSTED_SIGNAL"
    HIGH_CONFIDENCE_SIGNAL = "HIGH_CONFIDENCE_SIGNAL"

class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"

@dataclass
class TradingSignal:
    """Trading sinyali data class"""
    
    symbol: str
    signal_type: SignalType
    direction: Direction
    confidence: float
    price_target: float
    stop_loss: float
    timestamp: str = None
    entry_price: Optional[float] = None
    position_size: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    time_frame: str = "15min"
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        # ✅ TIMESTAMP DEFAULT DEĞER - KRİTİK DÜZELTME
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
            
        if self.risk_reward_ratio is None:
            self.calculate_risk_reward()
            
    def calculate_risk_reward(self) -> None:
        """Risk/reward oranını hesapla"""
        if self.entry_price and self.price_target and self.stop_loss:
            if self.direction == Direction.LONG:
                reward = self.price_target - self.entry_price
                risk = self.entry_price - self.stop_loss
            else:
                reward = self.entry_price - self.price_target
                risk = self.stop_loss - self.entry_price
                
            if risk > 0:
                self.risk_reward_ratio = reward / risk
            else:
                self.risk_reward_ratio = 1.0

class SignalGenerator:
    """AI destekli trading sinyal generator"""
    
    def __init__(self, whale_tracker: WhaleTracker, 
                 liquidation_analyzer: LiquidationAnalyzer,
                 ai_predictor: AIPredictor):
        self.whale_tracker = whale_tracker
        self.liquidation_analyzer = liquidation_analyzer
        self.ai_predictor = ai_predictor
        self.is_generating = True
        self.signal_history = []
        self.active_signals = {}
        self.logger = None

        # ✅ KRİTİK: Bu attribute'ları EKLE!
        self.min_risk_score = 0.2  # Default değer
        self.min_confidence = 0.3  # Default değer
        
        # Logger
        import logging
        self.logger = logging.getLogger('SignalGenerator')
        
        # Config'den değerleri al (eğer varsa)
        try:
            from config import MIN_RISK_SCORE, MIN_CONFIDENCE
            self.min_risk_score = MIN_RISK_SCORE
            self.min_confidence = MIN_CONFIDENCE
            print(f"✅ Config loaded: min_risk_score={self.min_risk_score}, min_confidence={self.min_confidence}")
        except ImportError:
            print(f"⚠️ Using default config: min_risk_score={self.min_risk_score}, min_confidence={self.min_confidence}")
        except Exception as e:
            print(f"⚠️ Config error: {e}, using defaults")
        
        print(f"✅ SignalGenerator initialized successfully")


    async def initialize(self) -> None:
        """Signal generator'ı başlat"""
        self.logger = await setup_logger("signal_generator")
        self.is_generating = True
        await self.logger.info("Signal Generator initialized", status="ready")
        
    async def generate_signals(self) -> None:
        """Generate trading signals based on market analysis"""
        signals = []
        
        try:
            # Get analysis data from all modules
            whale_data = await self.whale_tracker.get_whale_positions()
            liquidation_data = await self.liquidation_analyzer.get_recent_liquidations()
            predictions = await self.ai_predictor.get_predictions()
            
            # Analyze each symbol
            symbols_to_analyze = list(set(
                list(whale_data.keys()) + 
                [liq['symbol'] for liq in liquidation_data] + 
                list(predictions.keys())
            ))[:10]  # Limit to first 10 symbols
            
            for symbol in symbols_to_analyze:
                signal = await self._analyze_market(symbol, whale_data, liquidation_data, predictions)
                
                # ✅ FIX: Use .get() method for dictionaries instead of hasattr()
                if signal and signal.get('confidence', 0) > self.min_confidence:
                    # ✅ FIX: Also check risk_score using .get()
                    if signal.get('risk_score', 0) > self.min_risk_score:
                        signals.append(signal)
                        self.logger.info(f"Signal generated for {symbol}: {signal}")
            
            return signals
            
        except Exception as e:
            self.logger.error(f"Error generating signals: {e}")
            return []

    async def _generate_immediate_signals(self):
        """HEMEN sinyal üret - test için"""
        await self.logger.info("Generating immediate test signals")
        
        test_data = {
            'price': 50000,
            'volume_24h': 1000000000,
            'price_change_24h': 0.05,  # %5 değişim - YÜKSEK RİSK
            'timestamp': datetime.now().isoformat()
        }
        
        # BTC için hemen sinyal üret
        signal = await self.confirm_with_ai_prediction('BTCUSDT', test_data)
        if signal:
            await self._process_signal(signal)
            await self.logger.info(
                "Immediate test signal generated",
                symbol=signal.symbol,
                confidence=signal.confidence
            )
            
    async def _analyze_symbol(self, symbol: str) -> None:
        """Basit symbol analizi"""
        try:
            await self.logger.info(f"Analyzing {symbol}")
            
            # Basit market data
            market_data = {
                'price': 50000.0,
                'volume_24h': 1000000000,
                'price_change_24h': 0.03,  # %3 değişim
                'timestamp': datetime.now().isoformat()
            }
            
            # Sadece AI sinyali üret
            ai_signal = await self.confirm_with_ai_prediction(symbol, market_data)
            if ai_signal and ai_signal.confidence >= 0.7:
                await self._process_signal(ai_signal)
                
        except Exception as e:
            await self.logger.error(f"Analysis error for {symbol}", error=str(e))
            
    async def analyze_whale_liquidation_risk(self, symbol: str, 
                                           market_data: Dict[str, Any]) -> Optional[TradingSignal]:
        """Whale liquidation risk analizi ile sinyal üret"""
        try:
            whale_risk = await self.whale_tracker.calculate_liquidation_risk(
                symbol, market_data['price']
            )
            
            if whale_risk['risk_level'] in ['HIGH', 'CRITICAL']:
                direction = Direction.SHORT if whale_risk['risk_level'] == 'HIGH' else Direction.LONG
                
                current_price = market_data['price']
                if direction == Direction.SHORT:
                    price_target = current_price * 0.98
                    stop_loss = current_price * 1.01
                else:
                    price_target = current_price * 1.02
                    stop_loss = current_price * 0.99
                    
                signal = TradingSignal(
                    symbol=symbol,
                    signal_type=SignalType.WHALE_LIQUIDATION_SIGNAL,
                    direction=direction,
                    confidence=whale_risk['max_risk'],
                    price_target=price_target,
                    stop_loss=stop_loss,
                    entry_price=current_price,
                    metadata={
                        'whale_count': whale_risk['whale_count'],
                        'risk_level': whale_risk['risk_level'],
                        'max_risk_score': whale_risk['max_risk']
                    }
                )
                
                await self.logger.info(
                    "Whale liquidation signal generated",
                    symbol=symbol,
                    direction=direction.value,
                    confidence=signal.confidence,
                    whale_count=whale_risk['whale_count'],
                    risk_level=whale_risk['risk_level']
                )
                
                return signal
                
        except Exception as e:
            await self.logger.error("Whale liquidation signal error", error=str(e), symbol=symbol)
            
        return None
        
    async def detect_cascade_triggers(self, symbol: str, 
                                    market_data: Dict[str, Any]) -> Optional[TradingSignal]:
        """Domino etkisi tetikleyicilerini tespit et"""
        try:
            cascade_analysis = await self.liquidation_analyzer.detect_cascade_pattern(symbol)
            
            if cascade_analysis.get('cascade_detected', False):
                current_price = market_data['price']
                direction = Direction.SHORT
                
                price_target = current_price * 0.97
                stop_loss = current_price * 1.02
                
                signal = TradingSignal(
                    symbol=symbol,
                    signal_type=SignalType.CASCADE_PREDICTION_SIGNAL,
                    direction=direction,
                    confidence=min(cascade_analysis['cluster_count'] * 0.2, 0.9),
                    price_target=price_target,
                    stop_loss=stop_loss,
                    entry_price=current_price,
                    metadata={
                        'cluster_count': cascade_analysis['cluster_count'],
                        'total_events': cascade_analysis['total_cascade_events']
                    }
                )
                
                await self.logger.info(
                    "Cascade signal generated",
                    symbol=symbol,
                    direction=direction.value,
                    confidence=signal.confidence,
                    clusters=cascade_analysis['cluster_count']
                )
                
                return signal
                
        except Exception as e:
            await self.logger.error("Cascade signal error", error=str(e), symbol=symbol)
            
        return None
        
    async def confirm_with_ai_prediction(self, symbol: str, 
                                       market_data: Dict[str, Any]) -> Optional[TradingSignal]:
        """AI tahmini ile sinyal onayı"""
        try:
            ai_risk_prediction = await self.ai_predictor.predict_liquidation_risk(symbol, market_data)
            ai_price_prediction = await self.ai_predictor.predict_price_movement(symbol, market_data, 15)
            ai_vol_prediction = await self.ai_predictor.predict_volatility(symbol, market_data)
            
            combined_confidence = (
                ai_risk_prediction.confidence * 0.4 +
                ai_price_prediction.confidence * 0.4 +
                ai_vol_prediction.confidence * 0.2
            )
            
            if combined_confidence > 0.7:
                if ai_price_prediction.predicted_value > 0.02:
                    direction = Direction.LONG
                elif ai_price_prediction.predicted_value < -0.02:
                    direction = Direction.SHORT
                else:
                    return None
                    
                current_price = market_data['price']
                
                if direction == Direction.LONG:
                    price_target = current_price * (1 + abs(ai_price_prediction.predicted_value))
                    stop_loss = current_price * 0.99
                else:
                    price_target = current_price * (1 - abs(ai_price_prediction.predicted_value))
                    stop_loss = current_price * 1.01
                    
                signal = TradingSignal(
                    symbol=symbol,
                    signal_type=SignalType.AI_CONFIRMATION_SIGNAL,
                    direction=direction,
                    confidence=combined_confidence,
                    price_target=price_target,
                    stop_loss=stop_loss,
                    entry_price=current_price,
                    metadata={
                        'risk_prediction': ai_risk_prediction.predicted_value,
                        'price_prediction': ai_price_prediction.predicted_value,
                        'volatility_prediction': ai_vol_prediction.predicted_value
                    }
                )
                
                await self.logger.info(
                    "AI confirmation signal generated",
                    symbol=symbol,
                    direction=direction.value,
                    confidence=signal.confidence,
                    price_change=ai_price_prediction.predicted_value
                )
                
                return signal
                
        except Exception as e:
            await self.logger.error("AI confirmation signal error", error=str(e), symbol=symbol)
            
        return None
        
    async def generate_risk_adjusted_signal(self, signals: List[TradingSignal],
                                          symbol: str,
                                          market_data: Dict[str, Any]) -> Optional[TradingSignal]:
        """Risk ayarlı kombinasyon sinyali üret"""
        try:
            if not signals:
                return None
                
            combined_signal = await self._combine_signals(signals, symbol, market_data)
            
            if combined_signal and combined_signal.confidence >= CONFIG.MIN_CONFIDENCE:
                position_size = await self.calculate_position_size(combined_signal, market_data)
                combined_signal.position_size = position_size
                
                return combined_signal
                
        except Exception as e:
            await self.logger.error("Risk adjusted signal error", error=str(e), symbol=symbol)
            
        return None
        
    async def calculate_position_size(self, signal: TradingSignal,
                                    market_data: Dict[str, Any]) -> float:
        """Pozisyon büyüklüğü hesapla"""
        try:
            risk_per_trade = CONFIG.RISK_PER_TRADE
            account_size = 10000
            
            risk_amount = account_size * risk_per_trade
            
            if signal.direction == Direction.LONG:
                stop_distance = signal.entry_price - signal.stop_loss
            else:
                stop_distance = signal.stop_loss - signal.entry_price
                
            if stop_distance <= 0:
                return 0.0
                
            position_size = risk_amount / stop_distance
            
            max_position = account_size * 0.1
            position_size = min(position_size, max_position)
            
            min_position = 10
            position_size = max(position_size, min_position)
            
            await self.logger.debug(
                "Position size calculated",
                symbol=signal.symbol,
                position_size=position_size,
                risk_amount=risk_amount
            )
            
            return position_size
            
        except Exception as e:
            await self.logger.error("Position size calculation error", error=str(e), symbol=signal.symbol)
            return 0.0
            
    async def _combine_signals(self, signals: List[TradingSignal],
                             symbol: str,
                             market_data: Dict[str, Any]) -> Optional[TradingSignal]:
        """Çoklu sinyalleri birleştir"""
        try:
            if not signals:
                return None
                
            long_signals = [s for s in signals if s.direction == Direction.LONG]
            short_signals = [s for s in signals if s.direction == Direction.SHORT]
            
            if not long_signals and not short_signals:
                return None
                
            if len(long_signals) > len(short_signals):
                direction = Direction.LONG
                relevant_signals = long_signals
            else:
                direction = Direction.SHORT
                relevant_signals = short_signals
                
            avg_confidence = sum(s.confidence for s in relevant_signals) / len(relevant_signals)
            
            current_price = market_data['price']
            
            if direction == Direction.LONG:
                price_target = current_price * 1.02
                stop_loss = current_price * 0.99
            else:
                price_target = current_price * 0.98
                stop_loss = current_price * 1.01
                
            combined_signal = TradingSignal(
                symbol=symbol,
                signal_type=SignalType.RISK_ADJUSTED_SIGNAL,
                direction=direction,
                confidence=avg_confidence,
                price_target=price_target,
                stop_loss=stop_loss,
                entry_price=current_price,
                metadata={
                    'component_signals': len(relevant_signals),
                    'signal_types': [s.signal_type.value for s in relevant_signals]
                }
            )
            
            return combined_signal
            
        except Exception as e:
            await self.logger.error("Signal combination error", error=str(e), symbol=symbol)
            return None
            
    async def _get_market_data(self, symbol: str) -> Dict[str, Any]:
        """Piyasa verilerini getir"""
        return {
            'price': 45000.0,
            'volume_24h': 1000000000,
            'price_change_24h': 0.02,
            'timestamp': datetime.now().isoformat()
        }
        
    async def _process_signal(self, signal: TradingSignal) -> None:
        """FIXED: Sinyali işle ve kaydet - DEBUG EKLENDİ"""
        try:
            await self.logger.info(
                "🚨 DEBUG: Processing signal", 
                symbol=signal.symbol,
                confidence=signal.confidence,
                min_confidence=CONFIG.MIN_CONFIDENCE
            )
            
            # ✅ 1. CONFIDENCE CHECK - DEBUG
            if signal.confidence < CONFIG.MIN_CONFIDENCE:
                await self.logger.warning(
                    "Low confidence signal ignored", 
                    symbol=signal.symbol, 
                    confidence=signal.confidence,
                    required=CONFIG.MIN_CONFIDENCE
                )
                return
                
            # ✅ 2. ACTIVE SIGNALS MANAGEMENT - DEBUG
            if signal.symbol in self.active_signals:
                active_signal = self.active_signals[signal.symbol]
                await self.logger.info(
                    "Existing signal found",
                    symbol=signal.symbol,
                    old_confidence=active_signal.confidence,
                    new_confidence=signal.confidence
                )
                
                if active_signal.direction == signal.direction:
                    if signal.confidence > active_signal.confidence:
                        self.active_signals[signal.symbol] = signal
                        await self.logger.info("Replaced with higher confidence")
                    else:
                        await self.logger.info("Keeping existing (higher confidence)")
                else:
                    if signal.confidence > active_signal.confidence * 1.2:
                        self.active_signals[signal.symbol] = signal
                        await self.logger.info("Replaced opposite direction signal")
                    else:
                        await self.logger.info("Keeping existing opposite direction")
            else:
                self.active_signals[signal.symbol] = signal
                await self.logger.info("New signal added to active signals")
            
            # ✅ 3. HISTORY - DEBUG
            self.signal_history.append(signal)
            await self.logger.info(
                "Signal history updated",
                total_signals=len(self.signal_history)
            )
            
            # ✅ 4. FINAL LOG - BU GÖRÜNMELİ!
            await self.logger.info(
                "🚀 TRADING SIGNAL GENERATED - FINAL",
                symbol=signal.symbol,
                signal_type=signal.signal_type.value,
                direction=signal.direction.value,
                confidence=signal.confidence,
                risk_reward=signal.risk_reward_ratio,
                active_signals_count=len(self.active_signals)
            )
            
        except Exception as e:
            await self.logger.error(
                "Signal processing error", 
                error=str(e), 
                symbol=signal.symbol
            )
            
    async def get_active_signals(self) -> Dict[str, TradingSignal]:
        """Aktif sinyalleri getir"""
        return self.active_signals.copy()
        
    async def get_signal_history(self, limit: int = 50) -> List[TradingSignal]:
        """Sinyal geçmişini getir"""
        return self.signal_history[-limit:] if self.signal_history else []
        
    async def stop_generation(self) -> None:
        """Sinyal üretimini durdur"""
        self.is_generating = False
        await self.logger.info("Signal Generator stopped", status="stopped")


    async def _analyze_market(self, symbol: str, whale_data: dict, 
                            liquidation_data: list, predictions: dict):
        """
        Analyze market data and generate trading signal
        Returns: Signal dict or None
        """
       
        # ✅ DEBUG: Attribute'ları kontrol et
        print(f"🎯 DEBUG: self.min_risk_score = {getattr(self, 'min_risk_score', 'NOT SET')}")
        print(f"🎯 DEBUG: self.min_confidence = {getattr(self, 'min_confidence', 'NOT SET')}")
        
        # ✅ Eğer attribute'lar yoksa, default değerleri set et
        if not hasattr(self, 'min_risk_score'):
            self.min_risk_score = 0.2
            print(f"⚠️ WARNING: min_risk_score was not set, using default: {self.min_risk_score}")
        
        if not hasattr(self, 'min_confidence'):
            self.min_confidence = 0.3
            print(f"⚠️ WARNING: min_confidence was not set, using default: {self.min_confidence}")
       
       
        try:
            print(f"🎯 _analyze_market analyzing: {symbol}")
            
            # 1. Whale pozisyonlarını analiz et
            whale_positions = whale_data.get(symbol, [])
            whale_sell_pressure = 0
            whale_buy_pressure = 0
            
            for position in whale_positions:
                if position.get('side') == 'sell' or position.get('side') == 'short':
                    whale_sell_pressure += position.get('size', 0)
                else:
                    whale_buy_pressure += position.get('size', 0)
            
            # 2. Liquidation'ları analiz et
            recent_liquidations = [liq for liq in liquidation_data if liq.get('symbol') == symbol]
            liquidation_sell_count = sum(1 for liq in recent_liquidations if liq.get('side') == 'sell')
            liquidation_buy_count = sum(1 for liq in recent_liquidations if liq.get('side') == 'buy')
            
            # 3. AI predictions'ı al
            symbol_prediction = predictions.get(symbol, {})
            ai_risk_score = symbol_prediction.get('risk_score', 0.5)
            ai_confidence = symbol_prediction.get('confidence', 0.5)
            predicted_direction = symbol_prediction.get('predicted_direction', 'neutral')
            
            print(f"📊 Analysis for {symbol}:")
            print(f"   🐋 Whale: Sell={whale_sell_pressure:,.0f}, Buy={whale_buy_pressure:,.0f}")
            print(f"   💧 Liquidations: Sell={liquidation_sell_count}, Buy={liquidation_buy_count}")
            print(f"   🤖 AI: Risk={ai_risk_score:.2f}, Confidence={ai_confidence:.2f}, Direction={predicted_direction}")
            
            # 4. Sinyal kararını ver
            signal_side = None
            signal_reason = []
            
            # Whale pressure analizi
            whale_ratio = whale_sell_pressure / max(whale_buy_pressure, 1)
            if whale_ratio > 2.0:  # 2x daha fazla sell pressure
                signal_side = 'sell'
                signal_reason.append(f"Whale sell pressure ({whale_ratio:.1f}x)")
            elif whale_ratio < 0.5:  # 2x daha fazla buy pressure
                signal_side = 'buy'
                signal_reason.append(f"Whale buy pressure ({whale_ratio:.1f}x)")
            
            # Liquidation analizi
            if liquidation_sell_count > liquidation_buy_count * 1.5:
                if signal_side != 'buy':  # Çelişmiyorsa
                    signal_side = 'sell'
                    signal_reason.append(f"More sell liquidations ({liquidation_sell_count}/{liquidation_buy_count})")
            elif liquidation_buy_count > liquidation_sell_count * 1.5:
                if signal_side != 'sell':  # Çelişmiyorsa
                    signal_side = 'buy'
                    signal_reason.append(f"More buy liquidations ({liquidation_buy_count}/{liquidation_sell_count})")
            
            # AI prediction
            if predicted_direction == 'bearish' and ai_risk_score > 0.7:
                if signal_side != 'buy':  # Çelişmiyorsa
                    signal_side = 'sell'
                    signal_reason.append(f"AI bearish prediction (risk: {ai_risk_score:.2f})")
            elif predicted_direction == 'bullish' and ai_risk_score > 0.7:
                if signal_side != 'sell':  # Çelişmiyorsa
                    signal_side = 'buy'
                    signal_reason.append(f"AI bullish prediction (risk: {ai_risk_score:.2f})")
            
            # 5. Eğer sinyal oluştuysa
            if signal_side:
                # Önce attribute'ları kontrol et, yoksa oluştur
                if not hasattr(self, 'min_risk_score'):
                    self.min_risk_score = 0.2
                if not hasattr(self, 'min_confidence'):
                    self.min_confidence = 0.3
                
                # DEBUG: Threshold değerlerini göster
                print(f"📊 Threshold Check: AI Risk={ai_risk_score:.2f} >= {self.min_risk_score}? {ai_risk_score >= self.min_risk_score}")
                print(f"📊 Threshold Check: AI Conf={ai_confidence:.2f} >= {self.min_confidence}? {ai_confidence >= self.min_confidence}")
                
                # Sonra threshold kontrolü yap
                if ai_risk_score >= self.min_risk_score and ai_confidence >= self.min_confidence:
                    # Price levels (mock - gerçekte WebSocket'ten alınmalı)
                    current_price = 86300  # Mock price
                    
                    # Risk bazlı stop loss ve take profit
                    if signal_side == 'sell':
                        stop_loss = current_price * 1.005  # +0.5%
                        take_profit = current_price * 0.99  # -1.0%
                    else:  # buy
                        stop_loss = current_price * 0.995  # -0.5%
                        take_profit = current_price * 1.01  # +1.0%
                    
                    # Sinyal oluştur
                    signal = {
                        'symbol': symbol,
                        'side': signal_side,
                        'entry_price': current_price,
                        'stop_loss': round(stop_loss, 2),
                        'take_profit': round(take_profit, 2),
                        'risk_score': ai_risk_score,
                        'confidence': ai_confidence,
                        'reason': ' + '.join(signal_reason),
                        'timestamp': datetime.now().isoformat(),
                        'whale_sell_pressure': whale_sell_pressure,
                        'whale_buy_pressure': whale_buy_pressure,
                        'liquidation_sell_count': liquidation_sell_count,
                        'liquidation_buy_count': liquidation_buy_count
                    }
                    
                    print(f"🎯 SIGNAL GENERATED: {symbol} {signal_side.upper()} "
                        f"at {current_price} (Risk: {ai_risk_score:.2f}, Conf: {ai_confidence:.2f})")
                    print(f"   📝 Reason: {signal['reason']}")
                    
                    return signal
                else:
                    print(f"⏳ Threshold not met: Risk {ai_risk_score:.2f} < {self.min_risk_score} "
                        f"or Confidence {ai_confidence:.2f} < {self.min_confidence}")
                    return None

            print(f"⏳ No clear signal direction for {symbol}")
            return None
            
        except Exception as e:
            print(f"❌ Error in _analyze_market: {e}")
            import traceback
            traceback.print_exc()
            return None    
