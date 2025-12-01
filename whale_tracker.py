import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from config import CONFIG
from data.data_storage import DataStorage
from utils.logger import setup_logger
from utils.helpers import calculate_usd_value, format_quantity

class WhaleTracker:
    """Balina (whale) pozisyonlarını ve büyük işlemleri takip eder"""
    
    def __init__(self, data_storage: DataStorage):
        self.storage = data_storage
        self.is_tracking = False
        self.whale_thresholds = {
            'BTCUSDT': 2.5, 'ETHUSDT': 50, 'ADAUSDT': 50000, 'DOTUSDT': 5000,
            'LINKUSDT': 5000, 'BNBUSDT': 500, 'XRPUSDT': 100000, 'SOLUSDT': 1000,
            'MATICUSDT': 50000, 'AVAXUSDT': 5000
        }
        self.whale_positions = {}
        self.liquidation_risk_cache = {}
        self.logger = None
        
    async def initialize(self) -> None:
        """Whale tracker'ı başlat"""
        self.logger = await setup_logger("whale_tracker")
        self.is_tracking = True
        await self.logger.info("Whale Tracker initialized", status="ready")
        
    async def start_tracking(self) -> None:
        """Whale takibini başlat"""
        await self.logger.info("Starting whale tracking", status="starting")
        
        while self.is_tracking:
            await self._run_whale_analysis()
            await asyncio.sleep(30)
            
    async def _run_whale_analysis(self) -> None:
        """Periyodik whale analizi yürüt"""
        try:
            await self._analyze_recent_activity()
            await self._update_whale_positions()
            await self._calculate_liquidation_risks()
            
        except Exception as e:
            await self.logger.error("Whale analysis error", error=str(e))
            
    async def detect_large_trades(self, trade_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Büyük işlemleri tespit et (100K USD üstü)"""
        try:
            symbol = trade_data['symbol']
            quantity = trade_data['quantity']
            price = trade_data['price']
            
            usd_value = calculate_usd_value(quantity, price)
            threshold = self._get_whale_threshold(symbol)
            
            if usd_value >= 100000 or quantity >= threshold:
                whale_trade = {
                    'type': 'WHALE_TRADE',
                    'symbol': symbol,
                    'side': trade_data['side'],
                    'quantity': quantity,
                    'price': price,
                    'usd_value': usd_value,
                    'exchange': trade_data['exchange'],
                    'timestamp': trade_data['timestamp'],
                    'threshold_exceeded': usd_value >= 100000,
                    'confidence': self._calculate_trade_confidence(usd_value)
                }
                
                await self.logger.info(
                    "Whale trade detected",
                    symbol=symbol,
                    side=trade_data['side'],
                    quantity=format_quantity(quantity),
                    price=price,
                    usd_value=usd_value,
                    threshold=threshold,
                    confidence=whale_trade['confidence']
                )
                
                return whale_trade
                
        except Exception as e:
            await self.logger.error("Large trade detection error", error=str(e), symbol=trade_data.get('symbol'))
            
        return None
        
    async def analyze_liquidation(self, liquidation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Liquidation'ı analiz et ve whale potansiyelini değerlendir"""
        try:
            symbol = liquidation_data['symbol']
            quantity = liquidation_data['quantity']
            price = liquidation_data['price']
            side = liquidation_data['side']
            
            usd_value = calculate_usd_value(quantity, price)
            is_whale_liquidation = usd_value >= 50000
            is_major_liquidation = usd_value >= 100000
            
            analysis = {
                'is_whale': is_whale_liquidation,
                'is_major_whale': is_major_liquidation,
                'usd_value': usd_value,
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'price': price,
                'timestamp': liquidation_data['timestamp'],
                'exchange': liquidation_data['exchange'],
                'confidence_score': self._calculate_liquidation_confidence(usd_value),
                'impact_score': self._calculate_market_impact(usd_value, symbol)
            }
            
            if is_whale_liquidation:
                await self._record_whale_liquidation(analysis)
                
                whale_size = "MAJOR" if is_major_liquidation else "WHALE"
                await self.logger.warning(
                    "Whale liquidation detected",
                    symbol=symbol,
                    side=side,
                    quantity=format_quantity(quantity),
                    price=price,
                    usd_value=usd_value,
                    whale_size=whale_size,
                    impact_score=analysis['impact_score']
                )
                
            return analysis
            
        except Exception as e:
            await self.logger.error("Liquidation analysis error", error=str(e), symbol=liquidation_data.get('symbol'))
            return {'is_whale': False, 'error': str(e)}
            
    async def track_whale_positions(self) -> Dict[str, Any]:
        """Aktif whale pozisyonlarını takip et"""
        try:
            positions = await self._calculate_whale_positions()
            risk_analysis = await self._analyze_whale_risks(positions)
            
            await self.logger.info(
                "Whale position summary",
                active_whales=risk_analysis.get('active_whales', 0),
                high_risk_count=risk_analysis.get('high_risk_count', 0),
                total_exposure=risk_analysis.get('total_exposure', 0)
            )
            
            return risk_analysis
            
        except Exception as e:
            await self.logger.error("Whale position tracking error", error=str(e))
            return {}
        

    async def get_whale_positions(self, symbol: str = None):
        """
        Get whale positions from database
        Returns: Dict of whale positions
        """
        try:
            # Eğer symbol belirtilmişse
            if symbol:
                query = """
                    SELECT symbol, position_size, position_side, entry_price, 
                        liquidation_price, timestamp 
                    FROM whale_positions 
                    WHERE symbol = ? AND timestamp > datetime('now', '-1 hour')
                    ORDER BY timestamp DESC
                    LIMIT 50
                """
                positions = await self.data_storage.fetch_all(query, (symbol,))
            else:
                query = """
                    SELECT symbol, position_size, position_side, entry_price, 
                        liquidation_price, timestamp 
                    FROM whale_positions 
                    WHERE timestamp > datetime('now', '-1 hour')
                    ORDER BY timestamp DESC
                    LIMIT 100
                """
                positions = await self.data_storage.fetch_all(query)
            
            # Positions'ı dictionary formatına çevir
            result = {}
            for pos in positions:
                symbol = pos['symbol']
                if symbol not in result:
                    result[symbol] = []
                
                result[symbol].append({
                    'size': pos['position_size'],
                    'side': pos['position_side'],
                    'entry_price': pos['entry_price'],
                    'liquidation_price': pos['liquidation_price'],
                    'timestamp': pos['timestamp']
                })
            
            return result
            
        except Exception as e:
            await self.logger.error(f"Error getting whale positions: {str(e)}")
            return {}

            
    async def calculate_liquidation_risk(self, symbol: str, price: float) -> Dict[str, Any]:
        """Whale'ler için liquidation riskini hesapla"""
        try:
            whale_positions = self.whale_positions.get(symbol, [])
            
            if not whale_positions:
                return {'symbol': symbol, 'risk_level': 'LOW', 'whale_count': 0}
                
            risk_scores = []
            total_exposure = 0
            
            for position in whale_positions:
                risk_score = self._calculate_position_risk(position, price)
                risk_scores.append(risk_score)
                total_exposure += position.get('exposure', 0)
                
            avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0
            max_risk = max(risk_scores) if risk_scores else 0
            
            risk_level = self._determine_risk_level(max_risk)
                
            risk_analysis = {
                'symbol': symbol,
                'risk_level': risk_level,
                'average_risk': avg_risk,
                'max_risk': max_risk,
                'whale_count': len(whale_positions),
                'total_exposure': total_exposure,
                'timestamp': datetime.now().isoformat()
            }
            
            if risk_level in ['HIGH', 'CRITICAL']:
                await self.logger.warning(
                    "High liquidation risk",
                    symbol=symbol,
                    risk_level=risk_level,
                    whale_count=len(whale_positions),
                    max_risk=max_risk,
                    exposure=total_exposure
                )
                
            return risk_analysis
            
        except Exception as e:
            await self.logger.error("Liquidation risk calculation error", error=str(e), symbol=symbol)
            return {'symbol': symbol, 'risk_level': 'UNKNOWN', 'error': str(e)}
            
    def _get_whale_threshold(self, symbol: str) -> float:
        """Symbol'a özgü whale threshold'unu getir"""
        return self.whale_thresholds.get(symbol, 100000)
        
    def _calculate_trade_confidence(self, usd_value: float) -> float:
        """Trade whale confidence skoru hesapla"""
        if usd_value >= 500000: return 0.95
        elif usd_value >= 100000: return 0.85
        elif usd_value >= 50000: return 0.70
        else: return 0.0
            
    def _calculate_liquidation_confidence(self, usd_value: float) -> float:
        """Liquidation whale confidence skoru hesapla"""
        if usd_value >= 250000: return 0.98
        elif usd_value >= 100000: return 0.90
        elif usd_value >= 50000: return 0.75
        else: return 0.0
            
    def _calculate_market_impact(self, usd_value: float, symbol: str) -> float:
        """Piyasa etkisi skoru hesapla"""
        if symbol == 'BTCUSDT': return min(usd_value / 1000000, 1.0)
        elif symbol == 'ETHUSDT': return min(usd_value / 500000, 1.0)
        else: return min(usd_value / 100000, 1.0)
            
    def _calculate_position_risk(self, position: Dict[str, Any], current_price: float) -> float:
        """Pozisyon risk skoru hesapla"""
        try:
            entry_price = position.get('entry_price', current_price)
            liquidation_price = position.get('liquidation_price', 0)
            leverage = position.get('leverage', 1)
            
            if liquidation_price == 0: return 0.0
                
            price_ratio = abs(current_price - liquidation_price) / current_price
            leverage_risk = min(leverage / 10, 1.0)
            risk_score = (price_ratio * 0.6) + (leverage_risk * 0.4)
            
            return min(risk_score, 1.0)
            
        except Exception as e:
            return 0.0
            
    def _determine_risk_level(self, risk_score: float) -> str:
        """Risk seviyesini belirle"""
        if risk_score >= 0.8: return 'CRITICAL'
        elif risk_score >= 0.6: return 'HIGH'
        elif risk_score >= 0.4: return 'MEDIUM'
        else: return 'LOW'
        
    async def _analyze_recent_activity(self) -> None:
        """Son aktiviteleri analiz et"""
        try:
            # Son 1 saatlik liquidation'ları getir
            recent_liquidations = await self.storage.get_recent_liquidations(limit=100)
            
            for liquidation in recent_liquidations:
                await self.analyze_liquidation(liquidation)
                
            await self.logger.debug("Recent activity analysis completed", 
                                liquidations_analyzed=len(recent_liquidations))
        except Exception as e:
            await self.logger.error("Recent activity analysis error", error=str(e))

    async def _update_whale_positions(self) -> None:
        """Whale pozisyonlarını güncelle"""
        try:
            # Basit whale pozisyon tracking - gerçek implementasyon için exchange API gerekli
            # Şimdilik mock data ile çalışıyoruz
            mock_whales = [
                {
                    'symbol': 'BTCUSDT',
                    'position_size': 10.5,
                    'entry_price': 44000.0,
                    'leverage': 5,
                    'liquidation_price': 42000.0,
                    'timestamp': datetime.now().isoformat()
                }
            ]
            
            self.whale_positions['BTCUSDT'] = mock_whales
            await self.logger.debug("Whale positions updated", whales_tracked=len(mock_whales))
        except Exception as e:
            await self.logger.error("Whale positions update error", error=str(e))

    async def _calculate_liquidation_risks(self) -> None:
        """Liquidation risklerini hesapla"""
        try:
            for symbol in ['BTCUSDT', 'ETHUSDT']:
                risk_analysis = await self.calculate_liquidation_risk(symbol, 45000.0)
                self.liquidation_risk_cache[symbol] = risk_analysis
                
            await self.logger.debug("Liquidation risks calculated", 
                                symbols_analyzed=len(self.liquidation_risk_cache))
        except Exception as e:
            await self.logger.error("Liquidation risks calculation error", error=str(e))

    async def _record_whale_liquidation(self, analysis: Dict[str, Any]) -> None:
        """Whale liquidation'ını kaydet"""
        try:
            if analysis['is_whale']:
                # Whale liquidation'ları özel olarak işaretle
                analysis['whale_liquidation_recorded'] = True
                analysis['recorded_at'] = datetime.now().isoformat()
                
                await self.logger.info("Whale liquidation recorded", 
                                    symbol=analysis['symbol'],
                                    usd_value=analysis['usd_value'])
        except Exception as e:
            await self.logger.error("Whale liquidation recording error", error=str(e))
        
    async def _calculate_whale_positions(self, start_time: datetime = None, end_time: datetime = None) -> List[Dict[str, Any]]:
        """Whale pozisyonlarını hesapla"""
        return []
        
    async def _analyze_whale_risks(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Whale risklerini analiz et"""
        return {
            'active_whales': len(positions),
            'high_risk_count': 0,
            'total_exposure': 0,
            'timestamp': datetime.now().isoformat()
        }
        
    async def stop_tracking(self) -> None:
        """Whale takibini durdur"""
        self.is_tracking = False
        await self.logger.info("Whale Tracker stopped", status="stopped")
