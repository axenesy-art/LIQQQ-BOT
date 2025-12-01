import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import numpy as np  # ✅ EKSİK IMPORT EKLENDİ

from config import CONFIG
from data.data_storage import DataStorage
from utils.logger import setup_logger
from utils.helpers import calculate_usd_value, format_quantity, timestamp_to_datetime

class LiquidationAnalyzer:
    """Liquidation pattern'larını ve piyasa etkilerini analiz eder"""
    
    def __init__(self, data_storage: DataStorage):
        self.storage = data_storage
        self.is_analyzing = False
        self.heatmap_data = {}
        self.pressure_index_cache = {}
        self.cascade_patterns = {}
        self.logger = None
        
    async def initialize(self) -> None:
        """Liquidation analyzer'ı başlat"""
        self.logger = await setup_logger("liquidation_analyzer")
        self.is_analyzing = True
        await self._load_historical_data()
        await self.logger.info("Liquidation Analyzer initialized", status="ready")
        
    async def start_analysis(self) -> None:
        """Liquidation analizini başlat"""
        await self.logger.info("Starting liquidation analysis", status="starting")
        
        while self.is_analyzing:
            await self._run_comprehensive_analysis()
            await asyncio.sleep(60)
            
    async def generate_heatmap(self, symbol: str, timeframe_hours: int = 24, 
                             price_buckets: int = 50) -> Dict[str, Any]:
        """Liquidation heatmap oluştur"""
        try:
            liquidations = await self.storage.get_recent_liquidations(
                limit=1000, 
                symbol=symbol
            )
            
            if not liquidations:
                await self.logger.warning("No liquidation data for heatmap", symbol=symbol)
                return {'error': f'No liquidation data for {symbol}'}
                
            df = pd.DataFrame(liquidations)
            df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['usd_value'] = df['quantity'] * df['price']
            
            time_buckets = pd.cut(
                df['timestamp_dt'].dt.hour, 
                bins=timeframe_hours, 
                labels=False
            )
            
            price_range = (df['price'].min(), df['price'].max())
            price_bucket_size = (price_range[1] - price_range[0]) / price_buckets
            price_buckets = pd.cut(
                df['price'], 
                bins=price_buckets, 
                labels=False
            )
            
            heatmap_matrix = np.zeros((timeframe_hours, price_buckets))
            
            for idx, row in df.iterrows():
                time_idx = int((row['timestamp_dt'].hour / 24) * timeframe_hours)
                price_idx = int((row['price'] - price_range[0]) / price_bucket_size)
                
                if 0 <= time_idx < timeframe_hours and 0 <= price_idx < price_buckets:
                    heatmap_matrix[time_idx][price_idx] += row['usd_value']
                    
            heatmap_data = {
                'symbol': symbol,
                'timeframe_hours': timeframe_hours,
                'price_buckets': price_buckets,
                'price_range': price_range,
                'matrix': heatmap_matrix.tolist(),
                'total_liquidations': len(df),
                'total_volume': df['usd_value'].sum(),
                'timestamp': datetime.now().isoformat()
            }
            
            self.heatmap_data[symbol] = heatmap_data
            
            await self.logger.info(
                "Heatmap generated",
                symbol=symbol,
                liquidations=len(df),
                volume=df['usd_value'].sum(),
                timeframe=f"{timeframe_hours}h"
            )
            
            return heatmap_data
            
        except Exception as e:
            await self.logger.error("Heatmap generation error", error=str(e), symbol=symbol)
            return {'error': str(e)}
            
    async def calculate_pressure_index(self, symbol: str, 
                                     window_hours: int = 6) -> Dict[str, Any]:
        """Liquidation pressure index hesapla"""
        try:
            liquidations = await self.storage.get_recent_liquidations(
                limit=500, 
                symbol=symbol
            )
            
            if not liquidations:
                return {'symbol': symbol, 'pressure_index': 0.0, 'error': 'No data'}
                
            df = pd.DataFrame(liquidations)
            df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['usd_value'] = df['quantity'] * df['price']
            
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=window_hours)
            
            window_data = df[df['timestamp_dt'] >= start_time]
            
            if len(window_data) == 0:
                return {'symbol': symbol, 'pressure_index': 0.0}
                
            total_volume = window_data['usd_value'].sum()
            volume_std = window_data['usd_value'].std()
            count = len(window_data)
            
            volume_factor = min(total_volume / 1000000, 1.0)
            frequency_factor = min(count / 100, 1.0)
            volatility_factor = min(volume_std / 100000, 1.0) if not pd.isna(volume_std) else 0.0
            
            pressure_index = (
                volume_factor * 0.5 + 
                frequency_factor * 0.3 + 
                volatility_factor * 0.2
            )
            
            pressure_analysis = {
                'symbol': symbol,
                'pressure_index': round(pressure_index, 3),
                'total_volume': total_volume,
                'liquidation_count': count,
                'volume_std': volume_std if not pd.isna(volume_std) else 0.0,
                'window_hours': window_hours,
                'timestamp': datetime.now().isoformat(),
                'level': self._get_pressure_level(pressure_index)
            }
            
            self.pressure_index_cache[symbol] = pressure_analysis
            
            if pressure_index >= 0.7:
                await self.logger.warning(
                    "High liquidation pressure detected",
                    symbol=symbol,
                    pressure_index=pressure_index,
                    pressure_level=pressure_analysis['level'],
                    volume=total_volume,
                    count=count
                )
                
            return pressure_analysis
            
        except Exception as e:
            await self.logger.error("Pressure index calculation error", error=str(e), symbol=symbol)
            return {'symbol': symbol, 'pressure_index': 0.0, 'error': str(e)}
            
    async def detect_cascade_pattern(self, symbol: str, 
                                   lookback_minutes: int = 30) -> Dict[str, Any]:
        """Domino etkisi (cascade) pattern'lerini tespit et"""
        try:
            liquidations = await self.storage.get_recent_liquidations(
                limit=200, 
                symbol=symbol
            )
            
            if len(liquidations) < 10:
                return {'symbol': symbol, 'cascade_detected': False, 'reason': 'Insufficient data'}
                
            df = pd.DataFrame(liquidations)
            df = df.sort_values('timestamp')
            df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['usd_value'] = df['quantity'] * df['price']
            
            df['time_diff'] = df['timestamp_dt'].diff().dt.total_seconds().fillna(0)
            df['volume_diff'] = df['usd_value'].diff().fillna(0)
            
            time_threshold = 300
            volume_threshold = 50000
            
            cascade_clusters = []
            current_cluster = []
            
            for idx, row in df.iterrows():
                if row['usd_value'] >= volume_threshold:
                    if not current_cluster or (row['timestamp_dt'] - current_cluster[-1]['timestamp_dt']).total_seconds() <= time_threshold:
                        current_cluster.append(row)
                    else:
                        if len(current_cluster) >= 3:
                            cascade_clusters.append(current_cluster)
                        current_cluster = [row]
                else:
                    if len(current_cluster) >= 3:
                        cascade_clusters.append(current_cluster)
                    current_cluster = []
                    
            if len(current_cluster) >= 3:
                cascade_clusters.append(current_cluster)
                
            cascade_detected = len(cascade_clusters) > 0
            cascade_analysis = {
                'symbol': symbol,
                'cascade_detected': cascade_detected,
                'cluster_count': len(cascade_clusters),
                'total_cascade_events': sum(len(cluster) for cluster in cascade_clusters),
                'lookback_minutes': lookback_minutes,
                'timestamp': datetime.now().isoformat(),
                'clusters': []
            }
            
            for i, cluster in enumerate(cascade_clusters):
                cluster_volume = sum(row['usd_value'] for row in cluster)
                cluster_duration = (cluster[-1]['timestamp_dt'] - cluster[0]['timestamp_dt']).total_seconds()
                
                cluster_info = {
                    'cluster_id': i + 1,
                    'event_count': len(cluster),
                    'total_volume': cluster_volume,
                    'duration_seconds': cluster_duration,
                    'avg_volume': cluster_volume / len(cluster),
                    'start_time': cluster[0]['timestamp_dt'].isoformat(),
                    'end_time': cluster[-1]['timestamp_dt'].isoformat()
                }
                cascade_analysis['clusters'].append(cluster_info)
                
                await self.logger.warning(
                    "Cascade pattern detected",
                    symbol=symbol,
                    cluster_id=i+1,
                    events=len(cluster),
                    volume=cluster_volume,
                    duration=cluster_duration
                )
                
            self.cascade_patterns[symbol] = cascade_analysis
            
            return cascade_analysis
            
        except Exception as e:
            await self.logger.error("Cascade pattern detection error", error=str(e), symbol=symbol)
            return {'symbol': symbol, 'cascade_detected': False, 'error': str(e)}
            
    async def find_critical_levels(self, symbol: str, 
                                 price_range: Optional[Tuple[float, float]] = None,
                                 density_threshold: float = 0.8) -> Dict[str, Any]:
        """Kritik liquidation seviyelerini bul"""
        try:
            liquidations = await self.storage.get_recent_liquidations(
                limit=1000, 
                symbol=symbol
            )
            
            if not liquidations:
                return {'symbol': symbol, 'critical_levels': [], 'error': 'No data'}
                
            df = pd.DataFrame(liquidations)
            df['usd_value'] = df['quantity'] * df['price']
            
            if not price_range:
                price_range = (df['price'].min(), df['price'].max())
                
            price_min, price_max = price_range
            price_range_size = price_max - price_min
            
            price_levels = np.linspace(price_min, price_max, 100)
            densities = []
            
            for level in price_levels:
                level_data = df[
                    (df['price'] >= level - price_range_size * 0.01) & 
                    (df['price'] <= level + price_range_size * 0.01)
                ]
                density = level_data['usd_value'].sum() / df['usd_value'].sum() if df['usd_value'].sum() > 0 else 0
                densities.append(density)
                
            critical_levels = []
            density_threshold_value = max(densities) * density_threshold
            
            for i, density in enumerate(densities):
                if density >= density_threshold_value:
                    level_info = {
                        'price_level': round(price_levels[i], 2),
                        'density': round(density, 4),
                        'cumulative_volume': df[df['price'] >= price_levels[i]]['usd_value'].sum(),
                        'level_type': 'SUPPORT' if i < len(price_levels) // 2 else 'RESISTANCE'
                    }
                    critical_levels.append(level_info)
                    
            critical_levels.sort(key=lambda x: x['density'], reverse=True)
            
            critical_analysis = {
                'symbol': symbol,
                'critical_levels': critical_levels[:5],
                'price_range': price_range,
                'density_threshold': density_threshold,
                'timestamp': datetime.now().isoformat(),
                'total_levels_found': len(critical_levels)
            }
            
            if critical_levels:
                top_level = critical_levels[0]
                await self.logger.info(
                    "Critical level found",
                    symbol=symbol,
                    price=top_level['price_level'],
                    density=top_level['density'],
                    level_type=top_level['level_type']
                )
                
            return critical_analysis
            
        except Exception as e:
            await self.logger.error("Critical levels detection error", error=str(e), symbol=symbol)
            return {'symbol': symbol, 'critical_levels': [], 'error': str(e)}
            
    async def analyze_market_impact(self, symbol: str, 
                                  event_window_minutes: int = 10) -> Dict[str, Any]:
        """Liquidation'ların piyasa etkisini analiz et"""
        try:
            liquidations = await self.storage.get_recent_liquidations(
                limit=100, 
                symbol=symbol
            )
            
            if not liquidations:
                return {'symbol': symbol, 'impact_analysis': {}, 'error': 'No data'}
                
            df = pd.DataFrame(liquidations)
            total_impact = df['quantity'].sum() * df['price'].mean() if len(df) > 0 else 0
            
            impact_analysis = {
                'symbol': symbol,
                'total_impact_usd': total_impact,
                'impact_score': min(total_impact / 1000000, 1.0),
                'event_count': len(df),
                'avg_event_size': df['quantity'].mean() if len(df) > 0 else 0,
                'window_minutes': event_window_minutes,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.logger.info(
                "Market impact analysis",
                symbol=symbol,
                impact_score=impact_analysis['impact_score'],
                total_impact=total_impact,
                events=len(df)
            )
            
            return impact_analysis
            
        except Exception as e:
            await self.logger.error("Market impact analysis error", error=str(e), symbol=symbol)
            return {'symbol': symbol, 'impact_analysis': {}, 'error': str(e)}
            
    def _get_pressure_level(self, pressure_index: float) -> str:
        """Pressure index seviyesini belirle"""
        if pressure_index >= 0.8: return 'EXTREME'
        elif pressure_index >= 0.6: return 'HIGH'
        elif pressure_index >= 0.4: return 'MEDIUM'
        elif pressure_index >= 0.2: return 'LOW'
        else: return 'VERY_LOW'
            
    async def _run_comprehensive_analysis(self) -> None:
        """Kapsamlı liquidation analizi yürüt"""
        try:
            symbols = CONFIG.LIQUIDATION_SYMBOLS[:5]
            
            for symbol in symbols:
                await self.generate_heatmap(symbol)
                await self.calculate_pressure_index(symbol)
                await self.detect_cascade_pattern(symbol)
                await self.find_critical_levels(symbol)
                await self.analyze_market_impact(symbol)
                
            await self.logger.info("Comprehensive analysis completed", symbols_analyzed=len(symbols))
            
        except Exception as e:
            await self.logger.error("Comprehensive analysis error", error=str(e))
            
    async def _load_historical_data(self) -> None:
        """Geçmiş liquidation verilerini yükle"""
        try:
            # Son 24 saatlik verileri yükle
            historical_data = await self.storage.get_recent_liquidations(limit=1000)
            
            if historical_data:
                # Basit historical analysis
                total_volume = sum(item['quantity'] * item['price'] for item in historical_data)
                await self.logger.info("Historical data loaded", 
                                    records=len(historical_data),
                                    total_volume=total_volume)
            else:
                await self.logger.warning("No historical data available")
                
        except Exception as e:
            await self.logger.error("Historical data loading error", error=str(e))
        
    async def get_analysis_summary(self) -> Dict[str, Any]:
        """Mevcut analiz özetini getir"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'heatmaps_generated': len(self.heatmap_data),
            'pressure_indices': self.pressure_index_cache,
            'cascade_patterns': self.cascade_patterns,
            'symbols_analyzed': list(self.heatmap_data.keys())
        }
        
        return summary
    
    
    async def get_recent_liquidations(self, symbol=None, limit=50):
        """
        Get recent liquidations from database
        Returns: List of liquidation dicts
        """
        try:
            if symbol:
                query = """
                    SELECT symbol, side, size, price, timestamp, exchange
                    FROM liquidations 
                    WHERE symbol = ? AND timestamp > datetime('now', '-5 minutes')
                    ORDER BY timestamp DESC
                    LIMIT ?
                """
                params = (symbol, limit)
            else:
                query = """
                    SELECT symbol, side, size, price, timestamp, exchange
                    FROM liquidations 
                    WHERE timestamp > datetime('now', '-5 minutes')
                    ORDER BY timestamp DESC
                    LIMIT ?
                """
                params = (limit,)
            
            results = await self.data_storage.fetch_all(query, params)
            
            # Format results
            liquidations = []
            for row in results:
                liquidations.append({
                    'symbol': row['symbol'],
                    'side': row['side'],
                    'size': row['size'],
                    'price': row['price'],
                    'timestamp': row['timestamp'],
                    'exchange': row['exchange']
                })
            
            return liquidations
            
        except Exception as e:
            await self.logger.error(f"Error getting liquidations: {str(e)}")
            return []
    
    
    async def get_recent_liquidations(self, symbol: str = None, limit: int = 50) -> List[Dict]:
        """Get recent liquidations from database"""
        try:
            if not self.data_storage:
                return []
            
            if symbol:
                return await self.data_storage.get_recent_liquidations(symbol, limit)
            else:
                return await self.data_storage.get_recent_liquidations(limit=limit)
                
        except Exception as e:
            self.logger.error(f"Error getting recent liquidations: {e}")
            return []
    
    
        
    async def stop_analysis(self) -> None:
        """Liquidation analizini durdur"""
        self.is_analyzing = False
        await self.logger.info("Liquidation Analyzer stopped", status="stopped")
