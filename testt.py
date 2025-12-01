#!/usr/bin/env python3
"""
QUICK TEST FOR ALL FIXES
Run this to verify the critical fixes work
"""

import asyncio
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def test_database_fixes():
    """Test database methods are working"""
    print("🧪 Testing Database Fixes...")
    
    from data.data_storage import DataStorage
    
    try:
        storage = DataStorage()
        await storage.initialize()
        
        # Test fetch_one
        result = await storage.fetch_one("SELECT 1 as test")
        print(f"✅ fetch_one: {result}")
        
        # Test fetch_all  
        results = await storage.fetch_all("SELECT 1 as test")
        print(f"✅ fetch_all: {len(results)} results")
        
        # Test execute_query
        success = await storage.execute_query("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
        print(f"✅ execute_query: {success}")
        
        print("🎉 Database fixes WORKING!")
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

async def test_signal_generator_fixes():
    """Test signal generator dictionary handling"""
    print("🧪 Testing Signal Generator Fixes...")
    
    # Mock the dependencies
    class MockWhaleTracker:
        async def get_whale_positions(self, symbol=None):
            return {"BTCUSDT": [{"size": 100000, "side": "short"}]}
    
    class MockLiquidationAnalyzer:
        async def get_recent_liquidations(self, symbol=None, limit=50):
            return [{"symbol": "BTCUSDT", "side": "sell", "size": 1.5}]
    
    class MockAIPredictor:
        async def get_predictions(self, symbol=None):
            return {"BTCUSDT": {"risk_score": 0.8, "confidence": 0.9}}
    
    from trading.signal_generator import SignalGenerator
    
    try:
        sg = SignalGenerator(
            MockWhaleTracker(),
            MockLiquidationAnalyzer(), 
            MockAIPredictor()
        )
        
        # This should work with dictionaries now
        signals = await sg.generate_signals()
        
        print(f"✅ Signal generator: {len(signals)} signals")
        
        for signal in signals:
            print(f"   📊 Signal: {signal.get('symbol')} {signal.get('side')} "
                  f"(conf: {signal.get('confidence'):.2f})")
        
        print("🎉 Signal generator fixes WORKING!")
        return True
        
    except Exception as e:
        print(f"❌ Signal generator test failed: {e}")
        return False

async def main():
    print("🚀 ULTRA LIQUIDATION BOT - CRITICAL FIXES TEST")
    print("=" * 60)
    
    # Test database fixes
    db_ok = await test_database_fixes()
    
    # Test signal generator fixes  
    sg_ok = await test_signal_generator_fixes()
    
    print("\n" + "=" * 60)
    
    if db_ok and sg_ok:
        print("🎉 ALL CRITICAL FIXES ARE WORKING!")
        print("🚀 You can now run: python main.py")
    else:
        print("❌ SOME FIXES FAILED - Check the errors above")
    
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
