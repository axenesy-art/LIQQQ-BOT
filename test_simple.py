import asyncio
import logging
import sys
import os

# TensorFlow warning'larını kapat
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

logging.basicConfig(level=logging.INFO)

async def quick_signal_test():
    """Quick 30-second test of SignalGenerator"""
    print("⚡ QUICK SIGNAL TEST (30 seconds)")
    print("=" * 50)
    
    try:
        # 1. MOCK DEPENDENCIES
        print("\n1. Creating mock dependencies...")
        
        class MockWhaleTracker:
            async def get_whale_positions(self, symbol=None):
                return {
                    "BTCUSDT": [
                        {
                            "size": 150000,
                            "side": "short",
                            "entry_price": 86500,
                            "liquidation_price": 87500
                        }
                    ]
                }
        
        class MockLiquidationAnalyzer:
            async def get_recent_liquidations(self, symbol=None, limit=50):
                return [
                    {
                        "symbol": "BTCUSDT",
                        "side": "sell",
                        "size": 1.25,
                        "price": 86377.3
                    }
                ]
        
        class MockAIPredictor:
            async def get_predictions(self, symbol=None):
                return {
                    "BTCUSDT": {
                        "risk_score": 0.95,
                        "confidence": 0.90,
                        "predicted_direction": "bearish"
                    }
                }
        
        # 2. IMPORT SIGNALGENERATOR
        print("\n2. Importing SignalGenerator...")
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        from trading.signal_generator import SignalGenerator
        
        # 3. CREATE INSTANCE
        print("\n3. Creating SignalGenerator instance...")
        sg = SignalGenerator(
            whale_tracker=MockWhaleTracker(),
            liquidation_analyzer=MockLiquidationAnalyzer(),
            ai_predictor=MockAIPredictor()
        )
        
        print("   ✅ SignalGenerator instance created")
        
        # Check attributes
        print(f"\n4. Checking min_confidence: {getattr(sg, 'min_confidence', 'NOT FOUND')}")
        
        # 5. RUN TEST FOR 30 SECONDS
        print("\n5. Running 30-second test...")
        print("-" * 40)
        
        import time
        start_time = time.time()
        signal_count = 0
        iteration = 0
        
        while time.time() - start_time < 30:  # 30 saniye
            iteration += 1
            current_time = time.time() - start_time
            
            print(f"\n🔄 Iteration {iteration} ({current_time:.1f}s)")
            print("-" * 20)
            
            try:
                # Generate signals
                signals = await sg.generate_signals()
                
                if signals:
                    signal_count += len(signals)
                    
                    for i, signal in enumerate(signals, 1):
                        if isinstance(signal, dict):
                            symbol = signal.get('symbol', 'UNKNOWN')
                            side = signal.get('side', 'unknown')
                            confidence = signal.get('confidence', 0)
                            risk = signal.get('risk_score', 0)
                            
                            print(f"🎯 Signal #{signal_count}: {symbol} {side.upper()} "
                                  f"(Risk: {risk:.2f}, Conf: {confidence:.2f})")
                            
                            # Show signal details
                            if signal_count == 1:  # Only show details for first signal
                                print("   📋 Signal details:")
                                for key, value in signal.items():
                                    if isinstance(value, float):
                                        print(f"      {key}: {value:.4f}")
                                    else:
                                        print(f"      {key}: {value}")
                        else:
                            print(f"🎯 Signal #{signal_count}: {type(signal)} object")
                else:
                    print("⏳ No signals this iteration")
                    
            except Exception as e:
                print(f"❌ Error in iteration {iteration}: {e}")
            
            # Wait 5 seconds
            if time.time() - start_time < 30:  # Don't wait on last iteration
                await asyncio.sleep(5)
        
        # 6. FINAL RESULTS
        print("\n" + "=" * 50)
        print("📊 FINAL RESULTS:")
        print("=" * 50)
        print(f"   Total time: 30 seconds")
        print(f"   Iterations: {iteration}")
        print(f"   Signals generated: {signal_count}")
        print(f"   Signals per minute: {signal_count * 2}")  # 30 seconds * 2 = per minute
        
        if signal_count > 0:
            print(f"\n🎉 SUCCESS! SignalGenerator is working!")
            print(f"   Average: {signal_count/iteration:.1f} signals per iteration")
        else:
            print(f"\n❌ FAILURE! No signals generated")
            print(f"   Check the logs above for errors")
        
        print("=" * 50)
        
        return signal_count > 0
        
    except ImportError as e:
        print(f"\n❌ IMPORT ERROR: {e}")
        print(f"   Current directory: {os.getcwd()}")
        print(f"   Python path: {sys.path}")
        
        # List files
        if os.path.exists('trading'):
            print(f"   Files in trading/: {os.listdir('trading')}")
        else:
            print("   ❌ trading/ directory not found!")
        
        return False
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting Quick Signal Test...")
    success = asyncio.run(quick_signal_test())
    
    if success:
        print("\n✅ TEST PASSED! Proceed to main bot testing.")
    else:
        print("\n❌ TEST FAILED! Fix the issues first.")