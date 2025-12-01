import asyncio

async def quick_test():
    print("🧪 QUICK FIX TEST")
    
    # Mock _analyze_market that returns a DICTIONARY
    mock_signal = {
        'symbol': 'BTCUSDT',
        'side': 'sell',
        'confidence': 0.90,
        'risk_score': 0.95,
        'entry_price': 86300
    }
    
    # Test the fix
    min_confidence = 0.3
    
    # OLD WAY (wrong)
    print("\n❌ OLD WAY (hasattr):")
    try:
        if hasattr(mock_signal, 'confidence') and mock_signal.confidence > min_confidence:
            print("  Would accept signal")
        else:
            print("  Would reject signal (because hasattr fails)")
    except:
        print("  ERROR: hasattr doesn't work on dict!")
    
    # NEW WAY (correct)
    print("\n✅ NEW WAY (.get()):")
    if mock_signal.get('confidence', 0) > min_confidence:
        print(f"  ✅ ACCEPT: Confidence {mock_signal.get('confidence')} > {min_confidence}")
    else:
        print(f"  ❌ REJECT: Confidence too low")
    
    # Test with low confidence
    print("\n📊 TEST LOW CONFIDENCE (0.2):")
    low_signal = {'confidence': 0.2}
    if low_signal.get('confidence', 0) > min_confidence:
        print("  ❌ WRONG: Should reject but accepted")
    else:
        print(f"  ✅ CORRECT: Rejected (0.2 < {min_confidence})")

asyncio.run(quick_test())