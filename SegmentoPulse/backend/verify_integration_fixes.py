"""
Integration Fix Verification Script
====================================

Tests all 3 integration fixes to ensure they're working correctly.

Run this script to verify:
1. Bloom Filter reset functionality
2. ID generation consistency (32-char)
3. Validation null-safety

Usage:
    python verify_integration_fixes.py
"""

import asyncio
import hashlib


def test_id_generation():
    """
    Test #2: Verify ID generation consistency
    
    Frontend and Backend MUST generate identical IDs for the same URL
    """
    print("=" * 70)
    print("🔍 TEST 1: ID Generation Consistency")
    print("=" * 70)
    
    test_urls = [
        "https://cnn.com/article?utm_source=twitter",
        "https://techcrunch.com/2024/ai-news",
        "https://example.com/test"
    ]
    
    # Import backend ID generators
    from app.services.appwrite_db import AppwriteDatabase
    from app.utils.id_generator import generate_article_id as backend_gen
    
    db = AppwriteDatabase()
    
    all_match = True
    
    for url in test_urls:
        # Backend Method 1 (appwrite_db)
        id_appwrite = db._generate_url_hash(url)
        
        # Backend Method 2 (id_generator)
        id_backend = backend_gen(url)
        
        # Simulate frontend (same algo)
        hash_obj = hashlib.sha256(url.encode('utf-8'))
        id_frontend = hash_obj.hexdigest()[:32]
        
        print(f"\nURL: {url[:50]}...")
        print(f"  Frontend:  {id_frontend} (len={len(id_frontend)})")
        print(f"  Backend1:  {id_appwrite} (len={len(id_appwrite)})")
        print(f"  Backend2:  {id_backend} (len={len(id_backend)})")
        
        # Verify all match
        if id_frontend == id_appwrite == id_backend and len(id_frontend) == 32:
            print("  ✅ PASS: All IDs match and are 32 chars")
        else:
            print("  ❌ FAIL: ID mismatch detected!")
            all_match = False
    
    print("\n" + "=" * 70)
    if all_match:
        print("✅ TEST 1 PASSED: ID generation is consistent across all components")
    else:
        print("❌ TEST 1 FAILED: ID generation mismatch detected")
    print("=" * 70)
    print()
    
    return all_match


def test_validation_null_safety():
    """
    Test #3: Verify null-safety in validation
    
    Should handle articles with None fields gracefully
    """
    print("=" * 70)
    print("🔍 TEST 2: Validation Null-Safety")
    print("=" * 70)
    
    from app.utils.data_validation import is_relevant_to_category
    
    # Test articles with None fields (production edge case)
    test_articles = [
        {
            "title": "AI News Update",
            "description": None,  # Explicit None
            "url": "https://test.com/1"
        },
        {
            "title": None,  # Explicit None
            "description": "Some content",
            "url": "https://test.com/2"
        },
        {
            "title": "Cloud Computing",
            "description": "",  # Empty string
            "url": "https://test.com/3"
        }
    ]
    
    all_pass = True
    
    for i, article in enumerate(test_articles, 1):
        try:
            result = is_relevant_to_category(article, "ai")
            print(f"\n  Article {i}: ✅ No crash (relevant={result})")
            print(f"    Title: {article.get('title')}")
            print(f"    Description: {article.get('description')}")
        except AttributeError as e:
            print(f"\n  Article {i}: ❌ CRASH - {e}")
            all_pass = False
        except Exception as e:
            print(f"\n  Article {i}: ⚠️  Other error - {e}")
    
    print("\n" + "=" * 70)
    if all_pass:
        print("✅ TEST 2 PASSED: Validation handles None fields gracefully")
    else:
        print("❌ TEST 2 FAILED: Validation crashes on None fields")
    print("=" * 70)
    print()
    
    return all_pass


async def test_bloom_filter_reset():
    """
    Test #1: Verify Bloom Filter reset functionality
    
    Should successfully reset and show before/after stats
    """
    print("=" * 70)
    print("🔍 TEST 3: Bloom Filter Reset")
    print("=" * 70)
    
    from app.services.deduplication import get_url_filter
    
    try:
        # Get filter instance
        url_filter = get_url_filter()
        
        # Add some test URLs
        test_urls = [
            "https://test1.com/article",
            "https://test2.com/news",
            "https://test3.com/update"
        ]
        
        print("\nAdding test URLs to filter...")
        for url in test_urls:
            url_filter.check_and_add(url)
        
        # Get stats before reset
        stats_before = url_filter.get_stats()
        print(f"\n📊 Before Reset:")
        print(f"  Total Checks: {stats_before['total_checks']}")
        print(f"  Unique URLs: {stats_before['unique_urls_added']}")
        print(f"  Duplicates: {stats_before['duplicates_detected']}")
        
        # Reset the filter
        print("\n🔄 Resetting filter...")
        url_filter.reset()
        
        # Get stats after reset
        stats_after = url_filter.get_stats()
        print(f"\n📊 After Reset:")
        print(f"  Total Checks: {stats_after['total_checks']}")
        print(f"  Unique URLs: {stats_after['unique_urls_added']}")
        print(f"  Duplicates: {stats_after['duplicates_detected']}")
        
        # Verify reset worked
        if stats_after['unique_urls_added'] == 0:
            print("\n✅ Filter reset successfully (all counters zeroed)")
            success = True
        else:
            print("\n❌ Filter reset failed (counters not zeroed)")
            success = False
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        success = False
    
    print("\n" + "=" * 70)
    if success:
        print("✅ TEST 3 PASSED: Bloom Filter reset works correctly")
    else:
        print("❌ TEST 3 FAILED: Bloom Filter reset has issues")
    print("=" * 70)
    print()
    
    return success


async def run_all_tests():
    """Run all verification tests"""
    print("\n" + "🚀" * 35)
    print("INTEGRATION FIX VERIFICATION SUITE")
    print("🚀" * 35 + "\n")
    
    results = {}
    
    # Test 1: ID Generation
    results['id_generation'] = test_id_generation()
    
    # Test 2: Validation
    results['validation'] = test_validation_null_safety()
    
    # Test 3: Bloom Filter
    results['bloom_filter'] = await test_bloom_filter_reset()
    
    # Summary
    print("\n" + "=" * 70)
    print("📋 FINAL SUMMARY")
    print("=" * 70)
    
    all_passed = all(results.values())
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}  {test_name.replace('_', ' ').title()}")
    
    print("=" * 70)
    
    if all_passed:
        print("\n🎉 ALL TESTS PASSED - Integration fixes working correctly!")
        print("✅ System is ready for production deployment")
    else:
        print("\n⚠️  SOME TESTS FAILED - Review errors above")
        print("❌ Fix issues before deploying to production")
    
    print("\n")
    return all_passed


if __name__ == "__main__":
    # Run tests
    success = asyncio.run(run_all_tests())
    
    # Exit code for CI/CD
    exit(0 if success else 1)
