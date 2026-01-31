# test_r_fixed.py - SAFE test without breaking main script
import os
import json
import subprocess
import logging
from typing import Dict, Tuple, Optional, List, Any
from datetime import datetime, date, timedelta, time
import re

def test_r_with_safe_output():
    print("Testing R connection (safe version)...")

    # Test 1: Simple R command
    cmd = ['Rscript', '-e', 'cat("Hello from R\\n")']
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    print(f"Simple test: {'OK' if result.returncode == 0 else 'FAILED'}")

    # Test 2: Run with real data
    test_cmd = [
        'Rscript',
        'bet_resolver.R',
        'Patrick Mahomes',
        'nfl',
        '2024',
        'passing_yards_over',
        'Chiefs vs Dolphins 2024-01-13'
    ]

    print(f"\nRunning bet_resolver.R...")
    result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=30,
                            encoding='utf-8', errors='replace')

    print(f"\nReturn code: {result.returncode}")
    print(f"\nOutput (first 500 chars):\n{result.stdout[:500]}")

    if result.stderr:
        print(f"\nErrors:\n{result.stderr[:500]}")

    return result.returncode == 0


if __name__ == "__main__":
    test_r_with_safe_output()