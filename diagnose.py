# Save as diagnose.py
import os
import sys
import traceback

try:
    print("Testing Python execution...")
    print(f"Python version: {sys.version}")
    print(f"Current directory: {os.getcwd()}")
    
    print("\nTesting imports...")
    import argparse
    print("argparse: OK")
    import pandas as pd
    print("pandas: OK")
    import boto3
    print("boto3: OK")
    from msal import ConfidentialClientApplication
    print("msal: OK")
    
    print("\nTesting file access...")
    if os.path.exists("test_complete_workflow.py"):
        print("test_complete_workflow.py: Found")
        file_size = os.path.getsize("test_complete_workflow.py")
        print(f"File size: {file_size} bytes")
    else:
        print("test_complete_workflow.py: NOT FOUND")
        
    print("\nChecking for syntax errors...")
    import py_compile
    py_compile.compile("test_complete_workflow.py")
    print("No syntax errors found")
    
except Exception as e:
    print(f"\nERROR: {str(e)}")
    traceback.print_exc()