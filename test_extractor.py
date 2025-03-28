import os
from dotenv import load_dotenv
from src.document_processor.gpt_processor import GPTProcessor
from src.document_processor.textract_processor import TextractProcessor

# Load environment variables
load_dotenv()

# Initialize processors
gpt = GPTProcessor()
textract = TextractProcessor()

# Test file path
test_file = r"C:\Users\adit.s\Downloads\27916_VISA.pdf"  # Use the same test file on both systems

# Test GPT
print("Testing GPT processor...")
gpt_result = gpt.process_document(test_file, "passport")
print(f"GPT result keys: {gpt_result.keys() if isinstance(gpt_result, dict) else 'Error'}")

# Test Textract
print("Testing Textract processor...")
textract_result = textract.process_document(test_file, "passport")
print(f"Textract result keys: {textract_result.keys() if isinstance(textract_result, dict) else 'Error'}")