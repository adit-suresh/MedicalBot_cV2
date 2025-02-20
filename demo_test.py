#!/usr/bin/env python3
import os
import logging
import base64
import json
import requests
from datetime import datetime

# Set up logging according to the project's logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Import your project modules – these are based on your actual repository structure.
from src.email_handler.outlook_client import OutlookClient
from src.document_processor.ocr_processor import OCRProcessor
from src.document_processor.excel_processor import ExcelProcessor
from src.services.data_integrator import DataIntegrator
from src.services.workflow_runner import WorkflowRunner

def test_email_fetching():
    logging.info("=== Testing Email Fetching ===")
    try:
        outlook = OutlookClient()
        emails = outlook.fetch_emails()  # Uses your implemented fetching logic.
        logging.info(f"Fetched {len(emails)} emails.")
        return emails
    except Exception as e:
        logging.error(f"Email fetching error: {e}")
        return []

def test_ocr_processing(pdf_file):
    logging.info(f"=== Testing OCR Processing for: {pdf_file} ===")
    try:
        ocr = OCRProcessor()
        ocr_result = ocr.process_document(pdf_file)
        logging.info(f"OCR Result: {ocr_result}")
        return ocr_result
    except Exception as e:
        logging.error(f"OCR processing error: {e}")
        return {}

def test_excel_processing(excel_file):
    logging.info(f"=== Testing Excel Processing for: {excel_file} ===")
    try:
        excel_processor = ExcelProcessor()
        excel_data = excel_processor.process_excel_file(excel_file)
        logging.info(f"Extracted Excel Data: {excel_data}")
        return excel_data
    except Exception as e:
        logging.error(f"Excel processing error: {e}")
        return {}

def test_data_integration(excel_data, ocr_data):
    logging.info("=== Testing Data Integration ===")
    try:
        integrator = DataIntegrator()
        # combine_data() should merge Excel and OCR data following the priority rules:
        # Excel (priority 1) > Passport OCR (priority 2), with missing fields filled with '.'
        integrated_data = integrator.combine_data(excel_data, ocr_data)
        logging.info(f"Integrated Data: {integrated_data}")
        return integrated_data
    except Exception as e:
        logging.error(f"Data integration error: {e}")
        return {}

def run_full_workflow():
    logging.info("=== Running Full Workflow ===")
    try:
        runner = WorkflowRunner()
        # run_full_workflow() is assumed to run the end-to-end process:
        # fetching emails, processing documents, merging data, and generating the final Excel file.
        runner.run()
        logging.info("Full workflow executed successfully.")
    except Exception as e:
        logging.error(f"Full workflow error: {e}")

def send_excel_via_teams(final_excel_path):
    logging.info(f"=== Sending Final Excel File via Teams: {final_excel_path} ===")
    try:
        # Read and encode the Excel file
        with open(final_excel_path, "rb") as f:
            file_bytes = f.read()
        file_b64 = base64.b64encode(file_bytes).decode("utf-8")

        # Retrieve Teams integration parameters from environment variables.
        access_token = os.getenv("GRAPH_ACCESS_TOKEN")
        team_id = os.getenv("TEAMS_TEAM_ID")
        channel_id = os.getenv("TEAMS_CHANNEL_ID")
        if not (access_token and team_id and channel_id):
            logging.error("Missing Teams integration parameters. Please set GRAPH_ACCESS_TOKEN, TEAMS_TEAM_ID, and TEAMS_CHANNEL_ID.")
            return

        # Construct the message payload.
        message_payload = {
            "body": {
                "contentType": "html",
                "content": "Final Excel Report Attached. Effective Date: " + datetime.now().strftime("%d-%m-%Y")
            },
            "attachments": [
                {
                    "id": "1",
                    "contentType": "reference",
                    "contentUrl": f"data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{file_b64}",
                    "name": os.path.basename(final_excel_path)
                }
            ]
        }
        
        url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, data=json.dumps(message_payload))
        if response.status_code in (200, 201):
            logging.info("Excel file sent successfully via Teams.")
        else:
            logging.error(f"Failed to send file via Teams. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error in Teams file sending: {e}")

if __name__ == "__main__":
    # 1. Run tests for individual components.
    emails = test_email_fetching()
    
    sample_pdf = "samples/sample_document.pdf"  # Adjust this path to an existing sample PDF.
    if os.path.exists(sample_pdf):
        ocr_data = test_ocr_processing(sample_pdf)
    else:
        logging.warning("Sample PDF not found; skipping OCR test.")
        ocr_data = {}

    sample_excel = "samples/sample_customers.xlsx"  # Adjust to an existing sample Excel file.
    if os.path.exists(sample_excel):
        excel_data = test_excel_processing(sample_excel)
    else:
        logging.warning("Sample Excel not found; skipping Excel test.")
        excel_data = {}

    if excel_data or ocr_data:
        test_data_integration(excel_data, ocr_data)
    else:
        logging.warning("Insufficient data for integration test.")

    # 2. Optionally run the full workflow to generate the final Excel report.
    run_full_workflow()

    # 3. Send the final Excel file via Teams.
    final_excel = "final_report.xlsx"  # This should be the output path generated by your workflow.
    if os.path.exists(final_excel):
        send_excel_via_teams(final_excel)
    else:
        logging.error(f"Final Excel file not found at {final_excel}.")
