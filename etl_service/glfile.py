from etl_service.utils.amount_calc_prorrateo import amount_calculation
from etl_service.utils.catalogs_filter import catalogs_filter
from etl_service.utils.cost_centers import cost_centers
from etl_service.utils.data_format import gl_data_format
from etl_service.utils.emp_process import employee_process
from etl_service.utils.flatten_data import flatten_data
from etl_service.utils.flatten_json import flatten_json
from etl_service.utils.nan_elimination import nan_elimination
from etl_service.utils.get_entity import get_entity
from etl_service.utils.get_period import get_period
from etl_service.utils.row_creation import gl_row_creation
from etl_service.utils.get_catalog_filtered import get_catalog_filtered
from etl_service.utils.process_input import process_input
from etl_service.utils.grouped_process import grouped_process
from config.config import CATALOG_STORAGE, TEMPLATE_STORAGE, OUTPUT_STORAGE_GL

from fastapi import FastAPI, HTTPException, BackgroundTasks, Body
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional
import pandas as pd
import os
import uuid
from datetime import datetime
import logging
from azure.storage.blob import BlobServiceClient
import json
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="GL File Generation API",
    description="API for processing input data and generating General Ledger Excel files",
    version="1.0.0"
)

# Azure Storage connection string should be in config
from config.config import AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER_NAME

catalog = CATALOG_STORAGE
template = TEMPLATE_STORAGE

def run_gl(input_data, catalog_path, template_path):
    """
    Process input data and generate GL output.
    
    Args:
        input_data: JSON input data
        catalog_path: Path to catalog files
        template_path: Path to template files
        
    Returns:
        DataFrame with processed GL data
    """
    try:
        # Initial data processing
        df_in, catalog = flatten_data(input_data, catalog_path)
        df_input = flatten_json(df_in)
        
        # Process catalogs
        catalog1, catalog2, catalog3 = catalogs_filter(catalog)
        period_data = get_period(df_input, "")
        catalog1_final = get_catalog_filtered(catalog1, df_input, period_data)
        
        # Initialize analysis list to collect processing results
        analysis = []
        
        # Process each employee and collect results
        for employee in process_input(df_input):
            employee_result = employee_process(
                employee, 
                catalog1_final, 
                catalog2, 
                catalog3, 
                'ME03210.000-ISGDDMFNN',
                'ME01410.000-ISGDDMFNN',
                'ME05010.000-ISGDDMFNN',
                'ME54710.000-ISGDDMFNN',
                'ME02010.000-SSGDDMFNN',
                'ME01010.000-ISGDDMFNN'
            )
            # Add employee processing results to analysis list
            if employee_result:
                analysis.extend(employee_result)
        
        # Process grouped data
        grouped = grouped_process(analysis)
        
        # Format data for output
        output = gl_data_format(analysis, grouped)
        
        return output
    except Exception as e:
        logger.error(f"Error in run_gl: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def upload_to_azure_storage(file_path, blob_name):
    """
    Upload a file to Azure Blob Storage
    
    Args:
        file_path: Path to the local file
        blob_name: Name to be used in Azure Blob Storage
        
    Returns:
        URL of the uploaded blob
    """
    try:
        # Create the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Get the container client
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        # Upload the file
        with open(file_path, "rb") as data:
            blob_client = container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        
        # Return the URL
        return f"https://{blob_service_client.account_name}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{blob_name}"
    except Exception as e:
        logger.error(f"Error uploading to Azure: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_and_upload_gl(input_data):
    """
    Process GL data and upload the result to Azure Storage
    
    Args:
        input_data: Input data for GL processing
        
    Returns:
        Dict with status and file URL
    """
    try:
        # Run the GL process
        output_df = run_gl(input_data, catalog, template)
        
        # Generate a unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_id = str(uuid.uuid4())[:8]
        output_filename = f"gl_output_{timestamp}_{file_id}.xlsx"
        local_file_path = os.path.join(OUTPUT_STORAGE_GL, output_filename)
        
        # Create the output directory if it doesn't exist
        os.makedirs(OUTPUT_STORAGE_GL, exist_ok=True)
        
        # Save to Excel
        output_df.to_excel(local_file_path, index=False, engine='openpyxl')
        
        # Upload to Azure Storage
        blob_url = upload_to_azure_storage(local_file_path, output_filename)
        
        # Clean up local file (optional)
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        
        return {
            "status": "success",
            "message": "GL file generated and uploaded successfully",
            "file_url": blob_url
        }
    except Exception as e:
        logger.error(f"Error in process_and_upload_gl: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/")
def read_root():
    """API Root endpoint"""
    return {"message": "GL File Generation API is running"}

@app.post("/api/generate-gl", response_model=Dict[str, Any])
async def generate_gl(background_tasks: BackgroundTasks, input_data: Dict[str, Any] = Body(...)):
    """
    Generate GL file from input data
    
    This endpoint accepts JSON input data, processes it to generate a GL file,
    and uploads the file to Azure Storage.
    
    The processing happens in the background to avoid timeout issues.
    
    Returns:
        JSON response with task ID and status
    """
    try:
        # Validate input data (basic validation)
        if not input_data:
            raise HTTPException(status_code=400, detail="Input data is required")
        
        # Generate a task ID
        task_id = str(uuid.uuid4())
        
        # Add the task to background tasks
        background_tasks.add_task(process_and_upload_gl, input_data)
        
        return JSONResponse(
            status_code=202,
            content={
                "task_id": task_id,
                "status": "processing",
                "message": "GL file generation task has been queued"
            }
        )
    except Exception as e:
        logger.error(f"Error in generate_gl endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/generate-gl-sync", response_model=Dict[str, Any])
async def generate_gl_sync(input_data: Dict[str, Any] = Body(...)):
    """
    Generate GL file from input data (synchronous version)
    
    This endpoint accepts JSON input data, processes it to generate a GL file,
    and uploads the file to Azure Storage. The processing happens synchronously,
    so it may take some time to complete.
    
    Returns:
        JSON response with file URL and status
    """
    try:
        # Validate input data
        if not input_data:
            raise HTTPException(status_code=400, detail="Input data is required")
        
        # Process the data and upload the file
        result = process_and_upload_gl(input_data)
        
        if result.get("status") == "success":
            return JSONResponse(
                status_code=200,
                content=result
            )
        else:
            raise HTTPException(status_code=500, detail=result.get("message", "Unknown error"))
    except Exception as e:
        logger.error(f"Error in generate_gl_sync endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)