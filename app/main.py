import logging
from pdf_tables_parser import PDFMarketParser
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import StreamingResponse
import os
import tempfile
import zipfile
import io
from pathlib import Path
import uuid
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

OUTPUT_PATH = "/data/output"
INPUT_PATH = "/data/input"
PDF_PATH = f"{INPUT_PATH}/241025 Unicredit Macro & Markets Weekly Focus - python.pdf"


app = FastAPI(title="PDF Processing API", version="1.0.0")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "PDF Processing API is running"}

@app.post("/process-pdf/")
async def process_pdf(
    file: UploadFile = File(..., description="PDF file to process"),
    markets_at_a_glance_page: int = Form(default=2, description="Page number for markets at a glance section"),
    major_events_page: int = Form(default=3, description="Page number for major events section"),
    ):
    """
    Takes a PDF file and return a ZIP containing generated csv files.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")
    if markets_at_a_glance_page < 1 or major_events_page < 1:
        raise HTTPException(status_code=400, detail="Page numbers must be positive")

    # Unique dir for process, to store pdf and output csvs
    temp_dir = tempfile.mkdtemp(prefix=f"pdf_processing_{uuid.uuid4().hex[:8]}_")

    try:
        logger.info("Parsing PDF file into CSV files")

        pdf_path = os.path.join(temp_dir, file.filename)
        with open(pdf_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

        parser = PDFMarketParser(
            pdf_path=pdf_path,
            year=2025
        )

        # Process the PDF and generate files
        parser.parse_markets_at_a_glance(markets_at_a_glance_page)
        parser.parse_major_events_next_week(major_events_page)
        generated_files = parser.export_dfs_to_csv(temp_dir)

        if not generated_files:
            raise HTTPException(status_code=500, detail="No files were generated during processing")

        # ZIP file in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in generated_files:
                if os.path.exists(file_path):
                    # Add file to ZIP with relative path
                    arcname = os.path.relpath(file_path, temp_dir)
                    zip_file.write(file_path, arcname)
        # After adding all files, we must rewind the internal pointer to beggining to save the zip to disk
        zip_buffer.seek(0)

        base_filename = Path(file.filename).stem
        zip_filename = f"{base_filename}_processed_files.zip"

        return StreamingResponse(
            io.BytesIO(zip_buffer.read()),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing PDF: {str(e)}")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
