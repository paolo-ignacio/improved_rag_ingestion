# Text extraction and chunking business logic

import logging
from datetime import datetime
from typing import Dict, Any
import io
from pypdf import PdfReader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from .vector import VectorService
import re
# Configure logging to monitor background worker performance
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


vector_service = VectorService()
def process_document_pipeline(job_id: Any, filename: str, file_bytes: bytes, db_ref: Dict):
    try:
        logger.info(f"Starting async ingestion for Job {job_id} ({filename})")
        
        # Phase A: Text Extraction
        # TODO: Implement PyPDF2/PdfReader or an OCR engine like Tesseract for scanned PDFs
        # text = extract_text_from_bytes(file_bytes)
        pdf_stream = io.BytesIO(file_bytes)

        reader = PdfReader(pdf_stream)

        pages_text = [page.extract_text() for page in reader.pages if page.extract_text()]
        text = "\n\n".join(pages_text)

        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text).strip()


        # Phase B: Structural Chunking
        # TODO: Implement LangChain's RecursiveCharacterTextSplitter or a semantic splitter
        # chunks = split_text_into_chunks(text, chunk_size=1000, chunk_overlap=200)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            separators=["\n\n", "\n", ". ", " ", ""],
            length_function=len,
            is_separator_regex=False
        )

        splits = text_splitter.split_text(text)
        # Phase C: Embeddings Generation & Vector Store Upsert
        # TODO: Call OpenAI/HuggingFace embeddings API and save array matrices to Pinecone
        # vectors = generate_embeddings(chunks)
        # pinecone_upsert(vectors, namespace=str(job_id))
        embeddings = vector_service.generate_embeddings(splits)
        if vector_service.upsert_embeddings_to_pinecone(embeddings=embeddings, splits=splits, filename=filename):
            # Phase D: Success State
            db_ref[job_id]["status"] = "completed"
            db_ref[job_id]["detail"] = f"Successfully processed and indexed into Pinecone."
            logger.info(f"Job {job_id} finished successfully.")
        else:
            print("Failed")

    except Exception as e:
        # Critical for async workers: Catch and log failures so the backend never crashes silently
        logger.error(f"Job {job_id} failed. Error: {str(e)}")
        db_ref[job_id]["status"] = "failed"
        db_ref[job_id]["detail"] = f"Pipeline execution failed: {str(e)}"
    
    finally:
        db_ref[job_id]["updated_at"] = datetime.utcnow()