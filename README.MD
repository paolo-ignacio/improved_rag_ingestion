# RAG Engine Backend

FastAPI backend for uploading PDF documents, extracting text, chunking it, generating embeddings, and upserting vectors into Pinecone. The app also includes a small browser-based dashboard that lets you upload multiple PDFs and watch each job move from `processing` to `completed` or `failed` in real time.

## Features

- Upload one or more PDF files from the built-in UI
- Extract PDF text with `pypdf`
- Clean and chunk text with `langchain-text-splitters`
- Generate embeddings with `sentence-transformers`
- Store vectors in Pinecone
- Poll each job status until it finishes
- Dockerized development setup

## Architecture

```mermaid
flowchart LR
	A[User uploads PDFs in browser UI] --> B[FastAPI /upload]
	B --> C[Create job_id + mark processing]
	C --> D[Background worker: process_document_pipeline]
	D --> E[Read PDF with pypdf]
	E --> F[Clean + chunk text]
	F --> G[Generate embeddings]
	G --> H[Upsert vectors to Pinecone]
	H --> I[Update job status: completed]
	D --> J{Any error?}
	J -->|Yes| K[Update job status: failed + detail]
	I --> L[Frontend polls /api/v1/ingestion/status/{job_id}]
	K --> L
```

## Project Structure

```text
rag_engine_backend/
├── app/
│   ├── config.py
│   ├── main.py
│   ├── models/
│   │   └── ingestion.py
│   ├── routers/
│   │   └── ingestion.py
│   └── services/
│       ├── document.py
│       └── vector.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.MD
```

## Requirements

- Python 3.14
- Docker and Docker Compose, if you want to run it in a container
- Pinecone API key and index name

## Environment Variables

Create a `.env` file in the `app` folder:

```env
PINECONE_API_KEY=your-pinecone-api-key
PINECONE_INDEX_NAME=your-index-name
```

An example file is provided in [\.env.example](.env.example).

## Local Setup

1. Create and activate a Python 3.14 virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Start the app from the `app` directory:

```bash
cd app
uvicorn main:app --reload
```

4. Open the app in your browser:

```text
http://127.0.0.1:8000
```

## Docker Setup

1. Make sure `app/.env` exists.
2. Build and run the container:

```bash
docker compose up --build
```

3. Open the app in your browser:

```text
http://localhost:8000
```

## API Endpoints

- `GET /` - Browser UI
- `POST /upload` - Upload one or more PDFs
- `GET /status/{job_id}` - Check job status
- `GET /api/v1/ingestion/status/{job_id}` - Versioned job status endpoint used by the UI

## Workflow

1. Upload PDFs from the UI.
2. The backend creates a job record for each file and sets the status to `processing`.
3. A background worker extracts and cleans text, splits it into chunks, generates embeddings, and uploads them to Pinecone.
4. The worker updates the job to `completed` or `failed`.
5. The frontend polls the status endpoint and updates each card automatically.


## Notes

- The app currently uses in-memory job tracking, so job history resets when the process restarts.
- If a PDF is scanned or image-based, `pypdf` may extract little or no text, and you may need OCR later.
- The container image is pinned to Python 3.14 to match the project environment.

## Troubleshooting

- If uploads fail immediately, verify `PINECONE_API_KEY` and `PINECONE_INDEX_NAME` in `app/.env`.
- If Docker build fails, make sure you are using the corrected Python 3.11 base image.
- If the UI does not update, check that the backend is running and that the status endpoint is reachable.

## License

No license file is currently included.
