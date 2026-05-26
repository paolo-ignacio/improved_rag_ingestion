from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException, status
from fastapi.responses import HTMLResponse
import os
import base64
from starlette.formparsers import MultiPartParser
from time import sleep

import uuid
from models.ingestion import JobStatusResponse, IngestionJobResponse
from datetime import datetime
from services.document import process_document_pipeline
app = FastAPI()

MultiPartParser.max_file_size = 50 * 1024 * 1024
JOB_DB = {}

@app.get('/')
async def interface_part():
    html_content = """
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>PDF Ingestion Dashboard</title>
            <style>
                :root {
                    --bg: #07111f;
                    --bg2: #0e1b31;
                    --panel: rgba(12, 20, 36, 0.82);
                    --panel-border: rgba(255, 255, 255, 0.08);
                    --text: #eaf0ff;
                    --muted: #9fb0d0;
                    --accent: #6ea8fe;
                    --accent-2: #4ade80;
                    --danger: #fb7185;
                    --warning: #fbbf24;
                    --shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
                }

                * {
                    box-sizing: border-box;
                }

                body {
                    margin: 0;
                    min-height: 100vh;
                    font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                    color: var(--text);
                    background:
                        radial-gradient(circle at top left, rgba(110, 168, 254, 0.24), transparent 32%),
                        radial-gradient(circle at top right, rgba(74, 222, 128, 0.18), transparent 26%),
                        linear-gradient(145deg, var(--bg), var(--bg2));
                }

                .shell {
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 40px 20px 56px;
                }

                .hero {
                    display: grid;
                    grid-template-columns: 1.25fr 0.85fr;
                    gap: 24px;
                    align-items: stretch;
                }

                .panel {
                    background: var(--panel);
                    border: 1px solid var(--panel-border);
                    backdrop-filter: blur(16px);
                    border-radius: 22px;
                    box-shadow: var(--shadow);
                }

                .intro {
                    padding: 32px;
                }

                .eyebrow {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    padding: 8px 12px;
                    border-radius: 999px;
                    background: rgba(110, 168, 254, 0.12);
                    color: var(--accent);
                    font-size: 12px;
                    letter-spacing: 0.08em;
                    text-transform: uppercase;
                    margin-bottom: 18px;
                }

                h1 {
                    font-size: clamp(2rem, 4vw, 3.8rem);
                    line-height: 1.02;
                    margin: 0 0 16px;
                }

                .subtitle {
                    margin: 0;
                    color: var(--muted);
                    font-size: 1.02rem;
                    line-height: 1.65;
                    max-width: 62ch;
                }

                .stats {
                    display: grid;
                    grid-template-columns: repeat(3, minmax(0, 1fr));
                    gap: 12px;
                    margin-top: 24px;
                }

                .stat {
                    padding: 14px 16px;
                    border-radius: 16px;
                    background: rgba(255, 255, 255, 0.04);
                    border: 1px solid rgba(255, 255, 255, 0.06);
                }

                .stat .label {
                    color: var(--muted);
                    font-size: 12px;
                    text-transform: uppercase;
                    letter-spacing: 0.06em;
                }

                .stat .value {
                    margin-top: 8px;
                    font-size: 1.1rem;
                    font-weight: 700;
                }

                .upload {
                    padding: 28px;
                    display: flex;
                    flex-direction: column;
                    gap: 16px;
                }

                .upload h2,
                .results h2 {
                    margin: 0;
                    font-size: 1.2rem;
                }

                .dropzone {
                    border: 1.5px dashed rgba(159, 176, 208, 0.45);
                    border-radius: 18px;
                    padding: 22px;
                    background: rgba(255, 255, 255, 0.03);
                }

                .dropzone strong {
                    display: block;
                    margin-bottom: 6px;
                }

                .dropzone p {
                    margin: 0;
                    color: var(--muted);
                    line-height: 1.5;
                    font-size: 0.95rem;
                }

                .file-input {
                    width: 100%;
                    color: var(--muted);
                }

                .actions {
                    display: flex;
                    gap: 12px;
                    flex-wrap: wrap;
                }

                .button {
                    border: 0;
                    border-radius: 14px;
                    padding: 14px 18px;
                    font-weight: 700;
                    cursor: pointer;
                    transition: transform 0.18s ease, opacity 0.18s ease, box-shadow 0.18s ease;
                }

                .button:hover {
                    transform: translateY(-1px);
                }

                .button.primary {
                    background: linear-gradient(135deg, #6ea8fe, #4ade80);
                    color: #07111f;
                    box-shadow: 0 12px 24px rgba(110, 168, 254, 0.25);
                }

                .button.secondary {
                    background: rgba(255, 255, 255, 0.06);
                    color: var(--text);
                    border: 1px solid rgba(255, 255, 255, 0.08);
                }

                .button:disabled {
                    opacity: 0.55;
                    cursor: not-allowed;
                    transform: none;
                }

                .hint {
                    color: var(--muted);
                    font-size: 0.92rem;
                }

                .results {
                    margin-top: 24px;
                    padding: 28px;
                }

                .results-header {
                    display: flex;
                    justify-content: space-between;
                    gap: 12px;
                    align-items: end;
                    flex-wrap: wrap;
                    margin-bottom: 18px;
                }

                .results-header p {
                    margin: 0;
                    color: var(--muted);
                }

                .job-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
                    gap: 16px;
                }

                .job-card {
                    padding: 18px;
                    border-radius: 18px;
                    background: rgba(255, 255, 255, 0.04);
                    border: 1px solid rgba(255, 255, 255, 0.06);
                }

                .job-name {
                    margin: 0 0 10px;
                    font-size: 1rem;
                    font-weight: 700;
                    word-break: break-word;
                }

                .job-meta {
                    color: var(--muted);
                    font-size: 0.92rem;
                    line-height: 1.5;
                    margin: 0 0 14px;
                }

                .badge {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    padding: 8px 12px;
                    border-radius: 999px;
                    font-size: 0.84rem;
                    font-weight: 700;
                    letter-spacing: 0.02em;
                    margin-bottom: 12px;
                }

                .badge.processing {
                    background: rgba(251, 191, 36, 0.14);
                    color: var(--warning);
                }

                .badge.completed {
                    background: rgba(74, 222, 128, 0.14);
                    color: var(--accent-2);
                }

                .badge.completed .checkmark {
                    width: 12px;
                    height: 12px;
                    border-radius: 50%;
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    background: rgba(74, 222, 128, 0.2);
                    color: var(--accent-2);
                    font-size: 10px;
                    line-height: 1;
                }

                .badge.failed {
                    background: rgba(251, 113, 133, 0.14);
                    color: var(--danger);
                }

                .badge.queued {
                    background: rgba(110, 168, 254, 0.12);
                    color: var(--accent);
                }

                .detail {
                    margin: 10px 0 0;
                    color: var(--text);
                    font-size: 0.92rem;
                    line-height: 1.5;
                }

                .empty-state {
                    padding: 24px;
                    border-radius: 18px;
                    border: 1px dashed rgba(159, 176, 208, 0.35);
                    color: var(--muted);
                    text-align: center;
                }

                .status-line {
                    margin-top: 10px;
                    font-size: 0.9rem;
                    color: var(--muted);
                }

                .status-line.error {
                    color: var(--danger);
                }

                .spinner {
                    width: 12px;
                    height: 12px;
                    border-radius: 50%;
                    border: 2px solid rgba(255, 255, 255, 0.18);
                    border-top-color: var(--accent);
                    animation: spin 1s linear infinite;
                    display: inline-block;
                }

                @keyframes spin {
                    to {
                        transform: rotate(360deg);
                    }
                }

                @media (max-width: 900px) {
                    .hero {
                        grid-template-columns: 1fr;
                    }

                    .stats {
                        grid-template-columns: 1fr;
                    }
                }

                @media (max-width: 640px) {
                    .shell {
                        padding: 20px 14px 40px;
                    }

                    .intro,
                    .upload,
                    .results {
                        padding: 20px;
                    }
                }
            </style>
        </head>
        <body>
            <div class="shell">
                <section class="hero">
                    <div class="panel intro">
                        <div class="eyebrow">PDF Ingestion Dashboard</div>
                        <h1>Upload PDFs, track ingestion, and watch each document finish.</h1>
                        <p class="subtitle">
                            Each PDF gets its own job ID. The backend marks the job as <strong>processing</strong>, then updates it to <strong>completed</strong> or <strong>failed</strong> when the background worker finishes.
                        </p>
                        <div class="stats">
                            <div class="stat">
                                <div class="label">Workflow</div>
                                <div class="value">Upload -> Process -> Track</div>
                            </div>
                            <div class="stat">
                                <div class="label">Status</div>
                                <div class="value">Live polling</div>
                            </div>
                            <div class="stat">
                                <div class="label">Output</div>
                                <div class="value">One result per PDF</div>
                            </div>
                        </div>
                    </div>

                    <div class="panel upload">
                        <h2>Upload Documents</h2>
                        <div class="dropzone">
                            <strong>Choose one or more PDF files</strong>
                            <p>Supported files: PDF only. The UI will show a live job card for each upload and update its status automatically.</p>
                        </div>
                        <form id="upload-form">
                            <input class="file-input" id="files" name="files" type="file" multiple accept="application/pdf,.pdf" required>
                            <div style="height: 12px;"></div>
                            <div class="actions">
                                <button class="button primary" id="upload-btn" type="submit">Start Ingestion</button>
                                <button class="button secondary" id="clear-btn" type="button">Clear Results</button>
                            </div>
                            <div class="hint" id="file-hint">No files selected yet.</div>
                            <div class="status-line" id="upload-status"></div>
                        </form>
                    </div>
                </section>

                <section class="panel results">
                    <div class="results-header">
                        <div>
                            <h2>Job Status</h2>
                            <p>Each PDF gets its own card. Refresh is handled by the page.</p>
                        </div>
                    </div>
                    <div id="jobs" class="job-grid">
                        <div class="empty-state" id="empty-state">Uploaded jobs will appear here after you submit files.</div>
                    </div>
                </section>
            </div>

            <script>
                const form = document.getElementById('upload-form');
                const fileInput = document.getElementById('files');
                const fileHint = document.getElementById('file-hint');
                const uploadStatus = document.getElementById('upload-status');
                const jobsContainer = document.getElementById('jobs');
                const emptyState = document.getElementById('empty-state');
                const uploadBtn = document.getElementById('upload-btn');
                const clearBtn = document.getElementById('clear-btn');
                const pollingTimers = new Map();

                function setStatusMessage(message, isError = false) {
                    uploadStatus.textContent = message;
                    uploadStatus.className = isError ? 'status-line error' : 'status-line';
                }

                function clearPolling(jobId) {
                    const timer = pollingTimers.get(jobId);
                    if (timer) {
                        clearInterval(timer);
                        pollingTimers.delete(jobId);
                    }
                }

                function statusClass(status) {
                    const value = (status || '').toLowerCase();
                    if (value === 'completed') return 'completed';
                    if (value === 'failed') return 'failed';
                    if (value === 'processing') return 'processing';
                    return 'queued';
                }

                function statusLabel(status) {
                    const value = (status || '').toLowerCase();
                    if (value === 'completed') return 'Completed';
                    if (value === 'failed') return 'Failed';
                    if (value === 'processing') return 'Processing';
                    return 'Queued';
                }

                function ensureJobsVisible() {
                    if (emptyState) {
                        emptyState.style.display = 'none';
                    }
                }

                function createJobCard(job) {
                    ensureJobsVisible();

                    const card = document.createElement('article');
                    card.className = 'job-card';
                    card.dataset.jobId = job.job_id;
                    card.innerHTML = `
                        <div class="badge queued" data-role="badge"><span class="spinner"></span><span data-role="badge-text">Queued</span></div>
                        <h3 class="job-name" data-role="filename"></h3>
                        <p class="job-meta" data-role="meta"></p>
                        <p class="detail" data-role="detail"></p>
                    `;

                    card.querySelector('[data-role="filename"]').textContent = job.file_name;
                    card.querySelector('[data-role="meta"]').textContent = `Job ID: ${job.job_id}`;
                    card.querySelector('[data-role="detail"]').textContent = 'Waiting for the background worker to start.';
                    updateCard(card, { status: job.status || 'queued', detail: 'Waiting for the background worker to start.' });

                    jobsContainer.prepend(card);
                    return card;
                }

                function updateCard(card, statusPayload) {
                    const status = statusPayload.status || 'queued';
                    const detail = statusPayload.detail || '';
                    const badge = card.querySelector('[data-role="badge"]');
                    const badgeText = card.querySelector('[data-role="badge-text"]');
                    const detailNode = card.querySelector('[data-role="detail"]');

                    badge.className = `badge ${statusClass(status)}`;
                    if (statusClass(status) === 'processing') {
                        badge.innerHTML = '<span class="spinner"></span><span data-role="badge-text">Processing</span>';
                    } else if (statusClass(status) === 'completed') {
                        badge.innerHTML = '<span class="checkmark">✓</span><span data-role="badge-text">Completed</span>';
                    } else {
                        badge.innerHTML = `<span data-role="badge-text">${statusLabel(status)}</span>`;
                    }

                    if (badgeText) {
                        badgeText.textContent = statusLabel(status);
                    }

                    detailNode.textContent = detail || (statusClass(status) === 'completed'
                        ? 'Document indexed successfully.'
                        : statusClass(status) === 'failed'
                            ? 'The worker failed. Check the error message above.'
                            : 'Processing in progress...');
                }

                async function pollJob(jobId, card) {
                    clearPolling(jobId);

                    const timer = setInterval(async () => {
                        try {
                            const response = await fetch(`/api/v1/ingestion/status/${jobId}`);
                            if (!response.ok) {
                                throw new Error(`Status request failed (${response.status})`);
                            }

                            const data = await response.json();
                            updateCard(card, data);

                            const currentStatus = (data.status || '').toLowerCase();
                            if (currentStatus === 'completed' || currentStatus === 'failed') {
                                clearPolling(jobId);
                            }
                        } catch (error) {
                            updateCard(card, { status: 'failed', detail: error.message });
                            clearPolling(jobId);
                        }
                    }, 3000);

                    pollingTimers.set(jobId, timer);
                }

                fileInput.addEventListener('change', () => {
                    const count = fileInput.files.length;
                    fileHint.textContent = count === 0
                        ? 'No files selected yet.'
                        : `${count} file${count === 1 ? '' : 's'} selected.`;
                });

                clearBtn.addEventListener('click', () => {
                    pollingTimers.forEach((timer) => clearInterval(timer));
                    pollingTimers.clear();
                    jobsContainer.innerHTML = '<div class="empty-state" id="empty-state">Uploaded jobs will appear here after you submit files.</div>';
                    setStatusMessage('');
                });

                form.addEventListener('submit', async (event) => {
                    event.preventDefault();

                    if (!fileInput.files.length) {
                        setStatusMessage('Please choose at least one PDF file.', true);
                        return;
                    }

                    uploadBtn.disabled = true;
                    setStatusMessage('Submitting files to the ingestion pipeline...');

                    try {
                        const formData = new FormData();
                        Array.from(fileInput.files).forEach((file) => {
                            formData.append('files', file);
                        });

                        const response = await fetch('/upload', {
                            method: 'POST',
                            body: formData,
                        });

                        const payload = await response.json();

                        if (!response.ok) {
                            const message = payload && payload.detail ? payload.detail : 'Upload failed.';
                            throw new Error(message);
                        }

                        jobsContainer.innerHTML = '';
                        const jobs = Array.isArray(payload) ? payload : [payload];

                        jobs.forEach((job) => {
                            const card = createJobCard(job);
                            pollJob(job.job_id, card);
                        });

                        setStatusMessage(`Submitted ${jobs.length} job${jobs.length === 1 ? '' : 's'}. Tracking status automatically.`);
                    } catch (error) {
                        setStatusMessage(error.message, true);
                    } finally {
                        uploadBtn.disabled = false;
                    }
                });
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

@app.post('/upload',
          status_code = status.HTTP_202_ACCEPTED,
          response_model=list[IngestionJobResponse]
        )
async def upload_file(background_tasks: BackgroundTasks, files: list[UploadFile] = File(...)):
    
    response_jobs = []
    for file in files:
        file_bytes = await file.read()
        if len(file_bytes) > MultiPartParser.max_file_size:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Oops, pdf with size 1 or less than 1 mb is supported."
            )
        if not file.filename.endswith('.pdf'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only PDF documents are supported"
            )


        job_id = uuid.uuid4()
        JOB_DB[job_id] = {
            "status":"processing",
            "file_name":file.filename,
            "updated_at": datetime.now()
        }


        background_tasks.add_task(
            process_document_pipeline,
            job_id=job_id,
            filename=file.filename,
            file_bytes=file_bytes,
            db_ref=JOB_DB

        )
        response_jobs.append(
            IngestionJobResponse(
                job_id=job_id,
                file_name=file.filename,
                created_at=JOB_DB[job_id]['updated_at']
            )
        )
    return response_jobs

@app.get('/status/{job_id}',
        response_model=JobStatusResponse 
        )
async def get_job_status(job_id: uuid.UUID):

    if job_id not in JOB_DB:
        raise HTTPException(
            status_code=404,
            detail="Ingestion job not found"
        )


    job = JOB_DB[job_id]
    return JobStatusResponse(
        status=job["status"],
        job_id=job_id,
        detail=job.get('detail'),
        updated_at=job['updated_at'],
        file_name=job['file_name']
    )


@app.get('/api/v1/ingestion/status/{job_id}', response_model=JobStatusResponse)
async def get_job_status_v1(job_id: uuid.UUID):
    return await get_job_status(job_id)


