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
            <title>RAG Ingestion Studio</title>
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;700&family=Sora:wght@300;400;600;700&display=swap');

                :root {
                    --bg: #f8f2ea;
                    --bg2: #efe3d6;
                    --panel: rgba(255, 255, 255, 0.9);
                    --panel-border: rgba(31, 42, 52, 0.12);
                    --text: #1f2a35;
                    --muted: #58677a;
                    --accent: #ff6b4a;
                    --accent-2: #1c9c8a;
                    --accent-3: #f2b847;
                    --danger: #d64545;
                    --warning: #f2b847;
                    --shadow: 0 18px 40px rgba(33, 41, 52, 0.15);
                }

                * {
                    box-sizing: border-box;
                }

                body {
                    margin: 0;
                    min-height: 100vh;
                    font-family: "Sora", sans-serif;
                    color: var(--text);
                    background:
                        radial-gradient(1200px 520px at 10% -10%, rgba(255, 107, 74, 0.22), transparent 60%),
                        radial-gradient(980px 560px at 90% 0%, rgba(28, 156, 138, 0.18), transparent 58%),
                        linear-gradient(155deg, var(--bg), var(--bg2));
                }

                body::before {
                    content: "";
                    position: fixed;
                    inset: 0;
                    background:
                        radial-gradient(circle at 15% 20%, rgba(255, 107, 74, 0.12), transparent 44%),
                        radial-gradient(circle at 85% 10%, rgba(28, 156, 138, 0.12), transparent 38%),
                        radial-gradient(circle at 70% 80%, rgba(242, 184, 71, 0.12), transparent 40%);
                    opacity: 0.85;
                    pointer-events: none;
                    z-index: -2;
                }

                body::after {
                    content: "";
                    position: fixed;
                    inset: 0;
                    background-image:
                        linear-gradient(rgba(31, 42, 52, 0.05) 1px, transparent 1px),
                        linear-gradient(90deg, rgba(31, 42, 52, 0.05) 1px, transparent 1px);
                    background-size: 140px 140px;
                    opacity: 0.45;
                    pointer-events: none;
                    z-index: -1;
                }

                h1,
                h2,
                h3 {
                    font-family: "Fraunces", "Times New Roman", serif;
                    letter-spacing: -0.02em;
                }

                .shell {
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 36px 20px 56px;
                }

                .topbar {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 16px;
                    margin-bottom: 24px;
                }

                .brand {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }

                .logo-mark {
                    width: 46px;
                    height: 46px;
                    border-radius: 16px;
                    background: linear-gradient(140deg, var(--accent), var(--accent-2));
                    box-shadow: 0 12px 30px rgba(95, 224, 200, 0.35);
                }

                .brand-title {
                    font-size: 0.95rem;
                    text-transform: uppercase;
                    letter-spacing: 0.28em;
                    color: var(--muted);
                    margin-bottom: 4px;
                }

                .brand-subtitle {
                    font-size: 1.1rem;
                    font-weight: 600;
                }

                .topbar-actions {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    flex-wrap: wrap;
                }

                .pill {
                    padding: 6px 12px;
                    border-radius: 999px;
                    background: rgba(28, 156, 138, 0.12);
                    color: var(--accent-2);
                    font-size: 0.8rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.08em;
                }

                .link {
                    color: var(--text);
                    text-decoration: none;
                    font-weight: 600;
                    padding-bottom: 2px;
                    border-bottom: 1px solid rgba(31, 42, 52, 0.2);
                }

                .link:hover {
                    color: var(--accent-2);
                    border-bottom-color: var(--accent-2);
                }

                .hero {
                    display: grid;
                    grid-template-columns: 1.2fr 0.9fr;
                    gap: 24px;
                    align-items: stretch;
                }

                .panel {
                    background: var(--panel);
                    border: 1px solid var(--panel-border);
                    backdrop-filter: blur(18px);
                    border-radius: 24px;
                    box-shadow: var(--shadow);
                    animation: fade-up 0.6s ease;
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
                    background: rgba(255, 107, 74, 0.14);
                    color: var(--accent);
                    font-size: 12px;
                    letter-spacing: 0.08em;
                    text-transform: uppercase;
                    margin-bottom: 18px;
                }

                h1 {
                    font-size: clamp(2.1rem, 4vw, 3.7rem);
                    line-height: 1.02;
                    margin: 0 0 16px;
                }

                .subtitle {
                    margin: 0;
                    color: var(--muted);
                    font-size: 1.02rem;
                    line-height: 1.7;
                    max-width: 62ch;
                }

                .flow {
                    margin-top: 24px;
                    display: grid;
                    gap: 14px;
                }

                .flow-step {
                    display: flex;
                    gap: 14px;
                    align-items: flex-start;
                }

                .step-index {
                    width: 32px;
                    height: 32px;
                    border-radius: 50%;
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    background: rgba(242, 193, 78, 0.16);
                    color: var(--accent-2);
                    font-weight: 700;
                    font-size: 0.9rem;
                }

                .step-title {
                    font-weight: 700;
                    margin-bottom: 4px;
                }

                .step-detail {
                    color: var(--muted);
                    font-size: 0.92rem;
                }

                .stats {
                    display: grid;
                    grid-template-columns: repeat(3, minmax(0, 1fr));
                    gap: 12px;
                    margin-top: 26px;
                }

                .stat {
                    padding: 14px 16px;
                    border-radius: 16px;
                    background: rgba(255, 255, 255, 0.75);
                    border: 1px solid rgba(31, 42, 52, 0.08);
                }

                .stat .label {
                    color: var(--muted);
                    font-size: 12px;
                    text-transform: uppercase;
                    letter-spacing: 0.06em;
                }

                .stat .value {
                    margin-top: 8px;
                    font-size: 1.05rem;
                    font-weight: 700;
                }

                .upload {
                    padding: 28px;
                    display: flex;
                    flex-direction: column;
                    gap: 18px;
                }

                .upload-form {
                    display: flex;
                    flex-direction: column;
                    gap: 16px;
                }

                .upload h2,
                .results h2 {
                    margin: 0;
                    font-size: 1.35rem;
                }

                .dropzone {
                    border: 2px dashed rgba(31, 42, 52, 0.2);
                    border-radius: 20px;
                    padding: 22px;
                    background: rgba(255, 255, 255, 0.7);
                    transition: border-color 0.2s ease, background 0.2s ease, transform 0.2s ease;
                    cursor: pointer;
                }

                .dropzone.dragover {
                    border-color: var(--accent-2);
                    background: rgba(28, 156, 138, 0.08);
                    transform: translateY(-2px);
                }

                .dropzone.disabled {
                    opacity: 0.7;
                    pointer-events: none;
                }

                .drop-title {
                    font-weight: 700;
                    margin-bottom: 6px;
                }

                .dropzone p {
                    margin: 0;
                    color: var(--muted);
                    line-height: 1.5;
                    font-size: 0.95rem;
                }

                .drop-actions {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    margin-top: 14px;
                    flex-wrap: wrap;
                }

                .file-input {
                    display: none;
                }

                .file-list {
                    list-style: none;
                    margin: 0;
                    padding: 0;
                    display: grid;
                    gap: 10px;
                }

                .file-item {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 12px;
                    padding: 12px 14px;
                    border-radius: 14px;
                    background: rgba(255, 255, 255, 0.92);
                    border: 1px solid rgba(31, 42, 52, 0.1);
                }

                .file-info {
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                }

                .file-name {
                    font-weight: 600;
                    word-break: break-word;
                }

                .file-size {
                    color: var(--muted);
                    font-size: 0.85rem;
                }

                .file-remove {
                    border: 1px solid rgba(31, 42, 52, 0.15);
                    background: rgba(31, 42, 52, 0.05);
                    color: var(--text);
                    border-radius: 12px;
                    padding: 6px 10px;
                    cursor: pointer;
                    font-size: 0.85rem;
                }

                .file-remove:hover {
                    border-color: rgba(31, 42, 52, 0.3);
                }

                .file-summary {
                    display: flex;
                    justify-content: space-between;
                    gap: 12px;
                    font-size: 0.9rem;
                    color: var(--muted);
                    flex-wrap: wrap;
                }

                .progress {
                    height: 10px;
                    border-radius: 999px;
                    background: rgba(31, 42, 52, 0.12);
                    overflow: hidden;
                    opacity: 0;
                    transition: opacity 0.2s ease;
                }

                .progress.active {
                    opacity: 1;
                }

                .progress-bar {
                    height: 100%;
                    width: 0;
                    background: linear-gradient(135deg, var(--accent), var(--accent-2));
                    transition: width 0.2s ease;
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
                    background: linear-gradient(135deg, var(--accent), var(--accent-2));
                    color: #1f2a35;
                    box-shadow: 0 12px 24px rgba(255, 107, 74, 0.25);
                }

                .button.secondary {
                    background: rgba(31, 42, 52, 0.08);
                    color: var(--text);
                    border: 1px solid rgba(31, 42, 52, 0.18);
                }

                .button:disabled {
                    opacity: 0.55;
                    cursor: not-allowed;
                    transform: none;
                }

                .hint {
                    color: var(--muted);
                    font-size: 0.9rem;
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

                .results-note {
                    font-size: 0.9rem;
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
                    background: rgba(255, 255, 255, 0.92);
                    border: 1px solid rgba(31, 42, 52, 0.12);
                    animation: fade-up 0.5s ease;
                }

                .job-top {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 12px;
                    margin-bottom: 8px;
                }

                .copy-btn {
                    border: 1px solid rgba(31, 42, 52, 0.2);
                    background: rgba(31, 42, 52, 0.06);
                    color: var(--text);
                    border-radius: 10px;
                    padding: 6px 10px;
                    font-size: 0.78rem;
                    cursor: pointer;
                }

                .copy-btn:hover {
                    border-color: rgba(31, 42, 52, 0.35);
                }

                .job-name {
                    margin: 0 0 10px;
                    font-size: 1rem;
                    font-weight: 700;
                    word-break: break-word;
                }

                .job-meta {
                    color: var(--muted);
                    font-size: 0.88rem;
                    line-height: 1.5;
                    margin: 0 0 12px;
                    word-break: break-word;
                }

                .job-footer {
                    margin-top: 12px;
                    color: var(--muted);
                    font-size: 0.82rem;
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
                }

                .badge.processing {
                    background: rgba(242, 184, 71, 0.22);
                    color: #a05b00;
                }

                .badge.completed {
                    background: rgba(28, 156, 138, 0.18);
                    color: var(--accent-2);
                }

                .badge.failed {
                    background: rgba(214, 69, 69, 0.2);
                    color: var(--danger);
                }

                .badge.queued {
                    background: rgba(255, 107, 74, 0.15);
                    color: var(--accent);
                }

                .badge .dot {
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                    background: currentColor;
                }

                .detail {
                    margin: 6px 0 0;
                    color: var(--text);
                    font-size: 0.92rem;
                    line-height: 1.5;
                }

                .empty-state {
                    padding: 24px;
                    border-radius: 18px;
                    border: 1px dashed rgba(31, 42, 52, 0.2);
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
                    border: 2px solid rgba(31, 42, 52, 0.25);
                    border-top-color: var(--accent-2);
                    animation: spin 1s linear infinite;
                    display: inline-block;
                }

                @keyframes spin {
                    to {
                        transform: rotate(360deg);
                    }
                }

                @keyframes fade-up {
                    from {
                        opacity: 0;
                        transform: translateY(8px);
                    }
                    to {
                        opacity: 1;
                        transform: translateY(0);
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

                @media (prefers-reduced-motion: reduce) {
                    * {
                        animation: none !important;
                        transition: none !important;
                    }
                }
            </style>
        </head>
        <body>
            <div class="shell">
                <header class="topbar">
                    <div class="brand">
                        <div class="logo-mark"></div>
                        <div>
                            <div class="brand-title">RAG Ingestion</div>
                            <div class="brand-subtitle">PDF to Pinecone Studio</div>
                        </div>
                    </div>
                    <div class="topbar-actions">
                        <span class="pill">API online</span>
                        <a class="link" href="/docs" target="_blank" rel="noreferrer">Open API docs</a>
                    </div>
                </header>

                <section class="hero">
                    <div class="panel intro">
                        <div class="eyebrow">Ingestion control center</div>
                        <h1>Upload PDFs and watch ingestion resolve in real time.</h1>
                        <p class="subtitle">
                            Each PDF becomes a job with a dedicated status card. The pipeline extracts, chunks, and indexes
                            the document while the UI keeps you informed with live updates.
                        </p>
                        <div class="flow">
                            <div class="flow-step">
                                <span class="step-index">1</span>
                                <div>
                                    <div class="step-title">Upload PDFs</div>
                                    <div class="step-detail">Drag-and-drop or browse. File size and type are validated instantly.</div>
                                </div>
                            </div>
                            <div class="flow-step">
                                <span class="step-index">2</span>
                                <div>
                                    <div class="step-title">Track processing</div>
                                    <div class="step-detail">Jobs auto-refresh every 3 seconds until they complete.</div>
                                </div>
                            </div>
                            <div class="flow-step">
                                <span class="step-index">3</span>
                                <div>
                                    <div class="step-title">Confirm indexing</div>
                                    <div class="step-detail">Finished jobs report success or errors with full details.</div>
                                </div>
                            </div>
                        </div>
                        <div class="stats">
                            <div class="stat">
                                <div class="label">Max file size</div>
                                <div class="value">50 MB per PDF</div>
                            </div>
                            <div class="stat">
                                <div class="label">Polling</div>
                                <div class="value">Auto refresh every 3s</div>
                            </div>
                            <div class="stat">
                                <div class="label">Index target</div>
                                <div class="value">Pinecone vectors</div>
                            </div>
                        </div>
                    </div>

                    <div class="panel upload">
                        <form id="upload-form" class="upload-form">
                            <h2>Upload queue</h2>
                            <div class="dropzone" id="dropzone">
                                <div class="drop-title">Drag & drop your PDFs here</div>
                                <p>Multiple files supported. Non-PDFs and oversize files are skipped automatically.</p>
                                <div class="drop-actions">
                                    <button class="button secondary" id="browse-btn" type="button">Browse files</button>
                                    <span class="hint">Max 50 MB each</span>
                                </div>
                                <input class="file-input" id="files" name="files" type="file" multiple accept="application/pdf,.pdf">
                            </div>
                            <ul class="file-list" id="file-list"></ul>
                            <div class="file-summary">
                                <span id="file-total">0 files · 0 B</span>
                                <span id="file-limit">Limit: 50 MB each</span>
                            </div>
                            <div class="progress" id="progress-track" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
                                <div class="progress-bar" id="upload-progress"></div>
                            </div>
                            <div class="actions">
                                <button class="button primary" id="upload-btn" type="submit">Start ingestion</button>
                                <button class="button secondary" id="clear-btn" type="button">Clear results</button>
                            </div>
                            <div class="hint" id="file-hint">No files selected yet.</div>
                            <div class="status-line" id="upload-status" role="status" aria-live="polite"></div>
                        </form>
                    </div>
                </section>

                <section class="panel results">
                    <div class="results-header">
                        <div>
                            <h2>Job status</h2>
                            <p>Each PDF gets its own card. The list is preserved across reloads.</p>
                        </div>
                        <div class="results-note">Auto-refresh every 3s while jobs are active.</div>
                    </div>
                    <div id="jobs" class="job-grid">
                        <div class="empty-state" id="empty-state">Uploaded jobs will appear here after you submit files.</div>
                    </div>
                </section>
            </div>

            <script>
                const form = document.getElementById('upload-form');
                const fileInput = document.getElementById('files');
                const fileList = document.getElementById('file-list');
                const fileHint = document.getElementById('file-hint');
                const fileTotal = document.getElementById('file-total');
                const uploadStatus = document.getElementById('upload-status');
                const jobsContainer = document.getElementById('jobs');
                const uploadBtn = document.getElementById('upload-btn');
                const clearBtn = document.getElementById('clear-btn');
                const browseBtn = document.getElementById('browse-btn');
                const dropzone = document.getElementById('dropzone');
                const progressTrack = document.getElementById('progress-track');
                const progressBar = document.getElementById('upload-progress');
                const pollingTimers = new Map();
                const selectedFiles = new Map();
                const STORAGE_KEY = 'ingestion_jobs';
                const MAX_FILE_SIZE = 50 * 1024 * 1024;

                function setStatusMessage(message, isError = false) {
                    uploadStatus.textContent = message;
                    uploadStatus.className = isError ? 'status-line error' : 'status-line';
                }

                function formatBytes(bytes) {
                    if (!bytes && bytes !== 0) return '0 B';
                    const units = ['B', 'KB', 'MB', 'GB'];
                    let size = bytes;
                    let unitIndex = 0;
                    while (size >= 1024 && unitIndex < units.length - 1) {
                        size /= 1024;
                        unitIndex += 1;
                    }
                    const decimals = unitIndex === 0 ? 0 : 1;
                    return `${size.toFixed(decimals)} ${units[unitIndex]}`;
                }

                function formatTimestamp(value) {
                    if (!value) return 'just now';
                    const date = value instanceof Date ? value : new Date(value);
                    if (Number.isNaN(date.getTime())) return 'just now';
                    return date.toLocaleString();
                }

                function fileKey(file) {
                    return `${file.name}|${file.size}|${file.lastModified}`;
                }

                function syncFileInput() {
                    const dataTransfer = new DataTransfer();
                    Array.from(selectedFiles.values()).forEach((file) => dataTransfer.items.add(file));
                    fileInput.files = dataTransfer.files;
                }

                function updateSelectionSummary() {
                    const count = selectedFiles.size;
                    const totalBytes = Array.from(selectedFiles.values()).reduce((sum, file) => sum + file.size, 0);
                    fileHint.textContent = count === 0
                        ? 'No files selected yet.'
                        : `${count} file${count === 1 ? '' : 's'} ready to upload.`;
                    fileTotal.textContent = `${count} file${count === 1 ? '' : 's'} · ${formatBytes(totalBytes)}`;
                }

                function renderFileList() {
                    fileList.innerHTML = '';
                    if (selectedFiles.size === 0) {
                        updateSelectionSummary();
                        return;
                    }

                    Array.from(selectedFiles.entries()).forEach(([key, file]) => {
                        const item = document.createElement('li');
                        item.className = 'file-item';
                        item.innerHTML = `
                            <div class="file-info">
                                <span class="file-name">${file.name}</span>
                                <span class="file-size">${formatBytes(file.size)}</span>
                            </div>
                            <button class="file-remove" type="button" data-key="${key}">Remove</button>
                        `;
                        fileList.appendChild(item);
                    });

                    updateSelectionSummary();
                }

                function addFiles(files) {
                    const invalidFiles = [];
                    const oversizeFiles = [];

                    Array.from(files).forEach((file) => {
                        if (!file.name.toLowerCase().endsWith('.pdf')) {
                            invalidFiles.push(file);
                            return;
                        }

                        if (file.size > MAX_FILE_SIZE) {
                            oversizeFiles.push(file);
                            return;
                        }

                        selectedFiles.set(fileKey(file), file);
                    });

                    syncFileInput();
                    renderFileList();

                    if (invalidFiles.length) {
                        setStatusMessage(`Ignored ${invalidFiles.length} non-PDF file${invalidFiles.length === 1 ? '' : 's'}.`, true);
                    } else if (oversizeFiles.length) {
                        setStatusMessage(`Skipped ${oversizeFiles.length} file${oversizeFiles.length === 1 ? '' : 's'} over 50 MB.`, true);
                    } else {
                        setStatusMessage('');
                    }
                }

                function clearSelection() {
                    selectedFiles.clear();
                    syncFileInput();
                    renderFileList();
                }

                function setUploadState(isUploading) {
                    uploadBtn.disabled = isUploading;
                    browseBtn.disabled = isUploading;
                    clearBtn.disabled = isUploading;
                    dropzone.classList.toggle('disabled', isUploading);
                }

                function setProgress(value, isActive) {
                    progressTrack.classList.toggle('active', Boolean(isActive));
                    progressBar.style.width = `${value}%`;
                    progressTrack.setAttribute('aria-valuenow', Math.round(value));
                }

                function loadStoredJobs() {
                    try {
                        const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
                        return Array.isArray(stored) ? stored : [];
                    } catch (error) {
                        return [];
                    }
                }

                function saveStoredJobs(jobs) {
                    localStorage.setItem(STORAGE_KEY, JSON.stringify(jobs));
                }

                function upsertStoredJob(job) {
                    const jobs = loadStoredJobs();
                    const jobId = String(job.job_id);
                    const index = jobs.findIndex((item) => item.job_id === jobId);
                    const nextJob = { ...job, job_id: jobId };

                    if (index >= 0) {
                        jobs[index] = { ...jobs[index], ...nextJob };
                    } else {
                        jobs.unshift(nextJob);
                    }

                    saveStoredJobs(jobs.slice(0, 40));
                }

                function clearStoredJobs() {
                    localStorage.removeItem(STORAGE_KEY);
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
                    const currentEmptyState = document.getElementById('empty-state');
                    if (currentEmptyState) {
                        currentEmptyState.style.display = 'none';
                    }
                }

                function attachCopyHandler(card, jobId) {
                    const copyBtn = card.querySelector('[data-role="copy"]');
                    if (!copyBtn) return;

                    copyBtn.addEventListener('click', async () => {
                        try {
                            await navigator.clipboard.writeText(jobId);
                            const originalText = copyBtn.textContent;
                            copyBtn.textContent = 'Copied';
                            setTimeout(() => {
                                copyBtn.textContent = originalText;
                            }, 1200);
                        } catch (error) {
                            setStatusMessage('Could not copy the Job ID. Please copy it manually.', true);
                        }
                    });
                }

                function createJobCard(job, fromStorage = false) {
                    ensureJobsVisible();

                    const jobId = String(job.job_id);
                    const card = document.createElement('article');
                    card.className = 'job-card';
                    card.dataset.jobId = jobId;
                    const delay = Math.min(jobsContainer.children.length * 0.04, 0.24);
                    card.style.animationDelay = `${delay}s`;
                    card.innerHTML = `
                        <div class="job-top">
                            <div class="badge queued" data-role="badge"><span class="dot"></span><span data-role="badge-text">Queued</span></div>
                            <button class="copy-btn" type="button" data-role="copy">Copy ID</button>
                        </div>
                        <h3 class="job-name" data-role="filename"></h3>
                        <p class="job-meta" data-role="meta"></p>
                        <p class="detail" data-role="detail"></p>
                        <div class="job-footer" data-role="updated"></div>
                    `;

                    card.querySelector('[data-role="filename"]').textContent = job.file_name || 'Untitled PDF';
                    card.querySelector('[data-role="meta"]').textContent = `Job ID: ${jobId}`;
                    updateCard(card, {
                        status: job.status || 'queued',
                        detail: job.detail || 'Waiting for the background worker to start.',
                        updated_at: job.updated_at || job.created_at
                    });

                    attachCopyHandler(card, jobId);
                    jobsContainer.prepend(card);

                    if (!fromStorage) {
                        upsertStoredJob(job);
                    }

                    return card;
                }

                function updateCard(card, statusPayload) {
                    const status = statusPayload.status || 'queued';
                    const detail = statusPayload.detail || '';
                    const badge = card.querySelector('[data-role="badge"]');
                    const badgeText = card.querySelector('[data-role="badge-text"]');
                    const detailNode = card.querySelector('[data-role="detail"]');
                    const updatedNode = card.querySelector('[data-role="updated"]');

                    badge.className = `badge ${statusClass(status)}`;
                    if (statusClass(status) === 'processing') {
                        badge.innerHTML = '<span class="spinner"></span><span data-role="badge-text">Processing</span>';
                    } else if (statusClass(status) === 'completed') {
                        badge.innerHTML = '<span class="dot"></span><span data-role="badge-text">Completed</span>';
                    } else {
                        badge.innerHTML = `<span class="dot"></span><span data-role="badge-text">${statusLabel(status)}</span>`;
                    }

                    if (badgeText) {
                        badgeText.textContent = statusLabel(status);
                    }

                    detailNode.textContent = detail || (statusClass(status) === 'completed'
                        ? 'Document indexed successfully.'
                        : statusClass(status) === 'failed'
                            ? 'The worker failed. Check the error message above.'
                            : 'Processing in progress...');

                    if (updatedNode) {
                        const timestamp = statusPayload.updated_at || new Date().toISOString();
                        updatedNode.textContent = `Updated ${formatTimestamp(timestamp)}`;
                    }

                    const jobId = card.dataset.jobId;
                    if (jobId) {
                        upsertStoredJob({
                            job_id: jobId,
                            status,
                            detail,
                            updated_at: statusPayload.updated_at || new Date().toISOString(),
                            file_name: card.querySelector('[data-role="filename"]').textContent
                        });
                    }
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

                function restoreJobs() {
                    const storedJobs = loadStoredJobs();
                    if (!storedJobs.length) {
                        return;
                    }

                    jobsContainer.innerHTML = '';
                    storedJobs.forEach((job) => {
                        const card = createJobCard(job, true);
                        const status = (job.status || '').toLowerCase();
                        if (status !== 'completed' && status !== 'failed') {
                            pollJob(job.job_id, card);
                        }
                    });

                    setStatusMessage('Restored recent jobs from your last session.');
                }

                function uploadWithProgress(formData, onProgress) {
                    return new Promise((resolve, reject) => {
                        const xhr = new XMLHttpRequest();
                        xhr.open('POST', '/upload');
                        xhr.responseType = 'json';

                        xhr.upload.addEventListener('progress', (event) => {
                            if (event.lengthComputable) {
                                const percent = (event.loaded / event.total) * 100;
                                onProgress(percent);
                            }
                        });

                        xhr.addEventListener('load', () => {
                            const response = xhr.response || {};
                            if (xhr.status >= 200 && xhr.status < 300) {
                                resolve(response);
                            } else {
                                const message = response.detail || `Upload failed (${xhr.status}).`;
                                reject(new Error(message));
                            }
                        });

                        xhr.addEventListener('error', () => {
                            reject(new Error('Network error. Please retry.'));
                        });

                        xhr.send(formData);
                    });
                }

                browseBtn.addEventListener('click', () => {
                    fileInput.click();
                });

                dropzone.addEventListener('click', (event) => {
                    if (event.target.closest('button')) {
                        return;
                    }
                    fileInput.click();
                });

                dropzone.addEventListener('dragover', (event) => {
                    event.preventDefault();
                    dropzone.classList.add('dragover');
                });

                dropzone.addEventListener('dragleave', () => {
                    dropzone.classList.remove('dragover');
                });

                dropzone.addEventListener('drop', (event) => {
                    event.preventDefault();
                    dropzone.classList.remove('dragover');
                    addFiles(event.dataTransfer.files);
                });

                fileInput.addEventListener('change', () => {
                    addFiles(fileInput.files);
                });

                fileList.addEventListener('click', (event) => {
                    const target = event.target;
                    if (!target.classList.contains('file-remove')) {
                        return;
                    }

                    const key = target.getAttribute('data-key');
                    selectedFiles.delete(key);
                    syncFileInput();
                    renderFileList();
                });

                clearBtn.addEventListener('click', () => {
                    pollingTimers.forEach((timer) => clearInterval(timer));
                    pollingTimers.clear();
                    jobsContainer.innerHTML = '<div class="empty-state" id="empty-state">Uploaded jobs will appear here after you submit files.</div>';
                    setStatusMessage('');
                    clearStoredJobs();
                });

                form.addEventListener('submit', async (event) => {
                    event.preventDefault();

                    if (!selectedFiles.size) {
                        setStatusMessage('Please choose at least one PDF file.', true);
                        return;
                    }

                    setUploadState(true);
                    setProgress(2, true);
                    setStatusMessage('Uploading files to the ingestion pipeline...');

                    try {
                        const formData = new FormData();
                        Array.from(selectedFiles.values()).forEach((file) => {
                            formData.append('files', file);
                        });

                        const payload = await uploadWithProgress(formData, (percent) => {
                            setProgress(percent, true);
                        });

                        jobsContainer.innerHTML = '';
                        const jobs = Array.isArray(payload) ? payload : [payload];

                        jobs.forEach((job) => {
                            const card = createJobCard(job);
                            pollJob(job.job_id, card);
                        });

                        setProgress(100, true);
                        setTimeout(() => setProgress(0, false), 700);
                        clearSelection();
                        setStatusMessage(`Submitted ${jobs.length} job${jobs.length === 1 ? '' : 's'}. Tracking status automatically.`);
                    } catch (error) {
                        setStatusMessage(error.message, true);
                        setProgress(0, false);
                    } finally {
                        setUploadState(false);
                    }
                });

                renderFileList();
                restoreJobs();
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
            limit_mb = MultiPartParser.max_file_size // (1024 * 1024)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only PDF files up to {limit_mb} MB are supported."
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


