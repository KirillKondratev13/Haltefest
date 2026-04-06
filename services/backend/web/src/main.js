import 'htmx.org'
import { updateUserGraph } from './graph'

const FILES_DATA_URL = '/profile/files/data'
const POLL_INTERVAL_MS = 5000
const ANALYSIS_POLL_INTERVAL_MS = 2500

let refreshInFlight = false
let pollingId = null
let lastFilesData = null // Кэшируем последние данные
let lastUsername = null
let analysisPollingId = null
let analysisPollInFlight = false

const analysisState = {
	selectedFileId: null,
	selectedFileName: '',
	selectedAnalysisType: '',
	activeJobId: null,
	activeJobStatus: '',
	resultText: '',
	resultError: '',
	updatedAt: '',
	connectionIssue: '',
}

function analysisStartUrl(fileId) {
	return `/api/files/${encodeURIComponent(fileId)}/analysis`
}

function analysisJobUrl(jobId) {
	return `/api/analysis-jobs/${encodeURIComponent(jobId)}`
}

function analysisTypeLabel(analysisType) {
	switch ((analysisType || '').toLowerCase()) {
		case 'summary':
			return 'Summary'
		case 'chapters':
			return 'Chapters'
		case 'flashcards':
			return 'Flashcards'
		default:
			return analysisType || '-'
	}
}

function formatTimestamp(dateValue = new Date()) {
	return dateValue.toLocaleString()
}

function escapeHtml(value) {
	if (value === null || value === undefined) return ''
	return String(value)
		.replaceAll('&', '&amp;')
		.replaceAll('<', '&lt;')
		.replaceAll('>', '&gt;')
		.replaceAll('"', '&quot;')
		.replaceAll("'", '&#39;')
}

function formatBytes(bytes) {
	if (!bytes) return '0 B'
	const k = 1024
	const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
	const i = Math.floor(Math.log(bytes) / Math.log(k))
	return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i]
}

function normalizeFileData(payload) {
	const files = payload?.files || payload?.Files || []
	const username = payload?.username || payload?.Username || 'User'
	return { files, username }
}

function getFileSignature(file) {
	// Включаем поля, которые влияют на таблицу/граф,
	// чтобы polling корректно ловил статусные обновления.
	return [
		file.FileName || '',
		file.CreatedAt || '',
		file.FileSize || '',
		file.FileType || '',
		file.Status || '',
		file.Tag || '',
		file.FailureCause || '',
		file.DownloadURL || '',
		file.DeleteURL || '',
	].join('|')
}

function filesHaveChanged(oldFiles, newFiles) {
	if (!oldFiles || !newFiles) return true
	if (oldFiles.length !== newFiles.length) return true

	const oldSignatures = new Set(oldFiles.map(getFileSignature))
	const newSignatures = new Set(newFiles.map(getFileSignature))

	if (oldSignatures.size !== newSignatures.size) return true

	for (const sig of newSignatures) {
		if (!oldSignatures.has(sig)) return true
	}

	return false
}

function getFilesSection() {
	const profileRoot = document.querySelector('main .max-w-md')
	if (!profileRoot) return null
	return profileRoot.querySelector('.mt-10')
}

function ensureFilesTableContainer() {
	const filesSection = getFilesSection()
	if (!filesSection) return null

	let container = filesSection.querySelector('#files-table-container')
	if (!container) {
		container = document.createElement('div')
		container.id = 'files-table-container'
		filesSection.appendChild(container)
	}

	const legacyList = filesSection.querySelector('ul')
	if (legacyList) {
		legacyList.remove()
	}

	return container
}

function statusBadge(status) {
	const value = (status || '').toUpperCase()
	switch (value) {
		case 'READY':
			return '<span class="badge badge-success badge-sm">READY</span>'
		case 'PROCESSING':
			return '<span class="badge badge-warning badge-sm">PROCESSING</span>'
		case 'ERROR':
			return '<span class="badge badge-error badge-sm">ERROR</span>'
		case 'PENDING':
			return '<span class="badge badge-info badge-sm">PENDING</span>'
		default:
			return '<span class="badge badge-ghost badge-sm">N/A</span>'
	}
}

function renderFilesTable(files) {
	const container = ensureFilesTableContainer()
	if (!container) return

	if (!files.length) {
		container.innerHTML =
			'<p class="text-sm text-gray-500">No files uploaded yet.</p>'
		return
	}

	const rows = files
		.map(file => {
			const fileName = escapeHtml(file.FileName)
			const fileType = escapeHtml(file.FileType || '')
			const createdAt = escapeHtml(file.CreatedAt || '')
			const tag = escapeHtml(file.Tag || '-')
			const failureCause = escapeHtml(file.FailureCause || '-')
			const size = formatBytes(file.FileSize)
			const downloadUrl = escapeHtml(file.DownloadURL || '#')
			const deleteUrl = escapeHtml(file.DeleteURL || '#')

			return `
                <tr class="hover">
                    <td class="max-w-[240px] truncate" title="${fileName}">${fileName}</td>
                    <td class="max-w-[220px] truncate" title="${fileType}">${fileType}</td>
                    <td>${size}</td>
                    <td>${createdAt}</td>
                    <td>${statusBadge(file.Status)}</td>
                    <td class="max-w-[160px] truncate" title="${tag}">${tag}</td>
                    <td class="max-w-[280px] truncate" title="${failureCause}">${failureCause}</td>
                    <td class="whitespace-nowrap">
                        <a class="link link-primary mr-2" href="${downloadUrl}">download</a>
                        <button class="link link-error" type="button" data-file-delete-url="${deleteUrl}">delete</button>
                    </td>
                </tr>
            `
		})
		.join('')

	container.innerHTML = `
        <div class="overflow-x-auto border rounded-md">
            <table class="table table-zebra table-sm">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Type</th>
                        <th>Size</th>
                        <th>Created</th>
                        <th>Status</th>
                        <th>Tag</th>
                        <th>Failure Cause</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    `
}

function ensureAnalysisPanelRoot() {
	let root = document.getElementById('analysis-panel-root')
	if (root) return root

	const graphContainer = document.getElementById('graph-container')
	if (!graphContainer) return null

	root = document.createElement('section')
	root.id = 'analysis-panel-root'
	root.className = 'w-full max-w-6xl mx-auto mt-4 px-4'

	const wrapper = graphContainer.parentElement
	if (wrapper && wrapper.parentElement) {
		wrapper.parentElement.insertBefore(root, wrapper.nextSibling)
	} else {
		graphContainer.insertAdjacentElement('afterend', root)
	}

	return root
}

function renderAnalysisPanel() {
	const root = ensureAnalysisPanelRoot()
	if (!root) return

	const previousPre = root.querySelector('[data-analysis-result-pre]')
	const previousScrollTop = previousPre ? previousPre.scrollTop : 0

	const hasSelection =
		analysisState.selectedFileId !== null &&
		analysisState.selectedAnalysisType !== ''

	if (!hasSelection) {
		const nextHtml = `
            <div class="border border-base-300 rounded-lg bg-base-100 text-base-content shadow-md p-4">
                <h3 class="text-lg font-semibold mb-2">Analysis Result</h3>
                <p class="text-sm opacity-70">Выберите файл и тип анализа.</p>
            </div>
        `
		if (root.dataset.analysisRenderHtml === nextHtml) return
		root.innerHTML = nextHtml
		root.dataset.analysisRenderHtml = nextHtml
		return
	}

	const statusValue = (analysisState.activeJobStatus || '').toUpperCase()
	const updatedAt = escapeHtml(analysisState.updatedAt || '-')
	const fileName = escapeHtml(analysisState.selectedFileName || 'File')
	const analysisLabel = escapeHtml(
		analysisTypeLabel(analysisState.selectedAnalysisType)
	)
	const resultText = escapeHtml(analysisState.resultText || '')
	const resultError = escapeHtml(analysisState.resultError || '')
	const connectionIssue = escapeHtml(analysisState.connectionIssue || '')

	let contentHtml = '<p class="text-sm opacity-80">Ожидание запуска анализа...</p>'

	if (statusValue === 'QUEUED' || statusValue === 'PROCESSING') {
		contentHtml = `
            <div class="flex items-center gap-2 text-sm opacity-80">
                <span class="loading loading-spinner loading-sm"></span>
                <span>${statusValue === 'QUEUED' ? 'Задача в очереди...' : 'Анализ выполняется...'}</span>
            </div>
        `
	} else if (statusValue === 'DONE') {
		contentHtml = `
            <pre data-analysis-result-pre class="whitespace-pre-wrap text-sm leading-6 bg-base-200 text-base-content border border-base-300 rounded p-3 max-h-[360px] overflow-y-auto">${resultText || '-'}</pre>
        `
	} else if (statusValue === 'FAILED') {
		contentHtml = `
            <div class="space-y-2">
                <p class="text-sm text-red-600">${resultError || 'Анализ завершился с ошибкой.'}</p>
                <button class="btn btn-sm btn-error" type="button" data-analysis-retry>Повторить</button>
            </div>
        `
	}

	const nextHtml = `
        <div class="border border-base-300 rounded-lg bg-base-100 text-base-content shadow-md p-4">
            <div class="flex flex-wrap items-center gap-3 mb-3">
                <h3 class="text-lg font-semibold mr-auto">Analysis Result</h3>
                <span class="text-sm opacity-70">File: <b>${fileName}</b></span>
                <span class="text-sm opacity-70">Type: <b>${analysisLabel}</b></span>
                <span>${statusBadge(statusValue || 'N/A')}</span>
                <span class="text-xs opacity-60">Updated: ${updatedAt}</span>
            </div>
            ${connectionIssue ? `<p class="text-sm text-warning mb-2">${connectionIssue}</p>` : ''}
            ${contentHtml}
        </div>
    `

	if (root.dataset.analysisRenderHtml === nextHtml) return

	root.innerHTML = nextHtml
	root.dataset.analysisRenderHtml = nextHtml

	const nextPre = root.querySelector('[data-analysis-result-pre]')
	if (nextPre && previousPre) {
		const maxScrollTop = Math.max(0, nextPre.scrollHeight - nextPre.clientHeight)
		nextPre.scrollTop = Math.min(previousScrollTop, maxScrollTop)
	}
}

function stopAnalysisPolling() {
	if (analysisPollingId !== null) {
		window.clearInterval(analysisPollingId)
		analysisPollingId = null
	}
}

function normalizeApiError(message, fallback = 'Request failed') {
	const text = (message || '').trim()
	return text || fallback
}

function extractResultText(result) {
	if (!result) return ''
	if (typeof result === 'string') return result
	if (typeof result.result_text === 'string') return result.result_text
	return JSON.stringify(result, null, 2)
}

async function pollAnalysisJobOnce(jobId) {
	if (!jobId || analysisPollInFlight) return
	analysisPollInFlight = true

	try {
		const response = await fetch(analysisJobUrl(jobId), {
			headers: { Accept: 'application/json' },
		})
		if (!response.ok) {
			const body = await response.text()
			throw new Error(normalizeApiError(body, response.statusText))
		}

		const payload = await response.json()
		analysisState.activeJobStatus = String(payload.status || '').toUpperCase()
		analysisState.resultError = payload.error || ''
		analysisState.connectionIssue = ''
		analysisState.updatedAt = formatTimestamp()

		if (analysisState.activeJobStatus === 'DONE') {
			analysisState.resultText = extractResultText(payload.result)
			stopAnalysisPolling()
		} else if (analysisState.activeJobStatus === 'FAILED') {
			stopAnalysisPolling()
		}

		renderAnalysisPanel()
	} catch (err) {
		analysisState.connectionIssue = 'Проблема соединения, повторяем...'
		renderAnalysisPanel()
		console.error('Failed to poll analysis job', err)
	} finally {
		analysisPollInFlight = false
	}
}

function startAnalysisPolling(jobId) {
	stopAnalysisPolling()
	if (!jobId) return

	void pollAnalysisJobOnce(jobId)
	analysisPollingId = window.setInterval(() => {
		void pollAnalysisJobOnce(jobId)
	}, ANALYSIS_POLL_INTERVAL_MS)
}

async function startAnalysisRequest(fileId, fileName, analysisType) {
	if (!fileId || !analysisType) return

	analysisState.selectedFileId = fileId
	analysisState.selectedFileName = fileName || `file_${fileId}`
	analysisState.selectedAnalysisType = analysisType
	analysisState.resultText = ''
	analysisState.resultError = ''
	analysisState.connectionIssue = ''
	analysisState.activeJobStatus = 'QUEUED'
	analysisState.updatedAt = formatTimestamp()
	renderAnalysisPanel()

	const panelRoot = ensureAnalysisPanelRoot()
	if (panelRoot) {
		panelRoot.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
	}

	try {
		const response = await fetch(analysisStartUrl(fileId), {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				Accept: 'application/json',
			},
			body: JSON.stringify({
				analysis_type: analysisType,
				params: analysisType === 'flashcards' ? { max_cards: 10 } : {},
			}),
		})

		if (!response.ok) {
			const body = await response.text()
			throw new Error(normalizeApiError(body, response.statusText))
		}

		const payload = await response.json()
		analysisState.activeJobId = payload.job_id || null
		analysisState.activeJobStatus = String(payload.status || 'QUEUED').toUpperCase()
		analysisState.updatedAt = formatTimestamp()
		analysisState.connectionIssue = ''
		renderAnalysisPanel()

		if (analysisState.activeJobStatus === 'DONE') {
			await pollAnalysisJobOnce(analysisState.activeJobId)
			return
		}

		startAnalysisPolling(analysisState.activeJobId)
	} catch (err) {
		stopAnalysisPolling()
		analysisState.activeJobStatus = 'FAILED'
		analysisState.resultError = normalizeApiError(err?.message, 'Ошибка запуска анализа')
		analysisState.updatedAt = formatTimestamp()
		renderAnalysisPanel()
	}
}

function readInitialFileData() {
	const fileDataEl = document.getElementById('file-data')
	if (!fileDataEl) return null

	try {
		return normalizeFileData(JSON.parse(fileDataEl.textContent))
	} catch {
		return null
	}
}

function renderProfileData(payload, forceUpdate = false) {
	const { files, username } = normalizeFileData(payload)

	// Проверяем, изменились ли данные
	const hasFilesChanged = filesHaveChanged(lastFilesData, files)
	const hasUsernameChanged = lastUsername !== username

	// Обновляем таблицу только если изменились файлы
	if (hasFilesChanged || forceUpdate) {
		renderFilesTable(files)
	}

	// Обновляем граф только если есть изменения
	if (hasFilesChanged || hasUsernameChanged || forceUpdate) {
		updateUserGraph(username, files)
	}

	if (hasFilesChanged || hasUsernameChanged || forceUpdate) {
		lastFilesData = files
		lastUsername = username
	}

	if (analysisState.selectedFileId !== null) {
		const selectedFile = files.find(
			file => Number(file.ID) === Number(analysisState.selectedFileId)
		)
		let shouldRenderAnalysisPanel = false

		if (!selectedFile) {
			stopAnalysisPolling()
			analysisState.selectedFileId = null
			analysisState.selectedFileName = ''
			analysisState.selectedAnalysisType = ''
			analysisState.activeJobId = null
			analysisState.activeJobStatus = ''
			analysisState.resultText = ''
			analysisState.resultError = ''
			analysisState.connectionIssue = ''
			analysisState.updatedAt = ''
			shouldRenderAnalysisPanel = true
		} else {
			const nextFileName =
				selectedFile.FileName || analysisState.selectedFileName
			if (nextFileName !== analysisState.selectedFileName) {
				analysisState.selectedFileName = nextFileName
				shouldRenderAnalysisPanel = true
			}
		}

		if (shouldRenderAnalysisPanel) {
			renderAnalysisPanel()
		}
	}
}

async function refreshFilesData(forceUpdate = false) {
	if (refreshInFlight) return
	refreshInFlight = true

	try {
		const response = await fetch(FILES_DATA_URL, {
			headers: { Accept: 'application/json' },
		})
		if (!response.ok) {
			throw new Error(response.statusText)
		}
		const payload = await response.json()
		renderProfileData(payload, forceUpdate)
	} catch (err) {
		console.error('Failed to refresh files data', err)
	} finally {
		refreshInFlight = false
	}
}

function setupFilesTableActions() {
	document.addEventListener('click', async event => {
		const button = event.target.closest('[data-file-delete-url]')
		if (!button) return

		event.preventDefault()
		const deleteUrl = button.getAttribute('data-file-delete-url')
		if (!deleteUrl) return
		if (!confirm('Delete this file?')) return

		try {
			const response = await fetch(deleteUrl, { method: 'POST' })
			if (!response.ok) throw new Error(response.statusText)
			// Принудительное обновление после удаления
			await refreshFilesData(true)
		} catch (err) {
			alert('Failed to delete file: ' + err.message)
		}
	})
}

function setupUploadForm() {
	const form = document.getElementById('uploadForm')
	const fileInput = document.getElementById('file')
	const progressBar = document.getElementById('uploadProgress')
	const status = document.getElementById('uploadStatus')

	if (!form || !fileInput || !progressBar || !status) return

	form.addEventListener('submit', function (event) {
		event.preventDefault()

		const file = fileInput.files[0]
		if (!file) return

		const formData = new FormData()
		formData.append('file', file)

		const xhr = new XMLHttpRequest()
		xhr.open('POST', '/profile/upload', true)

		xhr.upload.onprogress = function (progressEvent) {
			if (!progressEvent.lengthComputable) return
			const percent = (progressEvent.loaded / progressEvent.total) * 100
			progressBar.style.width = percent + '%'
			status.textContent = `Uploading: ${percent.toFixed(1)}%`
		}

		xhr.onload = async function () {
			if (xhr.status === 200 || xhr.status === 303) {
				status.textContent = 'Upload complete. File is being processed...'
				progressBar.style.width = '100%'
				fileInput.value = ''
				// Принудительное обновление после загрузки
				await refreshFilesData(true)
			} else {
				status.textContent = 'Error: ' + xhr.responseText
			}
		}

		xhr.onerror = function () {
			status.textContent = 'Network error!'
		}

		xhr.send(formData)
	})
}

function startPolling() {
	if (pollingId !== null) {
		window.clearInterval(pollingId)
	}
	pollingId = window.setInterval(() => {
		void refreshFilesData(false) // Без принудительного обновления
	}, POLL_INTERVAL_MS)
}

function setupAnalysisActions() {
	window.addEventListener('graph:analysis-requested', event => {
		const detail = event?.detail || {}
		void startAnalysisRequest(
			Number(detail.fileId || 0),
			String(detail.fileName || ''),
			String(detail.analysisType || '')
		)
	})

	document.addEventListener('click', event => {
		const retryButton = event.target.closest('[data-analysis-retry]')
		if (!retryButton) return

		if (
			analysisState.selectedFileId === null ||
			!analysisState.selectedAnalysisType
		) {
			return
		}

		void startAnalysisRequest(
			analysisState.selectedFileId,
			analysisState.selectedFileName,
			analysisState.selectedAnalysisType
		)
	})
}

document.addEventListener('DOMContentLoaded', function () {
	const initialFileData = readInitialFileData()
	if (initialFileData) {
		// Первое рендерирование всегда принудительное
		renderProfileData(initialFileData, true)
		startPolling()
	}

	renderAnalysisPanel()
	setupUploadForm()
	setupFilesTableActions()
	setupAnalysisActions()
})

window.addEventListener('graph:file-deleted', () => {
	void refreshFilesData(true)
})
