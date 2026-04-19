import 'htmx.org'
import { updateUserGraph } from './graph'

const FILES_DATA_URL = '/profile/files/data'
const PREFERENCES_API_URL = '/api/preferences'
const POLL_INTERVAL_MS = 5000
const ANALYSIS_POLL_INTERVAL_MS = 2500
const CHAT_POLL_INTERVAL_MS = 2500

const CHAT_SCOPE_SINGLE = 'single-doc'
const CHAT_SCOPE_MULTI = 'multi-doc'
const CHAT_SCOPE_ALL = 'all-docs'

const CHAT_PROVIDER_LOCAL = 'local'
const CHAT_PROVIDER_GIGACHAT = 'gigachat'
const PREFERENCES_PAGE_PATH = '/preferences'

let refreshInFlight = false
let pollingId = null
let lastFilesData = null // Кэшируем последние данные
let lastUsername = null
let analysisPollingId = null
let analysisPollInFlight = false
let chatPollingId = null
let chatPollInFlight = false

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

const chatState = {
	panelVisible: false,
	threads: [],
	threadsLoaded: false,
	threadsLoading: false,
	threadsError: '',
	creatingThread: false,
	deletingThreadId: null,

	activeThreadId: null,
	activeThreadScope: '',
	activeThreadProvider: CHAT_PROVIDER_LOCAL,
	activeThreadTitle: '',
	activeThreadSelectedFileIds: [],

	messages: [],
	messagesLoading: false,
	messagesError: '',
	messageDraft: '',

	sendingMessage: false,
	pendingJobId: null,
	pendingJobStatus: '',
	pendingJobError: '',
	connectionIssue: '',
	lastUpdatedAt: '',

	draftScope: CHAT_SCOPE_SINGLE,
	draftProvider: CHAT_PROVIDER_LOCAL,
	draftSelectedFileIds: [],
	uiError: '',
}

const preferencesState = {
	loaded: false,
	loading: false,
	saving: false,
	error: '',
	success: '',
	summaryProvider: CHAT_PROVIDER_LOCAL,
	chaptersProvider: CHAT_PROVIDER_LOCAL,
	flashcardsProvider: CHAT_PROVIDER_LOCAL,
	chatDefaultProvider: CHAT_PROVIDER_LOCAL,
}

function analysisStartUrl(fileId) {
	return `/api/files/${encodeURIComponent(fileId)}/analysis`
}

function analysisJobUrl(jobId) {
	return `/api/analysis-jobs/${encodeURIComponent(jobId)}`
}

function chatThreadsUrl() {
	return '/api/chat/threads'
}

function chatThreadMessagesUrl(threadId) {
	return `/api/chat/threads/${encodeURIComponent(threadId)}/messages`
}

function chatJobUrl(jobId) {
	return `/api/chat/jobs/${encodeURIComponent(jobId)}`
}

function preferencesApiUrl() {
	return PREFERENCES_API_URL
}

function isPreferencesPage() {
	const normalized = String(window.location.pathname || '').replace(/\/+$/, '')
	return normalized === PREFERENCES_PAGE_PATH
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

function chatScopeLabel(scopeMode) {
	switch ((scopeMode || '').toLowerCase()) {
		case CHAT_SCOPE_SINGLE:
			return 'Single'
		case CHAT_SCOPE_MULTI:
			return 'Multi'
		case CHAT_SCOPE_ALL:
			return 'All'
		default:
			return scopeMode || '-'
	}
}

function chatProviderLabel(provider) {
	switch ((provider || '').toLowerCase()) {
		case CHAT_PROVIDER_LOCAL:
			return 'Local'
		case CHAT_PROVIDER_GIGACHAT:
			return 'GigaChat'
		default:
			return provider || '-'
	}
}

function formatTimestamp(dateValue = new Date()) {
	return dateValue.toLocaleString()
}

function formatApiTimestamp(value) {
	if (!value) return '-'
	const dateValue = new Date(value)
	if (Number.isNaN(dateValue.getTime())) {
		return String(value)
	}
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

function normalizeFileId(value) {
	const parsed = Number(value)
	if (!Number.isFinite(parsed) || parsed <= 0) return null
	return parsed
}

function getReadyFiles() {
	const files = Array.isArray(lastFilesData) ? lastFilesData : []
	return files.filter(file => String(file.Status || '').toUpperCase() === 'READY')
}

function getFileNameById(fileId) {
	const normalizedId = normalizeFileId(fileId)
	if (!normalizedId) return `file_${fileId}`
	const files = Array.isArray(lastFilesData) ? lastFilesData : []
	const found = files.find(file => Number(file.ID) === normalizedId)
	return found?.FileName || `file_${normalizedId}`
}

function sanitizeSelectedFileIds(values) {
	if (!Array.isArray(values) || values.length === 0) return []
	const seen = new Set()
	const result = []
	for (const value of values) {
		const normalized = normalizeFileId(value)
		if (!normalized || seen.has(normalized)) continue
		seen.add(normalized)
		result.push(normalized)
	}
	return result
}

function sortNumericAsc(values) {
	return [...values].sort((a, b) => a - b)
}

function isSameSelectedFiles(first, second) {
	const firstNormalized = sortNumericAsc(sanitizeSelectedFileIds(first))
	const secondNormalized = sortNumericAsc(sanitizeSelectedFileIds(second))
	if (firstNormalized.length !== secondNormalized.length) return false
	for (let i = 0; i < firstNormalized.length; i += 1) {
		if (firstNormalized[i] !== secondNormalized[i]) return false
	}
	return true
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

async function readApiError(response, fallback = 'Request failed') {
	const body = await response.text().catch(() => '')
	return normalizeApiError(body, fallback || response.statusText)
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

function ensurePreferencesPanelRoot() {
	let root = document.getElementById('preferences-panel-root')
	if (root) return root

	const placeholder = document.getElementById('preferences-page-root')
	if (placeholder) {
		placeholder.id = 'preferences-panel-root'
		placeholder.className = 'w-full max-w-4xl mx-auto mt-4 px-4'
		return placeholder
	}

	const main = document.querySelector('main')
	if (!main) return null

	root = document.createElement('section')
	root.id = 'preferences-panel-root'
	root.className = 'w-full max-w-4xl mx-auto mt-4 px-4'

	const profileCard = main.querySelector('.max-w-md')
	if (profileCard && profileCard.parentElement === main) {
		main.insertBefore(root, profileCard)
	} else {
		main.prepend(root)
	}

	return root
}

function renderPreferencesPanel() {
	const root = document.getElementById('preferences-panel-root')
	if (!root) return

	const providerSelect = (dataKey, value) => `
        <select class="select select-sm select-bordered w-full" data-pref-provider="${dataKey}">
            <option value="${CHAT_PROVIDER_LOCAL}" ${value === CHAT_PROVIDER_LOCAL ? 'selected' : ''}>Local</option>
            <option value="${CHAT_PROVIDER_GIGACHAT}" ${value === CHAT_PROVIDER_GIGACHAT ? 'selected' : ''}>GigaChat</option>
        </select>
    `

	const nextHtml = `
        <div class="border border-base-300 rounded-lg bg-base-100 text-base-content shadow-md p-4">
            <div class="flex items-center gap-2 mb-3">
                <h2 class="text-lg font-semibold mr-auto">Preferences</h2>
                ${preferencesState.loading || preferencesState.saving ? '<span class="loading loading-spinner loading-sm"></span>' : ''}
            </div>
            <p class="text-sm opacity-75 mb-4">Выбор LLM-провайдера для каждого режима анализа и дефолта в новом чате.</p>

            ${preferencesState.error ? `<p class="text-sm text-error mb-3">${escapeHtml(preferencesState.error)}</p>` : ''}
            ${preferencesState.success ? `<p class="text-sm text-success mb-3">${escapeHtml(preferencesState.success)}</p>` : ''}

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <label class="form-control">
                    <span class="label-text text-xs mb-1">Summary provider</span>
                    ${providerSelect('summary', preferencesState.summaryProvider)}
                </label>
                <label class="form-control">
                    <span class="label-text text-xs mb-1">Chapters provider</span>
                    ${providerSelect('chapters', preferencesState.chaptersProvider)}
                </label>
                <label class="form-control">
                    <span class="label-text text-xs mb-1">Flashcards provider</span>
                    ${providerSelect('flashcards', preferencesState.flashcardsProvider)}
                </label>
                <label class="form-control">
                    <span class="label-text text-xs mb-1">Default chat provider</span>
                    ${providerSelect('chat_default', preferencesState.chatDefaultProvider)}
                </label>
            </div>

            <div class="mt-4">
                <button
                    class="btn btn-primary btn-sm"
                    type="button"
                    data-pref-save
                    ${preferencesState.saving ? 'disabled' : ''}
                >
                    ${preferencesState.saving ? 'Saving...' : 'Save Preferences'}
                </button>
            </div>
        </div>
    `

	root.innerHTML = nextHtml
}

async function loadPreferences(force = false) {
	if (preferencesState.loading) return
	if (preferencesState.loaded && !force) return

	preferencesState.loading = true
	preferencesState.error = ''
	preferencesState.success = ''
	if (isPreferencesPage()) renderPreferencesPanel()

	try {
		const response = await fetch(preferencesApiUrl(), {
			headers: { Accept: 'application/json' },
		})
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка загрузки preferences'))
		}

		const payload = await response.json()
		applyPreferences(payload)
		applyPreferencesToDraftProvider()
		renderChatPanel()
	} catch (err) {
		preferencesState.error = normalizeApiError(
			err?.message,
			'Ошибка загрузки preferences'
		)
	} finally {
		preferencesState.loading = false
		if (isPreferencesPage()) renderPreferencesPanel()
	}
}

async function savePreferences() {
	if (preferencesState.saving) return

	preferencesState.saving = true
	preferencesState.error = ''
	preferencesState.success = ''
	renderPreferencesPanel()

	const payload = {
		summary_provider: normalizeChatProviderValue(preferencesState.summaryProvider),
		chapters_provider: normalizeChatProviderValue(
			preferencesState.chaptersProvider
		),
		flashcards_provider: normalizeChatProviderValue(
			preferencesState.flashcardsProvider
		),
		chat_default_provider: normalizeChatProviderValue(
			preferencesState.chatDefaultProvider
		),
	}

	try {
		const response = await fetch(preferencesApiUrl(), {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				Accept: 'application/json',
			},
			body: JSON.stringify(payload),
		})
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка сохранения preferences'))
		}

		const saved = await response.json()
		applyPreferences(saved)
		applyPreferencesToDraftProvider()
		chatState.uiError = ''
		preferencesState.success = 'Preferences saved'
		renderChatPanel()
	} catch (err) {
		preferencesState.error = normalizeApiError(
			err?.message,
			'Ошибка сохранения preferences'
		)
	} finally {
		preferencesState.saving = false
		renderPreferencesPanel()
	}
}

function normalizeChatScopeValue(value) {
	const normalized = String(value || '').trim().toLowerCase()
	if (
		normalized === CHAT_SCOPE_SINGLE ||
		normalized === CHAT_SCOPE_MULTI ||
		normalized === CHAT_SCOPE_ALL
	) {
		return normalized
	}
	return CHAT_SCOPE_SINGLE
}

function normalizeChatProviderValue(value) {
	const normalized = String(value || '').trim().toLowerCase()
	if (normalized === CHAT_PROVIDER_GIGACHAT) return CHAT_PROVIDER_GIGACHAT
	return CHAT_PROVIDER_LOCAL
}

function normalizePreferencesPayload(raw) {
	return {
		summaryProvider: normalizeChatProviderValue(raw?.summary_provider),
		chaptersProvider: normalizeChatProviderValue(raw?.chapters_provider),
		flashcardsProvider: normalizeChatProviderValue(raw?.flashcards_provider),
		chatDefaultProvider: normalizeChatProviderValue(raw?.chat_default_provider),
	}
}

function applyPreferences(prefs) {
	const normalized = normalizePreferencesPayload(prefs || {})
	preferencesState.summaryProvider = normalized.summaryProvider
	preferencesState.chaptersProvider = normalized.chaptersProvider
	preferencesState.flashcardsProvider = normalized.flashcardsProvider
	preferencesState.chatDefaultProvider = normalized.chatDefaultProvider
	preferencesState.loaded = true
}

function applyPreferencesToDraftProvider() {
	const preferred = normalizeChatProviderValue(preferencesState.chatDefaultProvider)
	if (!chatState.activeThreadId) {
		chatState.draftProvider = preferred
	}
}

function normalizeChatThread(raw) {
	return {
		thread_id: Number(raw?.thread_id || 0),
		title: String(raw?.title || ''),
		scope: normalizeChatScopeValue(raw?.scope),
		provider: normalizeChatProviderValue(raw?.provider),
		selected_file_ids: sanitizeSelectedFileIds(raw?.selected_file_ids || []),
		created_at: raw?.created_at || '',
		updated_at: raw?.updated_at || '',
	}
}

function normalizeChatMessage(raw) {
	const role = String(raw?.role || '').toLowerCase()
	return {
		message_id: Number(raw?.message_id || 0),
		role: role === 'assistant' ? 'assistant' : 'user',
		content: String(raw?.content || ''),
		created_at: raw?.created_at || '',
	}
}

function ensureChatPanelRoot() {
	let root = document.getElementById('chat-panel-root')
	if (root) return root

	root = document.createElement('section')
	root.id = 'chat-panel-root'
	root.className = 'w-full max-w-6xl mx-auto mt-4 px-4 pb-8'

	const analysisRoot = document.getElementById('analysis-panel-root')
	if (analysisRoot?.parentElement) {
		analysisRoot.insertAdjacentElement('afterend', root)
		return root
	}

	const graphContainer = document.getElementById('graph-container')
	if (!graphContainer) return null

	const wrapper = graphContainer.parentElement
	if (wrapper && wrapper.parentElement) {
		wrapper.parentElement.insertBefore(root, wrapper.nextSibling)
	} else {
		graphContainer.insertAdjacentElement('afterend', root)
	}

	return root
}

function getChatThreadById(threadId) {
	const normalizedId = Number(threadId || 0)
	if (!normalizedId) return null
	return (
		chatState.threads.find(thread => Number(thread.thread_id) === normalizedId) || null
	)
}

function getThreadTitle(thread) {
	const title = String(thread?.title || '').trim()
	if (title) return title

	if (
		thread?.scope === CHAT_SCOPE_SINGLE &&
		Array.isArray(thread?.selected_file_ids) &&
		thread.selected_file_ids.length === 1
	) {
		return `Chat: ${getFileNameById(thread.selected_file_ids[0])}`
	}
	if (thread?.scope === CHAT_SCOPE_MULTI) {
		const count = sanitizeSelectedFileIds(thread.selected_file_ids).length
		return count > 0 ? `Multi-doc (${count})` : 'Multi-doc'
	}
	if (thread?.scope === CHAT_SCOPE_ALL) {
		return 'All docs'
	}

	return `Thread #${thread?.thread_id || '-'}`
}

function getThreadSelectionSummary(thread) {
	if (!thread) return '-'
	if (thread.scope === CHAT_SCOPE_ALL) return 'Все документы'
	if (thread.scope === CHAT_SCOPE_SINGLE) {
		const selected = sanitizeSelectedFileIds(thread.selected_file_ids)
		if (!selected.length) return 'Документ не выбран'
		return getFileNameById(selected[0])
	}

	const names = sanitizeSelectedFileIds(thread.selected_file_ids).map(getFileNameById)
	if (!names.length) return 'Документы не выбраны'
	if (names.length <= 3) return names.join(', ')
	return `${names.slice(0, 3).join(', ')} +${names.length - 3}`
}

function setActiveThreadMeta(thread) {
	if (!thread) {
		chatState.activeThreadId = null
		chatState.activeThreadScope = ''
		chatState.activeThreadProvider = CHAT_PROVIDER_LOCAL
		chatState.activeThreadTitle = ''
		chatState.activeThreadSelectedFileIds = []
		return
	}

	chatState.activeThreadId = Number(thread.thread_id || 0) || null
	chatState.activeThreadScope = normalizeChatScopeValue(thread.scope)
	chatState.activeThreadProvider = normalizeChatProviderValue(thread.provider)
	chatState.activeThreadTitle = String(thread.title || '')
	chatState.activeThreadSelectedFileIds = sanitizeSelectedFileIds(
		thread.selected_file_ids || []
	)
}

function clearChatPendingState() {
	stopChatPolling()
	chatState.pendingJobId = null
	chatState.pendingJobStatus = ''
	chatState.pendingJobError = ''
	chatState.connectionIssue = ''
	chatState.lastUpdatedAt = ''
}

function resetActiveChatMessages() {
	chatState.messages = []
	chatState.messagesLoading = false
	chatState.messagesError = ''
	chatState.messageDraft = ''
	clearChatPendingState()
}

function reconcileChatDraftSelection(files) {
	const readyFiles = files.filter(
		file => String(file.Status || '').toUpperCase() === 'READY'
	)
	const readyIds = new Set(readyFiles.map(file => Number(file.ID)))
	const nextScope = normalizeChatScopeValue(chatState.draftScope)
	let nextSelected = sanitizeSelectedFileIds(chatState.draftSelectedFileIds)

	if (nextScope === CHAT_SCOPE_SINGLE) {
		nextSelected = nextSelected.filter(id => readyIds.has(id)).slice(0, 1)
		if (!nextSelected.length && readyFiles.length > 0) {
			nextSelected = [Number(readyFiles[0].ID)]
		}
	} else if (nextScope === CHAT_SCOPE_MULTI) {
		nextSelected = nextSelected.filter(id => readyIds.has(id))
	} else {
		nextSelected = []
	}

	chatState.draftScope = nextScope
	chatState.draftSelectedFileIds = nextSelected
}

function buildThreadSelectionFromDraft() {
	const scope = normalizeChatScopeValue(chatState.draftScope)
	const readyIds = new Set(getReadyFiles().map(file => Number(file.ID)))
	let selected = sanitizeSelectedFileIds(chatState.draftSelectedFileIds).filter(id =>
		readyIds.has(id)
	)

	if (scope === CHAT_SCOPE_SINGLE) {
		if (selected.length !== 1) {
			return {
				ok: false,
				error: 'Для single-doc нужно выбрать ровно 1 готовый документ.',
			}
		}
		return { ok: true, scope, selected }
	}

	if (scope === CHAT_SCOPE_MULTI) {
		if (selected.length < 2) {
			return {
				ok: false,
				error: 'Для multi-doc нужно выбрать минимум 2 готовых документа.',
			}
		}
		return { ok: true, scope, selected }
	}

	if (getReadyFiles().length === 0) {
		return {
			ok: false,
			error: 'Нет готовых документов со статусом READY для режима all-docs.',
		}
	}

	selected = []
	return { ok: true, scope: CHAT_SCOPE_ALL, selected }
}

function findMatchingThread(scope, selectedFileIds) {
	return (
		chatState.threads.find(
			thread =>
				thread.scope === scope &&
				isSameSelectedFiles(thread.selected_file_ids || [], selectedFileIds || [])
		) || null
	)
}

function renderChatPanel() {
	const root = ensureChatPanelRoot()
	if (!root) return

	const readyFiles = getReadyFiles()
	reconcileChatDraftSelection(readyFiles)

	const activeThread = getChatThreadById(chatState.activeThreadId)
	const activeThreadTitle = activeThread ? getThreadTitle(activeThread) : ''

	const scopeValue = normalizeChatScopeValue(chatState.draftScope)
	const providerValue = normalizeChatProviderValue(chatState.draftProvider)
	const selectedIds = sanitizeSelectedFileIds(chatState.draftSelectedFileIds)

	const singleSelectHtml = `
        <select class="select select-sm select-bordered w-full" data-chat-single-file>
            <option value="">Выберите документ</option>
            ${readyFiles
				.map(file => {
					const id = Number(file.ID)
					const selected = selectedIds.length === 1 && selectedIds[0] === id
					return `<option value="${id}" ${selected ? 'selected' : ''}>${escapeHtml(file.FileName || `file_${id}`)}</option>`
				})
				.join('')}
        </select>
    `

	const multiSelectHtml = readyFiles.length
		? readyFiles
				.map(file => {
					const id = Number(file.ID)
					const checked = selectedIds.includes(id)
					return `
                        <label class="label cursor-pointer justify-start gap-2 py-1">
                            <input class="checkbox checkbox-sm" type="checkbox" value="${id}" data-chat-multi-file ${checked ? 'checked' : ''}/>
                            <span class="label-text">${escapeHtml(file.FileName || `file_${id}`)}</span>
                        </label>
                    `
				})
				.join('')
		: '<p class="text-sm opacity-70">Нет READY документов.</p>'

	const scopeSelectionHtml =
		scopeValue === CHAT_SCOPE_SINGLE
			? singleSelectHtml
			: scopeValue === CHAT_SCOPE_MULTI
				? `<div class="max-h-36 overflow-y-auto border border-base-300 rounded p-2">${multiSelectHtml}</div>`
				: '<p class="text-sm opacity-70">Будут использованы все READY документы пользователя.</p>'

	const threadsHtml = chatState.threadsLoading
		? `
            <div class="text-sm opacity-80 flex items-center gap-2">
                <span class="loading loading-spinner loading-sm"></span>
                <span>Загружаем чаты...</span>
            </div>
        `
		: chatState.threads.length === 0
			? '<p class="text-sm opacity-70">Чатов пока нет. Создайте новый.</p>'
			: chatState.threads
					.map(thread => {
						const isActive =
							Number(chatState.activeThreadId) === Number(thread.thread_id)
						return `
                            <div class="border rounded p-2 ${isActive ? 'border-primary bg-base-200' : 'border-base-300'}">
                                <button
                                    class="text-left w-full"
                                    type="button"
                                    data-chat-open-thread="${thread.thread_id}"
                                >
                                    <div class="font-medium truncate">${escapeHtml(getThreadTitle(thread))}</div>
                                    <div class="text-xs opacity-70 mt-1">${escapeHtml(getThreadSelectionSummary(thread))}</div>
                                    <div class="text-xs opacity-60 mt-1">
                                        ${escapeHtml(chatScopeLabel(thread.scope))} • ${escapeHtml(chatProviderLabel(thread.provider))} • ${escapeHtml(formatApiTimestamp(thread.updated_at))}
                                    </div>
                                </button>
                                <button
                                    class="btn btn-ghost btn-xs mt-2 text-error"
                                    type="button"
                                    data-chat-delete-thread="${thread.thread_id}"
                                    ${Number(chatState.deletingThreadId) === Number(thread.thread_id) ? 'disabled' : ''}
                                >
                                    Delete
                                </button>
                            </div>
                        `
					})
					.join('')

	const messagesHtml = chatState.messagesLoading
		? `
            <div class="text-sm opacity-80 flex items-center gap-2">
                <span class="loading loading-spinner loading-sm"></span>
                <span>Загружаем историю...</span>
            </div>
        `
		: chatState.messagesError
			? `<p class="text-sm text-error">${escapeHtml(chatState.messagesError)}</p>`
			: chatState.messages.length === 0
				? '<p class="text-sm opacity-70">История пуста. Задайте первый вопрос.</p>'
				: chatState.messages
						.map(message => {
							const isUser = message.role === 'user'
							return `
                                <div class="flex ${isUser ? 'justify-end' : 'justify-start'} mb-3">
                                    <div class="max-w-[85%] rounded-lg border ${isUser ? 'bg-primary text-primary-content border-primary' : 'bg-base-200 text-base-content border-base-300'} px-3 py-2">
                                        <div class="text-xs opacity-80 mb-1">${isUser ? 'You' : 'Assistant'} • ${escapeHtml(formatApiTimestamp(message.created_at))}</div>
                                        <div class="whitespace-pre-wrap text-sm leading-6">${escapeHtml(message.content || '')}</div>
                                    </div>
                                </div>
                            `
						})
						.join('')

	const shouldShowJobStatus =
		Boolean(chatState.pendingJobId) || chatState.pendingJobStatus === 'FAILED'
	const statusHtml = shouldShowJobStatus
		? `
            <div class="text-sm ${chatState.pendingJobStatus === 'FAILED' ? 'text-error' : 'opacity-80'}">
                <span class="mr-2">${statusBadge(chatState.pendingJobStatus || 'N/A')}</span>
                ${chatState.pendingJobStatus === 'FAILED'
					? escapeHtml(chatState.pendingJobError || 'Запрос завершился ошибкой.')
					: 'LLM обрабатывает запрос...'
				}
            </div>
        `
		: ''

	const nextHtml = `
        <div class="border border-base-300 rounded-lg bg-base-100 text-base-content shadow-md p-4">
            <div class="flex flex-wrap items-center gap-3 mb-4">
                <h3 class="text-lg font-semibold mr-auto">Document Chat</h3>
                <button class="btn btn-sm btn-outline" type="button" data-chat-refresh-threads>Refresh</button>
            </div>

            ${chatState.threadsError ? `<p class="text-sm text-error mb-3">${escapeHtml(chatState.threadsError)}</p>` : ''}
            ${chatState.uiError ? `<p class="text-sm text-error mb-3">${escapeHtml(chatState.uiError)}</p>` : ''}
            ${chatState.connectionIssue ? `<p class="text-sm text-warning mb-3">${escapeHtml(chatState.connectionIssue)}</p>` : ''}

            <div class="grid grid-cols-1 lg:grid-cols-[340px_1fr] gap-4">
                <aside class="space-y-4">
                    <div class="border border-base-300 rounded p-3 space-y-2">
                        <div class="text-sm font-medium">Новый чат</div>
                        <label class="form-control">
                            <span class="label-text text-xs mb-1">Scope</span>
                            <select class="select select-sm select-bordered w-full" data-chat-scope>
                                <option value="${CHAT_SCOPE_SINGLE}" ${scopeValue === CHAT_SCOPE_SINGLE ? 'selected' : ''}>Single doc</option>
                                <option value="${CHAT_SCOPE_MULTI}" ${scopeValue === CHAT_SCOPE_MULTI ? 'selected' : ''}>Multi doc</option>
                                <option value="${CHAT_SCOPE_ALL}" ${scopeValue === CHAT_SCOPE_ALL ? 'selected' : ''}>All docs</option>
                            </select>
                        </label>
                        <label class="form-control">
                            <span class="label-text text-xs mb-1">Provider</span>
                            <select class="select select-sm select-bordered w-full" data-chat-provider>
                                <option value="${CHAT_PROVIDER_LOCAL}" ${providerValue === CHAT_PROVIDER_LOCAL ? 'selected' : ''}>Local</option>
                                <option value="${CHAT_PROVIDER_GIGACHAT}" ${providerValue === CHAT_PROVIDER_GIGACHAT ? 'selected' : ''}>GigaChat</option>
                            </select>
                        </label>
                        <div class="space-y-1">
                            <span class="text-xs opacity-80">Documents</span>
                            ${scopeSelectionHtml}
                        </div>
                        <button
                            class="btn btn-primary btn-sm w-full"
                            type="button"
                            data-chat-new-thread
                            ${chatState.creatingThread ? 'disabled' : ''}
                        >
                            ${chatState.creatingThread ? 'Создаём...' : 'New Chat'}
                        </button>
                    </div>

                    <div class="space-y-2">
                        <div class="text-sm font-medium">История чатов</div>
                        <div class="space-y-2 max-h-[360px] overflow-y-auto">
                            ${threadsHtml}
                        </div>
                    </div>
                </aside>

                <section class="border border-base-300 rounded p-3 flex flex-col min-h-[520px]">
                    ${
						activeThread
							? `
                        <div class="flex flex-wrap items-center gap-2 pb-3 border-b border-base-300">
                            <div class="font-medium mr-auto">${escapeHtml(activeThreadTitle)}</div>
                            <span class="text-xs opacity-70">Scope: ${escapeHtml(chatScopeLabel(activeThread.scope))}</span>
                            <span class="text-xs opacity-70">Provider: ${escapeHtml(chatProviderLabel(activeThread.provider))}</span>
                            ${chatState.lastUpdatedAt ? `<span class="text-xs opacity-60">Updated: ${escapeHtml(chatState.lastUpdatedAt)}</span>` : ''}
                        </div>
                        <div class="mt-3 flex-1 overflow-y-auto pr-1" data-chat-messages-wrap>
                            ${messagesHtml}
                        </div>
                        <div class="mt-3 space-y-2">
                            ${statusHtml}
                            <textarea
                                class="textarea textarea-bordered w-full min-h-[100px]"
                                placeholder="Введите вопрос по документам..."
                                data-chat-input
                                ${chatState.sendingMessage || !!chatState.pendingJobId ? 'disabled' : ''}
                            >${escapeHtml(chatState.messageDraft)}</textarea>
                            <div class="flex items-center gap-2">
                                <button
                                    class="btn btn-primary btn-sm"
                                    type="button"
                                    data-chat-send
                                    ${chatState.sendingMessage || !!chatState.pendingJobId ? 'disabled' : ''}
                                >
                                    ${chatState.sendingMessage ? 'Отправляем...' : 'Send'}
                                </button>
                                <span class="text-xs opacity-70">Ctrl/Cmd + Enter для отправки</span>
                            </div>
                        </div>
                    `
							: `
                        <div class="h-full flex items-center justify-center text-sm opacity-70">
                            Выберите чат в истории или создайте новый.
                        </div>
                    `
					}
                </section>
            </div>
        </div>
    `

	root.innerHTML = nextHtml

	const messagesWrap = root.querySelector('[data-chat-messages-wrap]')
	if (messagesWrap) {
		messagesWrap.scrollTop = messagesWrap.scrollHeight
	}
}

function stopChatPolling() {
	if (chatPollingId !== null) {
		window.clearInterval(chatPollingId)
		chatPollingId = null
	}
}

async function loadChatThreads(force = false) {
	if (chatState.threadsLoading) return
	if (chatState.threadsLoaded && !force) return

	chatState.threadsLoading = true
	chatState.threadsError = ''
	renderChatPanel()

	try {
		const response = await fetch(chatThreadsUrl(), {
			headers: { Accept: 'application/json' },
		})
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка загрузки чатов'))
		}

		const payload = await response.json()
		const threadsRaw = Array.isArray(payload?.threads) ? payload.threads : []
		chatState.threads = threadsRaw
			.map(normalizeChatThread)
			.filter(thread => thread.thread_id > 0)
		chatState.threadsLoaded = true

		const activeThread = getChatThreadById(chatState.activeThreadId)
		if (!activeThread) {
			setActiveThreadMeta(null)
			resetActiveChatMessages()
		} else {
			setActiveThreadMeta(activeThread)
		}
	} catch (err) {
		chatState.threadsError = normalizeApiError(
			err?.message,
			'Ошибка загрузки чатов'
		)
	} finally {
		chatState.threadsLoading = false
		renderChatPanel()
	}
}

async function loadChatMessages(threadId, { silent = false } = {}) {
	const normalizedThreadId = Number(threadId || 0)
	if (!normalizedThreadId) return

	if (!silent) {
		chatState.messagesLoading = true
		chatState.messagesError = ''
		renderChatPanel()
	}

	try {
		const response = await fetch(chatThreadMessagesUrl(normalizedThreadId), {
			headers: { Accept: 'application/json' },
		})
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка загрузки истории'))
		}

		const payload = await response.json()
		if (Number(chatState.activeThreadId) !== normalizedThreadId) return

		const rawMessages = Array.isArray(payload?.messages) ? payload.messages : []
		chatState.messages = rawMessages.map(normalizeChatMessage)
	} catch (err) {
		if (Number(chatState.activeThreadId) !== normalizedThreadId) return
		chatState.messagesError = normalizeApiError(
			err?.message,
			'Ошибка загрузки истории'
		)
	} finally {
		if (Number(chatState.activeThreadId) !== normalizedThreadId) return
		chatState.messagesLoading = false
		renderChatPanel()
	}
}

async function openChatThread(threadId) {
	const nextThread = getChatThreadById(threadId)
	if (!nextThread) return

	setActiveThreadMeta(nextThread)
	chatState.draftScope = nextThread.scope
	chatState.draftProvider = normalizeChatProviderValue(nextThread.provider)
	chatState.draftSelectedFileIds = sanitizeSelectedFileIds(
		nextThread.selected_file_ids
	)
	chatState.uiError = ''
	resetActiveChatMessages()
	renderChatPanel()
	await loadChatMessages(nextThread.thread_id)
}

function getDefaultThreadTitle(scope, selectedFileIds) {
	if (scope === CHAT_SCOPE_SINGLE && selectedFileIds.length === 1) {
		return `Chat: ${getFileNameById(selectedFileIds[0])}`
	}
	if (scope === CHAT_SCOPE_MULTI) {
		return `Multi-doc (${selectedFileIds.length})`
	}
	if (scope === CHAT_SCOPE_ALL) {
		return 'All docs chat'
	}
	return ''
}

async function createChatThreadFromDraft({ title = '' } = {}) {
	const selection = buildThreadSelectionFromDraft()
	if (!selection.ok) {
		chatState.uiError = selection.error || 'Проверьте параметры чата.'
		renderChatPanel()
		return null
	}

	chatState.creatingThread = true
	chatState.uiError = ''
	renderChatPanel()

	try {
		const requestBody = {
			scope: selection.scope,
			selected_file_ids: selection.selected,
			provider: normalizeChatProviderValue(chatState.draftProvider),
			title: title || getDefaultThreadTitle(selection.scope, selection.selected),
		}
		const response = await fetch(chatThreadsUrl(), {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				Accept: 'application/json',
			},
			body: JSON.stringify(requestBody),
		})
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка создания чата'))
		}

		const payload = await response.json()
		const createdThread = normalizeChatThread(payload)
		if (!createdThread.thread_id) {
			throw new Error('Ответ API не содержит thread_id')
		}

		chatState.threadsLoaded = false
		await loadChatThreads(true)
		await openChatThread(createdThread.thread_id)
		return createdThread
	} catch (err) {
		chatState.uiError = normalizeApiError(err?.message, 'Ошибка создания чата')
		renderChatPanel()
		return null
	} finally {
		chatState.creatingThread = false
		renderChatPanel()
	}
}

async function deleteChatThread(threadId) {
	const normalizedThreadId = Number(threadId || 0)
	if (!normalizedThreadId) return

	chatState.deletingThreadId = normalizedThreadId
	chatState.uiError = ''
	renderChatPanel()

	try {
		const response = await fetch(
			`/api/chat/threads/${encodeURIComponent(normalizedThreadId)}`,
			{
				method: 'DELETE',
			}
		)
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка удаления чата'))
		}

		const wasActive = Number(chatState.activeThreadId) === normalizedThreadId
		chatState.threadsLoaded = false
		await loadChatThreads(true)

		if (wasActive) {
			const nextThread = chatState.threads[0] || null
			if (nextThread) {
				await openChatThread(nextThread.thread_id)
			} else {
				setActiveThreadMeta(null)
				resetActiveChatMessages()
				renderChatPanel()
			}
		}
	} catch (err) {
		chatState.uiError = normalizeApiError(err?.message, 'Ошибка удаления чата')
		renderChatPanel()
	} finally {
		chatState.deletingThreadId = null
		renderChatPanel()
	}
}

async function pollChatJobOnce(jobId) {
	const normalizedJobId = Number(jobId || 0)
	if (!normalizedJobId || chatPollInFlight) return
	if (Number(chatState.pendingJobId) !== normalizedJobId) return
	chatPollInFlight = true

	try {
		const response = await fetch(chatJobUrl(normalizedJobId), {
			headers: { Accept: 'application/json' },
		})
		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка polling chat job'))
		}

		const payload = await response.json()
		if (Number(chatState.pendingJobId) !== normalizedJobId) return

		chatState.pendingJobStatus = String(payload?.status || '').toUpperCase()
		chatState.pendingJobError = payload?.error || ''
		chatState.connectionIssue = ''
		chatState.lastUpdatedAt = formatTimestamp()

		if (chatState.pendingJobStatus === 'DONE') {
			stopChatPolling()
			chatState.pendingJobId = null
			if (Number(chatState.activeThreadId) === Number(payload?.chat_id)) {
				await loadChatMessages(chatState.activeThreadId, { silent: true })
			}
			chatState.threadsLoaded = false
			await loadChatThreads(true)
		} else if (chatState.pendingJobStatus === 'FAILED') {
			stopChatPolling()
			chatState.pendingJobId = null
		}

		renderChatPanel()
	} catch (err) {
		chatState.connectionIssue = 'Проблема соединения, продолжаем polling...'
		renderChatPanel()
		console.error('Failed to poll chat job', err)
	} finally {
		chatPollInFlight = false
	}
}

function startChatPolling(jobId) {
	const normalizedJobId = Number(jobId || 0)
	stopChatPolling()
	if (!normalizedJobId) return

	void pollChatJobOnce(normalizedJobId)
	chatPollingId = window.setInterval(() => {
		void pollChatJobOnce(normalizedJobId)
	}, CHAT_POLL_INTERVAL_MS)
}

async function sendMessageToActiveThread() {
	const activeThreadId = Number(chatState.activeThreadId || 0)
	if (!activeThreadId) {
		chatState.uiError = 'Сначала выберите или создайте чат.'
		renderChatPanel()
		return
	}

	if (chatState.sendingMessage || chatState.pendingJobId) return

	const content = String(chatState.messageDraft || '').trim()
	if (!content) return

	chatState.sendingMessage = true
	chatState.uiError = ''
	chatState.pendingJobError = ''
	chatState.connectionIssue = ''
	renderChatPanel()

	try {
		const response = await fetch(chatThreadMessagesUrl(activeThreadId), {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				Accept: 'application/json',
			},
			body: JSON.stringify({
				content,
				provider: normalizeChatProviderValue(chatState.draftProvider),
				params: {},
			}),
		})

		if (!response.ok) {
			throw new Error(await readApiError(response, 'Ошибка отправки сообщения'))
		}

		const payload = await response.json()
		const messageId = Number(payload?.message_id || 0)
		if (messageId > 0) {
			const exists = chatState.messages.some(
				message => Number(message.message_id) === messageId
			)
			if (!exists) {
				chatState.messages.push(
					normalizeChatMessage({
						message_id: messageId,
						role: 'user',
						content,
						created_at: payload?.question_created_at || new Date().toISOString(),
					})
				)
			}
		}

		chatState.messageDraft = ''
		chatState.pendingJobId = Number(payload?.job_id || 0) || null
		chatState.pendingJobStatus = String(payload?.status || 'QUEUED').toUpperCase()
		chatState.pendingJobError = ''
		chatState.lastUpdatedAt = formatTimestamp()
		renderChatPanel()

		if (chatState.pendingJobId) {
			startChatPolling(chatState.pendingJobId)
		}
	} catch (err) {
		chatState.uiError = normalizeApiError(
			err?.message,
			'Ошибка отправки сообщения'
		)
		renderChatPanel()
	} finally {
		chatState.sendingMessage = false
		renderChatPanel()
	}
}

async function handleGraphChatOpenRequest(detail) {
	const fileId = normalizeFileId(detail?.fileId)
	if (!fileId) return

	chatState.draftScope = CHAT_SCOPE_SINGLE
	chatState.draftProvider = normalizeChatProviderValue(
		preferencesState.chatDefaultProvider
	)
	chatState.draftSelectedFileIds = [fileId]
	chatState.uiError = ''
	renderChatPanel()

	const panelRoot = ensureChatPanelRoot()
	if (panelRoot) {
		panelRoot.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
	}

	await loadChatThreads(false)

	const existing = findMatchingThread(CHAT_SCOPE_SINGLE, [fileId])
	if (existing) {
		await openChatThread(existing.thread_id)
		return
	}

	const fileName = String(detail?.fileName || '').trim() || getFileNameById(fileId)
	await createChatThreadFromDraft({ title: `Chat: ${fileName}` })
}

function setupChatActions() {
	window.addEventListener('graph:chat-open-requested', event => {
		void handleGraphChatOpenRequest(event?.detail || {})
	})

	document.addEventListener('change', event => {
		const scopeSelect = event.target.closest('[data-chat-scope]')
		if (scopeSelect) {
			chatState.draftScope = normalizeChatScopeValue(scopeSelect.value)
			chatState.uiError = ''
			reconcileChatDraftSelection(getReadyFiles())
			renderChatPanel()
			return
		}

		const providerSelect = event.target.closest('[data-chat-provider]')
		if (providerSelect) {
			chatState.draftProvider = normalizeChatProviderValue(providerSelect.value)
			renderChatPanel()
			return
		}

		const singleSelect = event.target.closest('[data-chat-single-file]')
		if (singleSelect) {
			const selectedId = normalizeFileId(singleSelect.value)
			chatState.draftSelectedFileIds = selectedId ? [selectedId] : []
			chatState.uiError = ''
			renderChatPanel()
			return
		}

		const multiCheckbox = event.target.closest('[data-chat-multi-file]')
		if (multiCheckbox) {
			const checkedValues = Array.from(
				document.querySelectorAll('[data-chat-multi-file]:checked')
			).map(element => element.value)
			chatState.draftSelectedFileIds = sanitizeSelectedFileIds(checkedValues)
			chatState.uiError = ''
			renderChatPanel()
		}
	})

	document.addEventListener('input', event => {
		const input = event.target.closest('[data-chat-input]')
		if (!input) return
		chatState.messageDraft = input.value
	})

	document.addEventListener('keydown', event => {
		const input = event.target.closest('[data-chat-input]')
		if (!input) return
		if (!event.ctrlKey && !event.metaKey) return
		if (event.key !== 'Enter') return
		event.preventDefault()
		void sendMessageToActiveThread()
	})

	document.addEventListener('click', event => {
		const refreshButton = event.target.closest('[data-chat-refresh-threads]')
		if (refreshButton) {
			void loadChatThreads(true)
			return
		}

		const newThreadButton = event.target.closest('[data-chat-new-thread]')
		if (newThreadButton) {
			void createChatThreadFromDraft()
			return
		}

		const openThreadButton = event.target.closest('[data-chat-open-thread]')
		if (openThreadButton) {
			const threadId = Number(openThreadButton.dataset.chatOpenThread || 0)
			if (threadId > 0) {
				void openChatThread(threadId)
			}
			return
		}

		const deleteThreadButton = event.target.closest('[data-chat-delete-thread]')
		if (deleteThreadButton) {
			const threadId = Number(deleteThreadButton.dataset.chatDeleteThread || 0)
			if (!threadId) return
			if (!confirm('Delete this chat thread?')) return
			void deleteChatThread(threadId)
			return
		}

		const sendButton = event.target.closest('[data-chat-send]')
		if (sendButton) {
			void sendMessageToActiveThread()
		}
	})
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

		if (hasFilesChanged || forceUpdate) {
			reconcileChatDraftSelection(files)
			renderChatPanel()
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

function setupPreferencesActions() {
	document.addEventListener('change', event => {
		const providerSelect = event.target.closest('[data-pref-provider]')
		if (!providerSelect) return

		const nextValue = normalizeChatProviderValue(providerSelect.value)
		switch (providerSelect.dataset.prefProvider) {
			case 'summary':
				preferencesState.summaryProvider = nextValue
				break
			case 'chapters':
				preferencesState.chaptersProvider = nextValue
				break
			case 'flashcards':
				preferencesState.flashcardsProvider = nextValue
				break
			case 'chat_default':
				preferencesState.chatDefaultProvider = nextValue
				break
			default:
				return
		}

		preferencesState.success = ''
		preferencesState.error = ''
		renderPreferencesPanel()
	})

	document.addEventListener('click', event => {
		const saveButton = event.target.closest('[data-pref-save]')
		if (!saveButton) return
		void savePreferences()
	})
}

document.addEventListener('DOMContentLoaded', function () {
	if (isPreferencesPage()) {
		ensurePreferencesPanelRoot()
		renderPreferencesPanel()
		setupPreferencesActions()
		void loadPreferences(false)
		return
	}

	const initialFileData = readInitialFileData()
	if (initialFileData) {
		// Первое рендерирование всегда принудительное
		renderProfileData(initialFileData, true)
		startPolling()
	} else {
		renderChatPanel()
	}

	renderAnalysisPanel()
	renderChatPanel()
	setupUploadForm()
	setupFilesTableActions()
	setupAnalysisActions()
	setupChatActions()

	void (async () => {
		if (initialFileData || isPreferencesPage()) {
			await loadPreferences(false)
		}
		await loadChatThreads(false)
		if (!chatState.activeThreadId && chatState.threads.length > 0) {
			await openChatThread(chatState.threads[0].thread_id)
		}
	})()
})

window.addEventListener('graph:file-deleted', () => {
	void refreshFilesData(true)
})
