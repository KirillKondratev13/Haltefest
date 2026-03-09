import 'htmx.org'
import { initUserGraph } from './graph'

const FILES_DATA_URL = '/profile/files/data'
const POLL_INTERVAL_MS = 5000

let refreshInFlight = false
let pollingId = null

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

function readInitialFileData() {
	const fileDataEl = document.getElementById('file-data')
	if (!fileDataEl) return null

	try {
		return normalizeFileData(JSON.parse(fileDataEl.textContent))
	} catch {
		return null
	}
}

function renderProfileData(payload) {
	const { files, username } = normalizeFileData(payload)
	renderFilesTable(files)
	initUserGraph(username, files)
}

async function refreshFilesData() {
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
		renderProfileData(payload)
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
			await refreshFilesData()
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
				await refreshFilesData()
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
		void refreshFilesData()
	}, POLL_INTERVAL_MS)
}

document.addEventListener('DOMContentLoaded', function () {
	const initialFileData = readInitialFileData()
	if (initialFileData) {
		renderProfileData(initialFileData)
		startPolling()
	}

	setupUploadForm()
	setupFilesTableActions()
})

window.addEventListener('graph:file-deleted', () => {
	void refreshFilesData()
})
