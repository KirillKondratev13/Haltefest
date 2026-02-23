import 'htmx.org'
// main.js
import { initUserGraph } from './graph'

const graphContainer = document.getElementById('graph-container')
if (graphContainer) {
	// Считываем JSON из <script id="file-data">
	const fileDataEl = document.getElementById('file-data')
	console.log(fileDataEl)
	if (fileDataEl) {
		const raw = fileDataEl.textContent
		const parsed = JSON.parse(raw)
		console.log(parsed.files, parsed.username)
		const files = parsed.files || []
		const username = parsed.username || 'User'

		initUserGraph(username, files)
	}
}
document.addEventListener('DOMContentLoaded', function () {
	const form = document.getElementById('uploadForm')
	const fileInput = document.getElementById('file')
	const progressBar = document.getElementById('uploadProgress')
	const status = document.getElementById('uploadStatus')

	if (form) {
		form.addEventListener('submit', function (e) {
			e.preventDefault()
			const file = fileInput.files[0]
			if (!file) return

			const formData = new FormData()
			formData.append('file', file)

			const xhr = new XMLHttpRequest()
			xhr.open('POST', '/profile/upload', true)

			xhr.upload.onprogress = function (e) {
				if (e.lengthComputable) {
					const percent = (e.loaded / e.total) * 100
					progressBar.style.width = percent + '%'
					status.textContent = `Uploading: ${percent.toFixed(1)}%`
				}
			}

			xhr.onload = function () {
				if (xhr.status === 200 || xhr.status === 303) {
					status.textContent = 'Upload complete!'
					// Можно обновить список файлов через HTMX или просто location.reload();
					setTimeout(() => location.reload(), 800)
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
})
