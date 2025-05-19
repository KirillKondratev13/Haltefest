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
