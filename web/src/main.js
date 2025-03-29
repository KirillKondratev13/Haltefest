import 'htmx.org'
// main.js
import { initUserGraph } from './graph'

const graphContainer = document.getElementById('graph-container')
if (graphContainer) {
	const username = graphContainer.dataset.username
	initUserGraph(username)
}
