import { DataSet } from 'vis-data'
import { Network } from 'vis-network'
import { getCategoryByFileType } from './fileCategories'

const USER_NODE_ID = 'user'
const EDGE_STYLE = { color: '#9ca3af', arrows: 'to' }
const LABEL_LEVELS = [
	{ scale: 0, mode: 'short' },
	{ scale: 1.7, mode: 'medium' },
	{ scale: 2.6, mode: 'full' },
]

let networkInstance = null
let contextMenu = null
let activeNodes = null
let activeEdges = null
let savedNodePositions = new Map()
let currentLabelMode = 'short'
let lastGraphFingerprint = ''

function ensureDataSets() {
	if (!activeNodes) activeNodes = new DataSet()
	if (!activeEdges) activeEdges = new DataSet()
}

function ensureContextMenu() {
	if (contextMenu) return contextMenu

	contextMenu = document.createElement('div')
	contextMenu.id = 'graph-context-menu'
	contextMenu.style.position = 'absolute'
	contextMenu.style.display = 'none'
	contextMenu.style.zIndex = 20000
	contextMenu.style.color = '#1f2937'
	contextMenu.className =
		'bg-white rounded-lg shadow-xl p-2 border border-gray-200 min-w-[220px]'
	document.body.appendChild(contextMenu)

	document.addEventListener('mousedown', event => {
		if (
			contextMenu.style.display === 'block' &&
			!contextMenu.contains(event.target)
		) {
			contextMenu.style.display = 'none'
		}
	})

	contextMenu.addEventListener('click', async event => {
		const downloadButton = event.target.closest('.download-btn')
		const deleteButton = event.target.closest('.delete-btn')
		const analysisButton = event.target.closest('.analysis-btn')

		if (downloadButton) {
			window.open(downloadButton.dataset.url, '_blank')
			contextMenu.style.display = 'none'
			return
		}

		if (analysisButton) {
			if (analysisButton.disabled) return

			const fileId = Number(analysisButton.dataset.fileId || '')
			const analysisType = analysisButton.dataset.analysisType || ''
			if (!Number.isFinite(fileId) || fileId <= 0 || !analysisType) return

			window.dispatchEvent(
				new CustomEvent('graph:analysis-requested', {
					detail: {
						fileId,
						fileName: analysisButton.dataset.fileName || 'File',
						analysisType,
					},
				})
			)
			contextMenu.style.display = 'none'
			return
		}

		if (!deleteButton) return
		if (!confirm('Delete this file?')) return

		try {
			const response = await fetch(deleteButton.dataset.url, { method: 'POST' })
			if (!response.ok) throw new Error(response.statusText)

			const nodeId = deleteButton.dataset.node
			if (activeNodes && nodeId) {
				removeNodeWithEdges(nodeId)
			}

			contextMenu.style.display = 'none'
			window.dispatchEvent(new Event('graph:file-deleted'))
		} catch (err) {
			alert('Failed to delete: ' + err.message)
		}
	})

	return contextMenu
}

function removeNodeWithEdges(nodeId) {
	if (!activeNodes || !activeEdges) return

	const edgesToRemove = activeEdges
		.get({ filter: edge => edge.from === nodeId || edge.to === nodeId })
		.map(edge => edge.id)

	if (edgesToRemove.length > 0) {
		activeEdges.remove(edgesToRemove)
	}

	activeNodes.remove(nodeId)
	savedNodePositions.delete(nodeId)
}

function getGraphFingerprint(username, files) {
	const fileSignatures = files
		.map(file =>
			[
				file.FileName || '',
				file.CreatedAt || '',
				file.FileSize || '',
				file.FileType || '',
				file.Status || '',
				file.Tag || '',
				file.FailureCause || '',
			].join('|')
		)
		.sort()
		.join('||')

	return `${username || 'User'}::${fileSignatures}`
}

function getLabelModeByScale(scale) {
	const level =
		LABEL_LEVELS.slice().reverse().find(item => scale >= item.scale) ||
		LABEL_LEVELS[0]
	return level.mode
}

function getFileIdentity(file) {
	return [file.FileName || '', file.CreatedAt || '', file.FileSize || ''].join('|')
}

function getFileNodeId(file) {
	return `file:${encodeURIComponent(getFileIdentity(file))}`
}

function getCategoryNodeId(category) {
	return `category:${encodeURIComponent(category)}`
}

function getTagNodeId(category, tagLabel) {
	return `tag:${encodeURIComponent(category)}:${encodeURIComponent(tagLabel)}`
}

function getEdgeId(from, to) {
	return `edge:${from}->${to}`
}

function isTextualFile(fileType) {
	if (!fileType) return false
	return (
		fileType === 'application/pdf' ||
		fileType === 'application/msword' ||
		fileType ===
			'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
		fileType.startsWith('text/plain')
	)
}

function normalizeTagLabel(file) {
	const tag = (file.Tag || '').trim()
	if (tag) return tag

	const status = (file.Status || '').toUpperCase()
	if (status === 'ERROR') return 'Error'
	if (status === 'PROCESSING' || status === 'PENDING') return 'Pending'
	return 'Unclassified'
}

function statusColor(status) {
	const value = (status || '').toUpperCase()
	switch (value) {
		case 'READY':
			return '#10b981'
		case 'PROCESSING':
			return '#f59e0b'
		case 'ERROR':
			return '#ef4444'
		default:
			return '#64748b'
	}
}

function saveNodePositions(nodeIds = null) {
	if (!networkInstance || !activeNodes) return

	const ids = nodeIds || activeNodes.getIds()
	if (!ids || ids.length === 0) return

	const positions = networkInstance.getPositions(ids)
	Object.entries(positions).forEach(([nodeId, pos]) => {
		savedNodePositions.set(nodeId, pos)
	})
}

function applySavedPosition(node) {
	const savedPos = savedNodePositions.get(node.id)
	if (!savedPos) return node
	return { ...node, x: savedPos.x, y: savedPos.y }
}

function resolveFileLabel(mode, shortLabel, mediumLabel, fullLabel) {
	if (mode === 'full') return fullLabel || mediumLabel || shortLabel
	if (mode === 'medium') return mediumLabel || shortLabel
	return shortLabel
}

function buildFileNode(file) {
	const status = (file.Status || '').toUpperCase()
	const tag = file.Tag || ''
	const shortLabel = shortenFileName(file.FileName)
	const mediumLabel = `${wrapLabel(file.FileName)}\n${formatBytes(file.FileSize)}\n${status || 'N/A'}`
	const fullLabel = `${wrapLabel(file.FileName)}\n${formatBytes(file.FileSize)}\n${status || 'N/A'}\n${tag || '-'}\n${file.CreatedAt || ''}`

	return {
		id: getFileNodeId(file),
		type: 'file',
		shape: 'ellipse',
		color: statusColor(status),
		label: resolveFileLabel(currentLabelMode, shortLabel, mediumLabel, fullLabel),
		shortLabel,
		mediumLabel,
		fullLabel,
		downloadUrl: file.DownloadURL,
		deleteUrl: file.DeleteURL,
		fileName: file.FileName,
		fileSize: file.FileSize,
		createdAt: file.CreatedAt,
		status,
		tag,
		failureCause: file.FailureCause || '',
		fileId: Number(file.ID) || 0,
	}
}

function buildDesiredGraph(username, files) {
	const nodes = new Map()
	const edges = new Map()

	nodes.set(USER_NODE_ID, {
		id: USER_NODE_ID,
		type: 'user',
		label: username || 'User',
		shape: 'circle',
		color: '#2563eb',
		font: { color: '#ffffff', size: 18 },
		size: 40,
	})

	const categories = new Map()
	files.forEach(file => {
		const category = getCategoryByFileType(file.FileType, file.FileName)
		if (!categories.has(category)) {
			const categoryNodeId = getCategoryNodeId(category)
			categories.set(category, categoryNodeId)
			nodes.set(categoryNodeId, {
				id: categoryNodeId,
				type: 'category',
				label: category,
				shape: 'box',
				color: '#7c3aed',
				size: 28,
			})
			edges.set(getEdgeId(USER_NODE_ID, categoryNodeId), {
				id: getEdgeId(USER_NODE_ID, categoryNodeId),
				from: USER_NODE_ID,
				to: categoryNodeId,
				...EDGE_STYLE,
			})
		}
	})

	files.forEach(file => {
		const category = getCategoryByFileType(file.FileType, file.FileName)
		const categoryNodeId = categories.get(category)
		if (!categoryNodeId) return

		const fileNode = buildFileNode(file)
		nodes.set(fileNode.id, fileNode)

		if (isTextualFile(file.FileType)) {
			const tagLabel = normalizeTagLabel(file)
			const tagNodeId = getTagNodeId(category, tagLabel)

			if (!nodes.has(tagNodeId)) {
				nodes.set(tagNodeId, {
					id: tagNodeId,
					type: 'tag',
					label: tagLabel,
					shape: 'diamond',
					color: '#0ea5e9',
					size: 22,
				})
			}

			edges.set(getEdgeId(categoryNodeId, tagNodeId), {
				id: getEdgeId(categoryNodeId, tagNodeId),
				from: categoryNodeId,
				to: tagNodeId,
				...EDGE_STYLE,
			})

			edges.set(getEdgeId(tagNodeId, fileNode.id), {
				id: getEdgeId(tagNodeId, fileNode.id),
				from: tagNodeId,
				to: fileNode.id,
				...EDGE_STYLE,
			})

			return
		}

		edges.set(getEdgeId(categoryNodeId, fileNode.id), {
			id: getEdgeId(categoryNodeId, fileNode.id),
			from: categoryNodeId,
			to: fileNode.id,
			...EDGE_STYLE,
		})
	})

	return { nodes, edges }
}

function syncNodes(desiredNodes) {
	const existingNodeIds = new Set(activeNodes.getIds())
	const desiredNodeIds = new Set(desiredNodes.keys())
	const nodesToAdd = []
	const nodesToUpdate = []
	const nodesToRemove = []

	for (const [nodeId, node] of desiredNodes.entries()) {
		if (existingNodeIds.has(nodeId)) {
			nodesToUpdate.push(node)
		} else {
			nodesToAdd.push(applySavedPosition(node))
		}
	}

	existingNodeIds.forEach(nodeId => {
		if (!desiredNodeIds.has(nodeId)) {
			nodesToRemove.push(nodeId)
		}
	})

	if (nodesToRemove.length > 0) {
		activeNodes.remove(nodesToRemove)
		nodesToRemove.forEach(nodeId => savedNodePositions.delete(nodeId))
	}
	if (nodesToAdd.length > 0) activeNodes.add(nodesToAdd)
	if (nodesToUpdate.length > 0) activeNodes.update(nodesToUpdate)
}

function syncEdges(desiredEdges) {
	const existingEdgeIds = new Set(activeEdges.getIds())
	const desiredEdgeIds = new Set(desiredEdges.keys())
	const edgesToAdd = []
	const edgesToUpdate = []
	const edgesToRemove = []

	for (const [edgeId, edge] of desiredEdges.entries()) {
		if (existingEdgeIds.has(edgeId)) {
			edgesToUpdate.push(edge)
		} else {
			edgesToAdd.push(edge)
		}
	}

	existingEdgeIds.forEach(edgeId => {
		if (!desiredEdgeIds.has(edgeId)) {
			edgesToRemove.push(edgeId)
		}
	})

	if (edgesToRemove.length > 0) activeEdges.remove(edgesToRemove)
	if (edgesToAdd.length > 0) activeEdges.add(edgesToAdd)
	if (edgesToUpdate.length > 0) activeEdges.update(edgesToUpdate)
}

function applyZoomLabels(scale) {
	const nextMode = getLabelModeByScale(scale)
	if (nextMode === currentLabelMode) return

	currentLabelMode = nextMode
	const updates = []

	activeNodes.forEach(node => {
		if (node.type !== 'file') return

		const newLabel = resolveFileLabel(
			currentLabelMode,
			node.shortLabel,
			node.mediumLabel,
			node.fullLabel
		)
		if (node.label !== newLabel) {
			updates.push({ id: node.id, label: newLabel })
		}
	})

	if (updates.length > 0) {
		activeNodes.update(updates)
	}
}

function setupNetworkHandlers() {
	const menu = ensureContextMenu()

	networkInstance.on('oncontext', params => {
		params.event.preventDefault()

		const pointer = params.pointer.DOM
		const nodeId = networkInstance.getNodeAt(pointer)
		if (!nodeId) {
			menu.style.display = 'none'
			return
		}

		const node = activeNodes.get(nodeId)
		if (!node || !node.downloadUrl) {
			menu.style.display = 'none'
			return
		}

		menu.innerHTML = `
                <div class="font-semibold mb-1">${escapeHtml(node.fileName || 'File')}</div>
                <div class="text-xs text-gray-700 mb-2">
                    ${escapeHtml(formatBytes(node.fileSize))} | ${escapeHtml(node.createdAt || '')}
                </div>
                <div class="text-xs text-gray-700 mb-2">
                    status: ${escapeHtml(node.status || 'N/A')}<br/>
                    tag: ${escapeHtml(node.tag || '-')}
                </div>
                ${node.failureCause
				? `<div class="text-xs text-red-600 mb-2">failure: ${escapeHtml(node.failureCause)}</div>`
				: ''
			}
                <button class="graph-btn w-full download-btn" data-url="${escapeHtml(node.downloadUrl)}">
                    Download
                </button>
                <button class="graph-btn w-full mt-1 delete-btn" data-url="${escapeHtml(node.deleteUrl)}" data-node="${escapeHtml(node.id)}">
                    Delete
                </button>
                <hr class="my-2 border-gray-200"/>
                <button
                    class="graph-btn w-full analysis-btn ${node.status !== 'READY' ? 'opacity-60 cursor-not-allowed' : ''}"
                    data-file-id="${escapeHtml(node.fileId || '')}"
                    data-file-name="${escapeHtml(node.fileName || 'File')}"
                    data-analysis-type="summary"
                    ${node.status !== 'READY' ? 'disabled title="Файл еще не готов к анализу"' : ''}
                >
                    Summary
                </button>
                <button
                    class="graph-btn w-full mt-1 analysis-btn ${node.status !== 'READY' ? 'opacity-60 cursor-not-allowed' : ''}"
                    data-file-id="${escapeHtml(node.fileId || '')}"
                    data-file-name="${escapeHtml(node.fileName || 'File')}"
                    data-analysis-type="chapters"
                    ${node.status !== 'READY' ? 'disabled title="Файл еще не готов к анализу"' : ''}
                >
                    Chapters
                </button>
                <button
                    class="graph-btn w-full mt-1 analysis-btn ${node.status !== 'READY' ? 'opacity-60 cursor-not-allowed' : ''}"
                    data-file-id="${escapeHtml(node.fileId || '')}"
                    data-file-name="${escapeHtml(node.fileName || 'File')}"
                    data-analysis-type="flashcards"
                    ${node.status !== 'READY' ? 'disabled title="Файл еще не готов к анализу"' : ''}
                >
                    Flashcards
                </button>
            `

		menu.style.left = pointer.x + window.scrollX + 5 + 'px'
		menu.style.top = pointer.y + window.scrollY + 5 + 'px'
		menu.style.display = 'block'
	})

	networkInstance.on('dragStart', () => {
		menu.style.display = 'none'
	})

	networkInstance.on('dragEnd', () => {
		saveNodePositions()
	})

	networkInstance.on('zoom', params => {
		menu.style.display = 'none'
		applyZoomLabels(params.scale)
	})

	networkInstance.on('deselectNode', () => {
		menu.style.display = 'none'
	})

	networkInstance.once('stabilized', () => {
		saveNodePositions()
	})
}

function ensureNetwork(container) {
	ensureDataSets()

	if (networkInstance) return

	networkInstance = new Network(
		container,
		{ nodes: activeNodes, edges: activeEdges },
		{
			nodes: { borderWidth: 2, shadow: true },
			edges: { width: 2, smooth: true, font: { size: 12, strokeWidth: 0 } },
			physics: {
				stabilization: { enabled: true, iterations: 700 },
				solver: 'forceAtlas2Based',
				forceAtlas2Based: {
					gravitationalConstant: -60,
					centralGravity: 0.01,
					springLength: 160,
					springConstant: 0.05,
				},
			},
			interaction: { hover: true, tooltipDelay: 150 },
			height: '600px',
			width: '100%',
		}
	)

	setupNetworkHandlers()
	applyZoomLabels(networkInstance.getScale())
}

export function initUserGraph(username, files) {
	const container = document.getElementById('graph-container')
	if (!container) return false

	ensureNetwork(container)
	saveNodePositions()

	const normalizedFiles = Array.isArray(files) ? files : []
	const desired = buildDesiredGraph(username, normalizedFiles)
	syncNodes(desired.nodes)
	syncEdges(desired.edges)

	lastGraphFingerprint = getGraphFingerprint(username, normalizedFiles)
	return true
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

function shortenFileName(name) {
	if (!name) return 'File'
	const lastDot = name.lastIndexOf('.')
	const ext = lastDot !== -1 ? name.slice(lastDot) : ''
	const copy = (name.match(/\(\d+\)(?=\.[^.]*$|$)/) || [''])[0]
	return `${name.slice(0, 10)}*${copy}${ext}`
}

function formatBytes(bytes) {
	if (!bytes) return '0 B'
	const k = 1024
	const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
	const i = Math.floor(Math.log(bytes) / Math.log(k))
	return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i]
}

function wrapLabel(text, lineLength = 20) {
	if (!text) return ''

	const lastDot = text.lastIndexOf('.')
	let name = text
	let ext = ''
	if (lastDot > 0) {
		name = text.slice(0, lastDot)
		ext = text.slice(lastDot)
	}

	const regex = new RegExp(`.{1,${lineLength}}`, 'g')
	const lines = name.match(regex) || []
	if (ext) lines.push(ext)
	return lines.join('\n')
}

export function getGraphState() {
	return {
		networkInstance,
		activeNodes,
		activeEdges,
		savedNodePositions,
	}
}

export function updateUserGraph(username, files) {
	const normalizedFiles = Array.isArray(files) ? files : []
	const nextFingerprint = getGraphFingerprint(username, normalizedFiles)

	if (networkInstance && nextFingerprint === lastGraphFingerprint) {
		return false
	}

	initUserGraph(username, normalizedFiles)
	return true
}
