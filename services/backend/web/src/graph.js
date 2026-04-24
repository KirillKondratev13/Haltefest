import { DataSet } from 'vis-data'
import { Network } from 'vis-network'
import { getCategoryByFileType } from './fileCategories'

const USER_NODE_ID = 'user'
const TOP_N_SESSION_KEY = 'graph_auto_tag_top_n'
const DEFAULT_EDGE_COLOR = '#9ca3af'
const HIGHLIGHT_EDGE_COLOR = '#2563eb'
const DIM_EDGE_COLOR = '#e5e7eb'
const DIM_NODE_COLOR = '#e5e7eb'
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
let autoTagTopN = 1
let showTagProbabilities = false
let selectedFileNodeId = ''
let selectedNode = { kind: 'none', nodeId: '' }
let graphInteractionMode = 'full'

function loadTopNFromSession() {
	try {
		const raw = window.sessionStorage.getItem(TOP_N_SESSION_KEY)
		const parsed = Number(raw)
		if (Number.isInteger(parsed) && parsed >= 1 && parsed <= 10) {
			autoTagTopN = parsed
		}
	} catch {
		autoTagTopN = 1
	}
}

function persistTopNToSession() {
	try {
		window.sessionStorage.setItem(TOP_N_SESSION_KEY, String(autoTagTopN))
	} catch {}
}

loadTopNFromSession()

function ensureDataSets() {
	if (!activeNodes) activeNodes = new DataSet()
	if (!activeEdges) activeEdges = new DataSet()
}

function normalizeFileTags(file) {
	const tags = Array.isArray(file?.Tags) ? file.Tags : []
	return tags
		.map(tag => ({
			tagId: Number(tag?.tag_id || 0),
			displayName: String(tag?.display_name || '').trim(),
			source: String(tag?.source || '').toUpperCase(),
			autoRank: Number(tag?.auto_rank || 0),
			score: Number(tag?.score),
		}))
		.filter(tag => tag.tagId > 0 && tag.displayName)
}

function getAutoTagsForRendering(file) {
	const normalized = normalizeFileTags(file)
	const autoTags = normalized
		.filter(tag => tag.source === 'AUTO' && tag.autoRank >= 1)
		.sort((left, right) => left.autoRank - right.autoRank)
	return autoTags.filter(tag => tag.autoRank <= autoTagTopN)
}

function formatTagProbabilityLabel(baseLabel, score) {
	if (!showTagProbabilities || !selectedFileNodeId || !Number.isFinite(score)) {
		return baseLabel
	}
	const percent = Math.max(0, Math.min(100, Math.round(score * 100)))
	return `${baseLabel} (${percent}%)`
}

function resolveTagNodeLabel(node) {
	const baseLabel = String(node.baseLabel || node.label || '')
	if (!showTagProbabilities || !selectedFileNodeId) return baseLabel

	const scoreByFileNode = node.scoreByFileNode || {}
	const score = Number(scoreByFileNode[selectedFileNodeId])
	if (!Number.isFinite(score)) return baseLabel
	return formatTagProbabilityLabel(baseLabel, score)
}

function applyTagProbabilityLabels() {
	if (!activeNodes) return
	const updates = []
	activeNodes.forEach(node => {
		if (node.type !== 'tag') return
		const nextLabel = resolveTagNodeLabel(node)
		if (node.label !== nextLabel) {
			updates.push({ id: node.id, label: nextLabel })
		}
	})
	if (updates.length > 0) {
		activeNodes.update(updates)
	}
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
		if (!contextMenu) return
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
		const chatButton = event.target.closest('.chat-btn')
		const addManualTagButton = event.target.closest('.add-manual-tag-btn')
		const editTagsButton = event.target.closest('.edit-tags-btn')

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

		if (chatButton) {
			if (chatButton.disabled) return
			const fileId = Number(chatButton.dataset.fileId || '')
			if (!Number.isFinite(fileId) || fileId <= 0) return

			window.dispatchEvent(
				new CustomEvent('graph:chat-open-requested', {
					detail: {
						fileId,
						fileName: chatButton.dataset.fileName || 'File',
					},
				})
			)
			contextMenu.style.display = 'none'
			return
		}

		if (addManualTagButton) {
			const fileId = Number(addManualTagButton.dataset.fileId || '')
			if (!Number.isFinite(fileId) || fileId <= 0) return
			window.dispatchEvent(
				new CustomEvent('graph:add-manual-tag-requested', {
					detail: {
						fileId,
						fileName: addManualTagButton.dataset.fileName || 'File',
					},
				})
			)
			contextMenu.style.display = 'none'
			return
		}

		if (editTagsButton) {
			const fileId = Number(editTagsButton.dataset.fileId || '')
			if (!Number.isFinite(fileId) || fileId <= 0) return
			window.dispatchEvent(
				new CustomEvent('graph:edit-tags-requested', {
					detail: {
						fileId,
						fileName: editTagsButton.dataset.fileName || 'File',
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

function normalizeInteractionMode(nextMode) {
	return String(nextMode || '').toLowerCase() === 'readonly' ? 'readonly' : 'full'
}

function formatNodeTagsForMenu(node) {
	const tags = Array.isArray(node?.tags) ? node.tags : []
	if (tags.length === 0) return escapeHtml(node?.tag || '-')
	return tags
		.map(item => {
			const name = String(item?.displayName || '').trim()
			if (!name) return ''
			const source = String(item?.source || '').toUpperCase()
			return source === 'MANUAL' ? `${name} [M]` : name
		})
		.filter(Boolean)
		.join(', ')
}

function removeNodeWithEdges(nodeId) {
	if (!activeNodes || !activeEdges) return

	const edgesToRemove = activeEdges
		.get({ filter: edge => edge.from === nodeId || edge.to === nodeId })
		.map(edge => edge.id)

	if (edgesToRemove.length > 0) activeEdges.remove(edgesToRemove)
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
				JSON.stringify(file.Tags || []),
				file.FailureCause || '',
			].join('|')
		)
		.sort()
		.join('||')

	return `${username || 'User'}::topN=${autoTagTopN}::showProb=${showTagProbabilities ? '1' : '0'}::${fileSignatures}`
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
	const allTags = normalizeFileTags(file)
	const autoTags = getAutoTagsForRendering(file)
	const topAutoTag = autoTags.find(tag => tag.autoRank === 1) || autoTags[0] || null
	const tag = topAutoTag ? topAutoTag.displayName : file.Tag || ''
	const shortLabel = shortenFileName(file.FileName)
	const mediumLabel = `${wrapLabel(file.FileName)}\n${formatBytes(file.FileSize)}\n${status || 'N/A'}`
	const fullLabel = `${wrapLabel(file.FileName)}\n${formatBytes(file.FileSize)}\n${status || 'N/A'}\n${tag || '-'}\n${file.CreatedAt || ''}`

	const baseColor = statusColor(status)
	return {
		id: getFileNodeId(file),
		type: 'file',
		shape: 'ellipse',
		color: baseColor,
		baseColor,
		label: resolveFileLabel(currentLabelMode, shortLabel, mediumLabel, fullLabel),
		shortLabel,
		mediumLabel,
		fullLabel,
		font: { color: '#111827' },
		baseFontColor: '#111827',
		downloadUrl: file.DownloadURL,
		deleteUrl: file.DeleteURL,
		fileName: file.FileName,
		fileSize: file.FileSize,
		createdAt: file.CreatedAt,
		status,
		tag,
		failureCause: file.FailureCause || '',
		fileId: Number(file.ID) || 0,
		tags: allTags,
	}
}

function createEdge(from, to) {
	return {
		id: getEdgeId(from, to),
		from,
		to,
		arrows: 'to',
		color: DEFAULT_EDGE_COLOR,
		baseColor: DEFAULT_EDGE_COLOR,
		width: 2,
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
		baseColor: '#2563eb',
		font: { color: '#ffffff', size: 18 },
		baseFontColor: '#ffffff',
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
				baseColor: '#7c3aed',
				font: { color: '#111827' },
				baseFontColor: '#111827',
				size: 28,
			})
			edges.set(getEdgeId(USER_NODE_ID, categoryNodeId), createEdge(USER_NODE_ID, categoryNodeId))
		}
	})

	files.forEach(file => {
		const category = getCategoryByFileType(file.FileType, file.FileName)
		const categoryNodeId = categories.get(category)
		if (!categoryNodeId) return

		const fileNode = buildFileNode(file)
		nodes.set(fileNode.id, fileNode)

		if (isTextualFile(file.FileType)) {
			const allTags = normalizeFileTags(file)
			const autoTags = getAutoTagsForRendering(file)
			const manualTags = allTags.filter(tag => tag.source === 'MANUAL')
			const tagsForGraph = []
			const seenTagLabels = new Set()

			for (const tag of autoTags) {
				const key = tag.displayName.toLowerCase()
				if (seenTagLabels.has(key)) continue
				seenTagLabels.add(key)
				tagsForGraph.push(tag)
			}
			for (const tag of manualTags) {
				const key = tag.displayName.toLowerCase()
				if (seenTagLabels.has(key)) continue
				seenTagLabels.add(key)
				tagsForGraph.push(tag)
			}

			if (tagsForGraph.length > 0) {
				tagsForGraph.forEach(tag => {
					const tagNodeId = getTagNodeId(category, tag.displayName)
					const scoreByFileNode = Number.isFinite(tag.score)
						? { [fileNode.id]: tag.score }
						: {}
					if (!nodes.has(tagNodeId)) {
						nodes.set(tagNodeId, {
							id: tagNodeId,
							type: 'tag',
							label: tag.displayName,
							baseLabel: tag.displayName,
							scoreByFileNode,
							shape: 'diamond',
							color: '#0ea5e9',
							baseColor: '#0ea5e9',
							font: { color: '#111827' },
							baseFontColor: '#111827',
							size: 22,
						})
					} else {
						const current = nodes.get(tagNodeId)
						nodes.set(tagNodeId, {
							...current,
							baseLabel: current.baseLabel || tag.displayName,
							scoreByFileNode: {
								...(current.scoreByFileNode || {}),
								...scoreByFileNode,
							},
						})
					}

					edges.set(
						getEdgeId(categoryNodeId, tagNodeId),
						createEdge(categoryNodeId, tagNodeId)
					)
					edges.set(
						getEdgeId(tagNodeId, fileNode.id),
						createEdge(tagNodeId, fileNode.id)
					)
				})
				return
			}

			const fallbackTagLabel = normalizeTagLabel(file)
			const fallbackTagNodeId = getTagNodeId(category, fallbackTagLabel)
			if (!nodes.has(fallbackTagNodeId)) {
				nodes.set(fallbackTagNodeId, {
					id: fallbackTagNodeId,
					type: 'tag',
					label: fallbackTagLabel,
					baseLabel: fallbackTagLabel,
					scoreByFileNode: {},
					shape: 'diamond',
					color: '#0ea5e9',
					baseColor: '#0ea5e9',
					font: { color: '#111827' },
					baseFontColor: '#111827',
					size: 22,
				})
			}

			edges.set(getEdgeId(categoryNodeId, fallbackTagNodeId), createEdge(categoryNodeId, fallbackTagNodeId))
			edges.set(getEdgeId(fallbackTagNodeId, fileNode.id), createEdge(fallbackTagNodeId, fileNode.id))
			return
		}

		edges.set(getEdgeId(categoryNodeId, fileNode.id), createEdge(categoryNodeId, fileNode.id))
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
		if (!desiredNodeIds.has(nodeId)) nodesToRemove.push(nodeId)
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
		if (!desiredEdgeIds.has(edgeId)) edgesToRemove.push(edgeId)
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
		if (node.label !== newLabel) updates.push({ id: node.id, label: newLabel })
	})

	if (updates.length > 0) activeNodes.update(updates)
}

function setSelectionByNode(node) {
	if (!node) {
		selectedNode = { kind: 'none', nodeId: '' }
		selectedFileNodeId = ''
		return
	}
	if (node.type === 'file') {
		selectedNode = { kind: 'file', nodeId: node.id }
		selectedFileNodeId = node.id
		return
	}
	if (node.type === 'tag') {
		selectedNode = { kind: 'tag', nodeId: node.id }
		selectedFileNodeId = ''
		return
	}
	selectedNode = { kind: 'none', nodeId: '' }
	selectedFileNodeId = ''
}

function buildHighlightSets() {
	const highlightedNodeIds = new Set()
	const highlightedEdgeIds = new Set()
	if (!activeNodes || !activeEdges || selectedNode.kind === 'none' || !selectedNode.nodeId) {
		return { highlightedNodeIds, highlightedEdgeIds }
	}

	if (selectedNode.kind === 'file') {
		highlightedNodeIds.add(selectedNode.nodeId)
		activeEdges.forEach(edge => {
			// File selection: highlight only depth-1 parents of type TAG.
			if (edge.to !== selectedNode.nodeId) return
			const parentNode = activeNodes.get(edge.from)
			if (!parentNode || parentNode.type !== 'tag') return
			highlightedNodeIds.add(edge.from)
			highlightedEdgeIds.add(edge.id)
		})
		return { highlightedNodeIds, highlightedEdgeIds }
	}

	if (selectedNode.kind === 'tag') {
		highlightedNodeIds.add(selectedNode.nodeId)
		activeEdges.forEach(edge => {
			// Tag selection: highlight only depth-1 children of type FILE.
			if (edge.from !== selectedNode.nodeId) return
			const childNode = activeNodes.get(edge.to)
			if (!childNode || childNode.type !== 'file') return
			highlightedNodeIds.add(edge.to)
			highlightedEdgeIds.add(edge.id)
		})
		return { highlightedNodeIds, highlightedEdgeIds }
	}

	return { highlightedNodeIds, highlightedEdgeIds }
}

function applyGraphEmphasis() {
	if (!activeNodes || !activeEdges) return

	const hasSelection = selectedNode.kind !== 'none' && Boolean(selectedNode.nodeId)
	const { highlightedNodeIds, highlightedEdgeIds } = buildHighlightSets()

	const nodeUpdates = []
	activeNodes.forEach(node => {
		const isHighlighted = hasSelection && highlightedNodeIds.has(node.id)
		const nextColor = hasSelection ? (isHighlighted ? node.baseColor || node.color : DIM_NODE_COLOR) : node.baseColor || node.color
		const nextFontColor = hasSelection
			? isHighlighted
				? node.baseFontColor || '#111827'
				: '#94a3b8'
			: node.baseFontColor || '#111827'
		const nextBorderWidth = hasSelection ? (isHighlighted ? 4 : 1) : 2

		if (node.color !== nextColor || (node.font?.color || '') !== nextFontColor || (node.borderWidth || 0) !== nextBorderWidth) {
			nodeUpdates.push({
				id: node.id,
				color: nextColor,
				font: { ...(node.font || {}), color: nextFontColor },
				borderWidth: nextBorderWidth,
			})
		}
	})
	if (nodeUpdates.length > 0) activeNodes.update(nodeUpdates)

	const edgeUpdates = []
	activeEdges.forEach(edge => {
		const isHighlighted = hasSelection && highlightedEdgeIds.has(edge.id)
		const nextColor = hasSelection ? (isHighlighted ? HIGHLIGHT_EDGE_COLOR : DIM_EDGE_COLOR) : edge.baseColor || DEFAULT_EDGE_COLOR
		const nextWidth = hasSelection ? (isHighlighted ? 4 : 1) : 2
		if (edge.color !== nextColor || (edge.width || 0) !== nextWidth) {
			edgeUpdates.push({ id: edge.id, color: nextColor, width: nextWidth })
		}
	})
	if (edgeUpdates.length > 0) activeEdges.update(edgeUpdates)
}

function refreshSelectionVisuals() {
	applyTagProbabilityLabels()
	applyGraphEmphasis()
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
		if (!node || node.type !== 'file') {
			menu.style.display = 'none'
			return
		}
		setSelectionByNode(node)
		refreshSelectionVisuals()

		const baseInfoHtml = `
                <div class="font-semibold mb-1">${escapeHtml(node.fileName || 'File')}</div>
                <div class="text-xs text-gray-700 mb-2">
                    ${escapeHtml(formatBytes(node.fileSize))} | ${escapeHtml(node.createdAt || '')}
                </div>
                <div class="text-xs text-gray-700 mb-2">
                    status: ${escapeHtml(node.status || 'N/A')}<br/>
                    tags: ${escapeHtml(formatNodeTagsForMenu(node))}
                </div>
                ${node.failureCause
				? `<div class="text-xs text-red-600 mb-2">failure: ${escapeHtml(node.failureCause)}</div>`
				: ''
			}
            `

		if (graphInteractionMode === 'readonly') {
			menu.innerHTML = `
                ${baseInfoHtml}
                ${
									node.downloadUrl
										? `<button class="graph-btn w-full download-btn" data-url="${escapeHtml(node.downloadUrl)}">Download</button>`
										: '<div class="text-xs opacity-70">Read-only mode</div>'
								}
            `
		} else {
			menu.innerHTML = `
                ${baseInfoHtml}
                ${
									node.downloadUrl
										? `<button class="graph-btn w-full download-btn" data-url="${escapeHtml(node.downloadUrl)}">Download</button>`
										: ''
								}
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
                <button
                    class="graph-btn w-full mt-1 chat-btn ${node.status !== 'READY' ? 'opacity-60 cursor-not-allowed' : ''}"
                    data-file-id="${escapeHtml(node.fileId || '')}"
                    data-file-name="${escapeHtml(node.fileName || 'File')}"
                    ${node.status !== 'READY' ? 'disabled title="Файл еще не готов для чата"' : ''}
                >
                    Chat
                </button>
                <hr class="my-2 border-gray-200"/>
                <button
                    class="graph-btn w-full add-manual-tag-btn"
                    data-file-id="${escapeHtml(node.fileId || '')}"
                    data-file-name="${escapeHtml(node.fileName || 'File')}"
                >
                    Add manual tag
                </button>
                <button
                    class="graph-btn w-full mt-1 edit-tags-btn"
                    data-file-id="${escapeHtml(node.fileId || '')}"
                    data-file-name="${escapeHtml(node.fileName || 'File')}"
                >
                    Edit tags
                </button>
            `
		}

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
		setSelectionByNode(null)
		refreshSelectionVisuals()
	})

	networkInstance.on('click', params => {
		const clickedNodeId =
			Array.isArray(params.nodes) && params.nodes.length === 1 ? params.nodes[0] : ''
		const node = clickedNodeId ? activeNodes.get(clickedNodeId) : null
		setSelectionByNode(node)
		refreshSelectionVisuals()
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

	if (selectedNode.nodeId && !desired.nodes.has(selectedNode.nodeId)) {
		setSelectionByNode(null)
	}
	refreshSelectionVisuals()

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
	if (networkInstance && nextFingerprint === lastGraphFingerprint) return false
	initUserGraph(username, normalizedFiles)
	return true
}

export function getGraphTopN() {
	return autoTagTopN
}

export function setGraphTopN(nextValue) {
	const parsed = Number(nextValue)
	if (!Number.isInteger(parsed) || parsed < 1 || parsed > 10) return false
	if (autoTagTopN === parsed) return false
	autoTagTopN = parsed
	persistTopNToSession()
	return true
}

export function setShowTagProbabilities(nextValue) {
	const normalized = Boolean(nextValue)
	if (showTagProbabilities === normalized) return false
	showTagProbabilities = normalized
	refreshSelectionVisuals()
	return true
}

export function setGraphInteractionMode(nextMode) {
	const normalized = normalizeInteractionMode(nextMode)
	if (graphInteractionMode === normalized) return false
	graphInteractionMode = normalized
	if (contextMenu) contextMenu.style.display = 'none'
	return true
}
