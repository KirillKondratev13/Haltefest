import { DataSet } from 'vis-data'
import { Network } from 'vis-network'
import { getCategoryByFileType } from './fileCategories'

let networkInstance = null
let contextMenu = null
let activeNodes = null

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

		if (downloadButton) {
			window.open(downloadButton.dataset.url, '_blank')
			contextMenu.style.display = 'none'
			return
		}

		if (!deleteButton) return
		if (!confirm('Delete this file?')) return

		try {
			const response = await fetch(deleteButton.dataset.url, { method: 'POST' })
			if (!response.ok) throw new Error(response.statusText)

			const nodeId = Number.parseInt(deleteButton.dataset.node, 10)
			if (activeNodes && Number.isInteger(nodeId)) {
				activeNodes.remove(nodeId)
			}

			contextMenu.style.display = 'none'
			window.dispatchEvent(new Event('graph:file-deleted'))
		} catch (err) {
			alert('Failed to delete: ' + err.message)
		}
	})

	return contextMenu
}

function isTextualFile(fileType) {
	if (!fileType) return false
	return (
		fileType === 'application/pdf' ||
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

export function initUserGraph(username, files) {
	const container = document.getElementById('graph-container')
	if (!container) return

	if (networkInstance) {
		networkInstance.destroy()
		networkInstance = null
	}

	const nodes = new DataSet()
	const edges = new DataSet()
	activeNodes = nodes

	const USER_NODE_ID = 1
	nodes.add({
		id: USER_NODE_ID,
		label: username || 'User',
		shape: 'circle',
		color: '#2563eb',
		font: { color: '#ffffff', size: 18 },
		size: 40,
	})

	const categories = new Set()
	files.forEach(file => categories.add(getCategoryByFileType(file.FileType)))

	const categoryNodeIds = {}
	let categoryNodeCounter = 100
	Array.from(categories).forEach(category => {
		const categoryNodeID = categoryNodeCounter++
		categoryNodeIds[category] = categoryNodeID

		nodes.add({
			id: categoryNodeID,
			label: category,
			shape: 'box',
			color: '#7c3aed',
			size: 28,
		})
		edges.add({ from: USER_NODE_ID, to: categoryNodeID, color: '#9ca3af', arrows: 'to' })
	})

	const tagNodeIds = {}
	let tagNodeCounter = 500
	let fileNodeCounter = 1000

	files.forEach(file => {
		const category = getCategoryByFileType(file.FileType)
		const categoryNodeID = categoryNodeIds[category]
		if (!categoryNodeID) return

		const status = (file.Status || '').toUpperCase()
		const tag = file.Tag || ''
		const failureCause = file.FailureCause || ''
		const nodeID = fileNodeCounter++

		nodes.add({
			id: nodeID,
			label: shortenFileName(file.FileName),
			shape: 'ellipse',
			color: statusColor(status),
			shortLabel: shortenFileName(file.FileName),
			mediumLabel: `${wrapLabel(file.FileName)}\n${formatBytes(file.FileSize)}\n${status || 'N/A'}`,
			fullLabel: `${wrapLabel(file.FileName)}\n${formatBytes(file.FileSize)}\n${status || 'N/A'}\n${
				tag || '-'
			}\n${file.CreatedAt}`,
			downloadUrl: file.DownloadURL,
			deleteUrl: file.DeleteURL,
			fileName: file.FileName,
			fileSize: file.FileSize,
			createdAt: file.CreatedAt,
			status,
			tag,
			failureCause,
		})

		if (isTextualFile(file.FileType)) {
			const tagLabel = normalizeTagLabel(file)
			const tagKey = `${category}:${tagLabel}`

			if (!tagNodeIds[tagKey]) {
				const tagNodeID = tagNodeCounter++
				tagNodeIds[tagKey] = tagNodeID
				nodes.add({
					id: tagNodeID,
					label: tagLabel,
					shape: 'diamond',
					color: '#0ea5e9',
					size: 22,
				})
				edges.add({
					from: categoryNodeID,
					to: tagNodeID,
					color: '#9ca3af',
					arrows: 'to',
				})
			}

			edges.add({
				from: tagNodeIds[tagKey],
				to: nodeID,
				color: '#9ca3af',
				arrows: 'to',
			})
			return
		}

		edges.add({ from: categoryNodeID, to: nodeID, color: '#9ca3af', arrows: 'to' })
	})

	networkInstance = new Network(
		container,
		{ nodes, edges },
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

	const menu = ensureContextMenu()

	networkInstance.on('oncontext', params => {
		params.event.preventDefault()

		const pointer = params.pointer.DOM
		const nodeID = networkInstance.getNodeAt(pointer)
		if (!nodeID) {
			menu.style.display = 'none'
			return
		}

		const node = nodes.get(nodeID)
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
            ${
							node.failureCause
								? `<div class="text-xs text-red-600 mb-2">failure: ${escapeHtml(
										node.failureCause
								  )}</div>`
								: ''
						}
            <button class="graph-btn w-full download-btn" data-url="${escapeHtml(node.downloadUrl)}">
                Download
            </button>
            <button class="graph-btn w-full mt-1 delete-btn" data-url="${escapeHtml(node.deleteUrl)}" data-node="${
							node.id
						}">
                Delete
            </button>
        `

		menu.style.left = pointer.x + window.scrollX + 5 + 'px'
		menu.style.top = pointer.y + window.scrollY + 5 + 'px'
		menu.style.display = 'block'
	})

	networkInstance.on('dragStart', () => {
		menu.style.display = 'none'
	})
	networkInstance.on('zoom', () => {
		menu.style.display = 'none'
	})
	networkInstance.on('deselectNode', () => {
		menu.style.display = 'none'
	})

	const LEVELS = [
		{ scale: 0, show: 'short' },
		{ scale: 1.7, show: 'medium' },
		{ scale: 2.6, show: 'full' },
	]

	networkInstance.on('zoom', params => {
		const scale = params.scale
		const level =
			LEVELS.slice()
				.reverse()
				.find(item => scale >= item.scale) || LEVELS[0]

		nodes.forEach(node => {
			let newLabel = node.label
			if (level.show === 'short' && node.shortLabel) newLabel = node.shortLabel
			if (level.show === 'medium' && node.mediumLabel) newLabel = node.mediumLabel
			if (level.show === 'full' && node.fullLabel) newLabel = node.fullLabel
			if (node.label !== newLabel) {
				nodes.update({ id: node.id, label: newLabel })
			}
		})
	})
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
