// graph.js
import { DataSet } from 'vis-data'
import { Network } from 'vis-network'
import { getCategoryByFileType } from './fileCategories'

export function initUserGraph(username, files) {
	/* ---------- 1. создаём наборы ---------- */
	const nodes = new DataSet()
	const edges = new DataSet()

	/* ---------- 2. центральный узел пользователя ---------- */
	const USER_NODE_ID = 1
	nodes.add({
		id: USER_NODE_ID,
		label: username,
		shape: 'circle',
		color: '#6366f1',
		font: { color: '#ffffff', size: 18 },
		size: 40,
		shortLabel: username,
		extendedLabel: `${username} (User)`,
	})

	/* ---------- 3. категории ---------- */
	const categorySet = new Set()
	files.forEach(f => categorySet.add(getCategoryByFileType(f.FileType)))
	const categoryArray = Array.from(categorySet)

	let categoryIdCounter = 100
	const categoryIdMap = {}

	categoryArray.forEach(cat => {
		const id = categoryIdCounter++
		categoryIdMap[cat] = id
		nodes.add({
			id,
			label: cat,
			shape: 'box',
			color: '#ef4444',
			size: 30,
			shortLabel: cat,
			extendedLabel: `${cat} (Category)`,
		})
		edges.add({ from: USER_NODE_ID, to: id, color: '#888', arrows: 'to' })
	})

	/* ---------- 4. файлы ---------- */
	let fileNodeId = 1000

	files.forEach(f => {
		const catNodeId = categoryIdMap[getCategoryByFileType(f.FileType)]
		const nodeId = fileNodeId++
		const shortLabel = shortenFileName(f.FileName)
		const mediumLabel = `${wrapLabel(f.FileName)}\n${formatBytes(f.FileSize)}`
		const fullLabel = `${wrapLabel(f.FileName)}\n${formatBytes(f.FileSize)}\n${
			f.CreatedAt
		}`

		nodes.add({
			id: nodeId,
			label: shortLabel,
			shape: 'ellipse',
			color: '#10b981',
			shortLabel,
			mediumLabel,
			fullLabel,
			downloadUrl: f.DownloadURL,
			deleteUrl: f.DeleteURL,
			fileName: f.FileName,
			fileSize: f.FileSize,
			createdAt: f.CreatedAt,
		})

		edges.add({ from: catNodeId, to: nodeId, color: '#888', arrows: 'to' })
	})

	/* ---------- 5. создаём сеть ---------- */
	const container = document.getElementById('graph-container')
	if (!container) return

	const network = new Network(
		container,
		{ nodes, edges },
		{
			nodes: { borderWidth: 2, shadow: true },
			edges: { width: 2, smooth: true, font: { size: 12, strokeWidth: 0 } },
			physics: {
				stabilization: { enabled: true, iterations: 1000 },
				solver: 'forceAtlas2Based',
				forceAtlas2Based: {
					gravitationalConstant: -50,
					centralGravity: 0.01,
					springLength: 150,
					springConstant: 0.05,
				},
			},
			interaction: { hover: true, tooltipDelay: 200 },
			height: '600px',
			width: '100%',
		}
	)

	// ====== КОНТЕКСТНОЕ МЕНЮ ======

	// Создаем div для меню
	let contextMenu = document.getElementById('graph-context-menu')
	if (!contextMenu) {
		contextMenu = document.createElement('div')
		contextMenu.id = 'graph-context-menu'
		contextMenu.style.position = 'absolute'
		contextMenu.style.display = 'none'
		contextMenu.style.zIndex = 20000
		contextMenu.style.color = '#1f2937'
		contextMenu.className =
			'bg-white rounded-lg shadow-xl p-2 border border-gray-200 min-w-[180px]'

		document.body.appendChild(contextMenu)
	}

	// Скроем меню при клике вне его
	document.addEventListener('mousedown', e => {
		if (
			contextMenu.style.display === 'block' &&
			!contextMenu.contains(e.target)
		) {
			contextMenu.style.display = 'none'
		}
	})

	// Контекстное меню по правому клику на узле
	network.on('oncontext', function (params) {
		params.event.preventDefault() // Запретить стандартное меню

		const pointer = params.pointer.DOM

		// Определяем, был ли клик на узле
		const nodeId = network.getNodeAt(pointer)
		if (!nodeId) {
			contextMenu.style.display = 'none'
			return
		}

		const node = nodes.get(nodeId)
		if (!node || !node.downloadUrl) return // Только для файловых узлов

		// Формируем меню
		contextMenu.innerHTML = `
            <div class="font-semibold mb-1 ">${node.fileName || 'File'}</div>
            <div class="text-xs text-gray-900 mb-2">${formatBytes(
							node.fileSize
						)} | ${node.createdAt}</div>
            <button class="graph-btn w-full download-btn" data-url="${
							node.downloadUrl
						}">
                📥 Скачать
            </button>
            <button class="graph-btn w-full mt-1 delete-btn" data-url="${
							node.deleteUrl
						}" data-node="${node.id}">
                🗑 Удалить
            </button>
        `

		// Позиционируем меню (учти скролл!)
		contextMenu.style.left = pointer.x + window.scrollX + 5 + 'px'
		contextMenu.style.top = pointer.y + window.scrollY + 5 + 'px'
		contextMenu.style.display = 'block'
	})

	// Обработка кликов по меню
	contextMenu.addEventListener('click', async function (e) {
		const dl = e.target.closest('.download-btn')
		const del = e.target.closest('.delete-btn')
		if (dl) {
			window.open(dl.dataset.url, '_blank')
			contextMenu.style.display = 'none'
			return
		}
		if (del) {
			if (!confirm('Удалить этот файл?')) return
			try {
				const res = await fetch(del.dataset.url, { method: 'POST' })
				if (!res.ok) throw new Error(res.statusText)
				nodes.remove(parseInt(del.dataset.node, 10))
				contextMenu.style.display = 'none'
			} catch (err) {
				alert('Не удалось удалить: ' + err.message)
			}
		}
	})

	// Скрываем меню по колёсику мыши/перетаскиванию
	network.on('dragStart', () => {
		contextMenu.style.display = 'none'
	})
	network.on('zoom', () => {
		contextMenu.style.display = 'none'
	})
	network.on('deselectNode', () => {
		contextMenu.style.display = 'none'
	})

	// ========== zoom-уровни ========== (как у тебя)
	const LEVELS = [
		{ scale: 0, show: 'short' }, // scale < 1.5
		{ scale: 1.5, show: 'medium' }, // 1.5 ≤ scale < 2.5
		{ scale: 2.5, show: 'full' }, // scale ≥ 2.5
	]
	network.on('zoom', params => {
		const scale = params.scale
		// Найдем максимальный подходящий level по scale:
		const level =
			LEVELS.slice()
				.reverse()
				.find(l => scale >= l.scale) || LEVELS[0]
		nodes.forEach(n => {
			let newLabel = n.shortLabel
			if (level.show === 'medium' && n.mediumLabel) newLabel = n.mediumLabel
			else if (level.show === 'full' && n.fullLabel) newLabel = n.fullLabel
			if (n.label !== newLabel) nodes.update({ id: n.id, label: newLabel })
		})
		let springLength = 150
		let gravConst = -50
		if (scale > 2.5) {
			springLength = 320
			gravConst = -120
		} else if (scale > 1.5) {
			springLength = 210
			gravConst = -80
		}
		network.setOptions({
			physics: {
				forceAtlas2Based: {
					springLength,
					gravitationalConstant: gravConst,
				},
			},
		})
	})
}

// ========== helpers ========== (оставляем как есть)
function shortenFileName(name) {
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
	// Не разбивать внутри расширения файла
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
