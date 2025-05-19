// graph.js
import { DataSet } from 'vis-data'
import { Network } from 'vis-network'
import { getCategoryByFileType } from './fileCategories' // если отдельный файл

export function initUserGraph(username, files) {
	// 1. Создаём DataSet для узлов и рёбер
	const nodes = new DataSet()
	const edges = new DataSet()

	// Добавляем "узел пользователя"
	const USER_NODE_ID = 1
	nodes.add({
		id: USER_NODE_ID,
		label: username,
		shape: 'circle',
		color: '#6366f1',
		font: { color: '#ffffff', size: 18 },
		size: 40,
		// Можем хранить нечто вроде: extendedLabel, shortLabel
		shortLabel: username,
		extendedLabel: username + ' (User node)',
	})

	// Собираем категории
	// Вычислим все категории (множество)
	let categorySet = new Set()
	files.forEach(f => {
		const cat = getCategoryByFileType(f.FileType)
		categorySet.add(cat)
	})

	// Превращаем Set в массив
	const categoryArray = Array.from(categorySet)

	// Создадим map категория -> nodeId
	let categoryIdCounter = 100
	let categoryIdMap = {}

	categoryArray.forEach(cat => {
		categoryIdMap[cat] = categoryIdCounter
		nodes.add({
			id: categoryIdCounter,
			label: cat,
			shape: 'box',
			color: '#ef4444', // красный
			size: 30,
			shortLabel: cat,
			extendedLabel: cat + ' (Category)',
		})
		edges.add({
			from: USER_NODE_ID,
			to: categoryIdCounter,
			label: '',
			color: '#888',
			arrows: 'to',
		})
		categoryIdCounter++
	})

	// Теперь добавим узлы для файлов
	let fileNodeIdStart = 1000
	files.forEach(f => {
		const cat = getCategoryByFileType(f.FileType)
		const catNodeId = categoryIdMap[cat]

		const nodeId = fileNodeIdStart
		fileNodeIdStart++

		// Сформируем "короткую подпись" (только имя файла)
		const shortLabel = shortenFileName(f.FileName)

		// "Расширенная подпись" (можем включить размер, дату):
		const extendedLabel = `${f.FileName}\nSize: ${f.FileSize} bytes\nCreated: ${f.CreatedAt}\n Link: ${f.DownloadURL}\n Delete: ${f.DeleteURL}\n(Zoom in for more...)`
		//const extendedLabel = `${f.FileName}\nSize: ${f.FileSize} bytes\nCreated: ${f.CreatedAt}\n Link: ${<a href={ templ.URL(f.DownloadURL) }>download</a>}\n(Zoom in for more...)`
		nodes.add({
			id: nodeId,
			label: shortLabel, // По умолчанию пусть будет короткая
			shape: 'ellipse',
			color: '#10b981', // зелёный
			shortLabel,
			extendedLabel,
			// Дополнительно можем хранить DownloadURL, DeleteURL
			downloadURL: f.DownloadURL,
			deleteURL: f.DeleteURL,
		})

		edges.add({
			from: catNodeId,
			to: nodeId,
			label: '',
			color: '#888',
			arrows: 'to',
		})
	})

	// Инициализируем Network
	const container = document.getElementById('graph-container')
	if (!container) return

	const data = { nodes, edges }
	const options = {
		nodes: {
			borderWidth: 2,
			shadow: true,
		},
		edges: {
			width: 2,
			smooth: true,
			font: { size: 12, strokeWidth: 0 },
		},
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
		interaction: {
			hover: true,
			tooltipDelay: 200,
		},
		height: '600px', // Как у вас
		width: '100%',
	}

	const network = new Network(container, data, options)

	// Слушаем zoom, чтобы переключать label
	network.on('zoom', params => {
		// params.scale — текущий уровень зума
		const threshold = 1.5
		if (params.scale >= threshold) {
			// Устанавливаем extendedLabel
			nodes.forEach(node => {
				nodes.update({
					id: node.id,
					label: node.extendedLabel,
				})
			})
		} else {
			// Возвращаем shortLabel
			nodes.forEach(node => {
				nodes.update({
					id: node.id,
					label: node.shortLabel,
				})
			})
		}
	})
}
function shortenFileName(filename) {
	// Находим расширение файла (последнее вхождение точки)
	const lastDotIndex = filename.lastIndexOf('.')
	const extension = lastDotIndex !== -1 ? filename.slice(lastDotIndex) : ''

	// Находим номер копии (например, "(1)")
	const copyNumberMatch = filename.match(/\(\d+\)(?=\.[^.]*$|$)/)
	const copyNumber = copyNumberMatch ? copyNumberMatch[0] : ''

	// Берем первые 10 символов основного имени (без учета копии и расширения)
	const baseName = filename.slice(0, 10)

	// Собираем итоговое имя: первые 10 символов + "*" + номер копии + расширение
	return `${baseName}*${copyNumber}${extension}`
}
