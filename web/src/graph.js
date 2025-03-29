import { DataSet } from 'vis-data'
import { Network } from 'vis-network'

export function initUserGraph(username) {
	// Узлы (профиль, видео, текст)
	const nodes = new DataSet([
		{
			id: 1,
			label: username,
			shape: 'circle',
			color: '#6366f1',
			font: { color: '#ffffff', size: 18 },
			size: 40,
		},
		{
			id: 2,
			label: 'Видео',
			shape: 'square',
			color: '#ef4444',
			font: { size: 16 },
			size: 30,
		},
		{
			id: 3,
			label: 'Текст',
			shape: 'diamond',
			color: '#10b981',
			font: { size: 16 },
			size: 30,
		},
	])

	// Связи между узлами
	const edges = new DataSet([
		{ from: 1, to: 2, label: 'загрузил', color: '#888', arrows: 'to' },
		{ from: 1, to: 3, label: 'написал', color: '#888', arrows: 'to' },
	])

	// Контейнер для графа (теперь на всю ширину экрана)
	const container = document.getElementById('graph-container')
	if (!container) return

	// Настройки графа
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
			stabilization: {
				enabled: true,
				iterations: 1000,
			},
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
		height: '600px', // Увеличиваем высоту
		width: '100%', // На всю ширину контейнера
	}

	new Network(container, data, options)
}
