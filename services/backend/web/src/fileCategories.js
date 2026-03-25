const loggedCategoryDecisions = new Set()

function logCategoryDecision(inputFileType, fileName, normalizedFileType, category, reason) {
	if (typeof window === 'undefined' || !window.console?.debug) return

	const key = `${normalizedFileType}|${fileName || ''}|${category}|${reason}`
	if (loggedCategoryDecisions.has(key)) return
	loggedCategoryDecisions.add(key)

	console.debug('[fileCategories] category resolved', {
		inputFileType,
		fileName,
		normalizedFileType,
		category,
		reason,
	})
}

export function getCategoryByFileType(fileType, fileName = '') {
	const normalized = (fileType || '').toLowerCase().trim()
	const normalizedName = (fileName || '').toLowerCase()
	let category = 'Other'
	let reason = 'fallback_other'

	if (!normalized) {
		reason = 'empty_mime_type'
	} else if (normalized.startsWith('image/')) {
		category = 'Photo'
		reason = 'mime_prefix_image'
	} else if (normalized.startsWith('video/')) {
		category = 'Video'
		reason = 'mime_prefix_video'
	} else if (normalized.startsWith('audio/')) {
		category = 'Audio'
		reason = 'mime_prefix_audio'
	} else if (
		normalized === 'application/pdf' ||
		normalized ===
			'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
		normalized === 'application/msword' ||
		normalized.startsWith('text/plain')
	) {
		category = 'Document'
		reason = 'document_mime_match'
	} else if (normalizedName.endsWith('.docx')) {
		category = 'Document'
		reason = 'filename_extension_docx_fallback'
	} else if (normalizedName.endsWith('.pdf')) {
		category = 'Document'
		reason = 'filename_extension_pdf_fallback'
	} else if (normalizedName.endsWith('.txt')) {
		category = 'Document'
		reason = 'filename_extension_txt_fallback'
	} else if (
		normalized.includes('zip') ||
		normalized.includes('rar') ||
		normalized.includes('7z')
	) {
		category = 'Archive'
		reason = 'archive_mime_match'
	}

	logCategoryDecision(fileType, fileName, normalized, category, reason)
	return category
}
