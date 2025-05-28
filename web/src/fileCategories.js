export function getCategoryByFileType(fileType) {
	// Можно определить по MIME:
	// image/png -> "Photo"
	// video/mp4 -> "Video"
	// audio/mpeg -> "Audio"
	// application/zip -> "Archive"
	// ...

	// Упростим
	if (fileType.startsWith('image/')) {
		return 'Photo'
	} else if (fileType.startsWith('video/')) {
		return 'Video'
	} else if (fileType.startsWith('audio/')) {
		return 'Audio'
	} else if (
		fileType.includes('zip') ||
		fileType.includes('rar') ||
		fileType.includes('7z')
	) {
		return 'Archive'
	} else {
		return 'Other'
	}
}
