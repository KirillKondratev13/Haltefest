/** @type {import('tailwindcss').Config} */
module.exports = {
	content: ['../internal/view/**/*.templ', './src/**/*.html', './src/**/*.js'],
	theme: {
		extend: {},
	},
	plugins: [require('daisyui')],
}
