({
	appDir: "site",
	baseUrl: ".",
	dir: "site-build",
	paths: {
		"components": "../components",
		"app": "scripts/app"
	},
	modules: [
		{
			name: "scripts/main",
		}
	],
	optimize: "none",
})
