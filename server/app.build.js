({
	appDir: "site",
	baseUrl: ".",
	dir: "site-build",
	paths: {
		"components": "../components",
	},
	modules: [
		{
			name: "scripts/main",
		}
	],
	fileExclusionRegExp: /(^\.)|(^components$)/
})
