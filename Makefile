PLUGIN_VERSION=0.0.3
PLUGIN_ID=connections-audit

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip python-runnables plugin.json 
