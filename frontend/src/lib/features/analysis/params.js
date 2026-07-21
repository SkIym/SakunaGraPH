export function mergeAnalysisParams(filters = '', extras = {}) {
	const params = new URLSearchParams(filters);
	for (const [key, value] of Object.entries(extras)) {
		if (value === undefined || value === null || value === '') continue;
		params.set(key, String(value));
	}
	return params;
}
