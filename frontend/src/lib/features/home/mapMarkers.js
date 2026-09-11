import { normalizePsgcCode } from '$lib/mapData.js';

export const HOME_RECORD_MARKER_LIMIT = 5;

function regionIdCandidates(value) {
	const raw = String(value ?? '').trim();
	if (!raw) return [];
	const candidates = new Set([raw, normalizePsgcCode(raw)]);
	if (/^\d+$/.test(raw) && raw.length < 10) candidates.add(raw.padEnd(10, '0'));
	return [...candidates];
}

function matchingRegionId(value, availableIds) {
	return regionIdCandidates(value).find((candidate) => availableIds.has(candidate)) ?? null;
}

export function buildHomeRecordMarkers({
	rankings = [],
	pathData = [],
	pathGenerator = null,
	limit = HOME_RECORD_MARKER_LIMIT,
} = {}) {
	if (!pathGenerator?.centroid || !Array.isArray(rankings) || !Array.isArray(pathData)) {
		return [];
	}

	const availableIds = new Set(pathData.map((item) => item.regionPsgc).filter(Boolean));
	const resolved = rankings
		.map((ranking) => ({
			...ranking,
			count: Number(ranking?.count ?? 0),
			regionId: matchingRegionId(ranking?.id, availableIds),
		}))
		.filter((ranking) => ranking.regionId && Number.isFinite(ranking.count) && ranking.count > 0)
		.sort((a, b) => b.count - a.count)
		.slice(0, limit);

	const maxCount = Math.max(...resolved.map((ranking) => ranking.count), 1);
	return resolved.flatMap((ranking) => {
		const features = pathData
			.filter((item) => item.regionPsgc === ranking.regionId)
			.map((item) => item.feature)
			.filter(Boolean);
		if (features.length === 0) return [];

		let point;
		try {
			point = pathGenerator.centroid({ type: 'FeatureCollection', features });
		} catch {
			return [];
		}
		if (!point?.every(Number.isFinite)) return [];

		return [
			{
				id: ranking.regionId,
				label: ranking.label || ranking.regionId,
				count: ranking.count,
				x: point[0],
				y: point[1],
				radius: 4.5 + 3.5 * Math.sqrt(ranking.count / maxCount),
			},
		];
	});
}
