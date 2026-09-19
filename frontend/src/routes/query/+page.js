export function load({ url }) {
	return {
		competencyId: url.searchParams.get('cq') ?? '',
	};
}
