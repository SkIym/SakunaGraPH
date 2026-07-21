import { json } from '@sveltejs/kit';

export function GET() {
	return json(
		{ status: 'ok', service: 'sakunagraph-frontend' },
		{ headers: { 'cache-control': 'no-store' } },
	);
}
