import { describe, expect, it } from 'vitest';
import { CONSUMED_API_OPERATIONS } from '../../src/lib/api/contracts.js';
import snapshot from './openapi.snapshot.json';

const protectedAskPaths = ['/api/ask', '/api/ask/preview', '/api/ask/stream'];

describe('Stage 0 API inventory', () => {
	it('records every protected ask endpoint', () => {
		for (const path of protectedAskPaths) expect(snapshot.paths).toHaveProperty(path);
	});

	it('keeps the legacy ask fields required', () => {
		expect(snapshot.schemas.AskResponse.required).toEqual(
			expect.arrayContaining(['sparql', 'answer', 'rows']),
		);
	});

	it('records the analysis CSV download contract', () => {
		expect(snapshot.paths['/api/analysis/events/export.csv'].get.responses).toHaveProperty('200');
	});

	it('covers every operation exposed by the frontend API modules', () => {
		const operations = Object.values(CONSUMED_API_OPERATIONS);
		for (const operation of operations) {
			expect(snapshot.paths, operation.path).toHaveProperty(operation.path);
			expect(
				snapshot.paths[operation.path],
				`${operation.method.toUpperCase()} ${operation.path}`,
			).toHaveProperty(operation.method);
		}

		expect(new Set(operations.map(({ method, path }) => `${method} ${path}`)).size).toBe(
			operations.length,
		);
	});
});
