import { describe, expect, it, vi } from 'vitest';
import {
	createQueryWorkbench,
	isSelectQuery,
	isWriteOperation,
	MAX_QUERY_LENGTH,
} from '../../src/lib/features/home/queryWorkbench.svelte.js';

describe('landing query workbench', () => {
	it('blocks write operations before transport', async () => {
		const execute = vi.fn();
		const workbench = createQueryWorkbench({ execute });
		workbench.query = 'DELETE WHERE { ?s ?p ?o }';

		await workbench.run();

		expect(isWriteOperation(workbench.query)).toBe(true);
		expect(execute).not.toHaveBeenCalled();
		expect(workbench.error).toMatch(/read-only/);
	});

	it('accepts SELECT queries after PREFIX declarations and rejects other read forms', async () => {
		expect(isSelectQuery('PREFIX : <https://sakuna.ph/>\nSELECT * WHERE { ?s ?p ?o }')).toBe(true);
		expect(
			isSelectQuery('PREFIX owl: <http://www.w3.org/2002/07/owl#>\nSELECT * WHERE { ?s ?p ?o }'),
		).toBe(true);
		expect(isSelectQuery('ASK WHERE { ?s ?p ?o }')).toBe(false);

		const execute = vi.fn();
		const workbench = createQueryWorkbench({ execute });
		workbench.query = 'CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }';

		await workbench.run();

		expect(execute).not.toHaveBeenCalled();
		expect(workbench.error).toMatch(/SELECT/);
	});

	it('blocks oversized queries before transport', async () => {
		const execute = vi.fn();
		const workbench = createQueryWorkbench({ execute });
		workbench.query = `SELECT * WHERE { ?s ?p ?o } # ${'x'.repeat(MAX_QUERY_LENGTH)}`;

		await workbench.run();

		expect(execute).not.toHaveBeenCalled();
		expect(workbench.error).toMatch(/too long/);
	});

	it('ignores concurrent runs while a request is active', async () => {
		let resolve;
		const execute = vi.fn(
			() =>
				new Promise((done) => {
					resolve = done;
				}),
		);
		const workbench = createQueryWorkbench({ execute });

		const first = workbench.run();
		const second = workbench.run();
		expect(execute).toHaveBeenCalledTimes(1);
		resolve({ head: { vars: [] }, results: { bindings: [] } });
		await Promise.all([first, second]);
	});

	it('preserves the SPARQL result shape and opens the modal', async () => {
		const response = { head: { vars: ['event'] }, results: { bindings: [] } };
		const execute = vi.fn().mockResolvedValue(response);
		const workbench = createQueryWorkbench({ execute });

		await workbench.run();

		expect(execute).toHaveBeenCalledWith(workbench.query.trim(), {
			signal: expect.any(AbortSignal),
		});
		expect(workbench.results).toEqual(response);
		expect(workbench.resultsOpen).toBe(true);
		workbench.closeResults();
		expect(workbench.resultsOpen).toBe(false);
	});
});
