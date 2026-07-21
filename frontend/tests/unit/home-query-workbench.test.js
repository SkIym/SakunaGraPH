import { describe, expect, it, vi } from 'vitest';
import {
	createQueryWorkbench,
	isWriteOperation,
} from '../../src/lib/features/home/queryWorkbench.svelte.js';

describe('landing query workbench', () => {
	it('blocks write operations before transport', async () => {
		const execute = vi.fn();
		const workbench = createQueryWorkbench({ execute });
		workbench.query = 'DELETE WHERE { ?s ?p ?o }';

		await workbench.run();

		expect(isWriteOperation(workbench.query)).toBe(true);
		expect(execute).not.toHaveBeenCalled();
		expect(workbench.error).toMatch(/Write operations/);
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
