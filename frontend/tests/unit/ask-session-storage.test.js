import { describe, expect, it } from 'vitest';
import {
	ASK_WORKSPACE_STORAGE_KEY,
	createAskSession,
	exportSessionMarkdown,
	loadAskWorkspace,
	saveAskWorkspace,
	titleFromMessages,
} from '../../src/lib/features/ask/session-storage.js';

function memoryStorage() {
	const values = new Map();
	return {
		getItem: (key) => values.get(key) ?? null,
		setItem: (key, value) => values.set(key, value),
	};
}

describe('ask research session storage', () => {
	it('derives a concise title from the first question', () => {
		expect(titleFromMessages([{ role: 'user', text: 'Which provinces recorded flooding?' }])).toBe(
			'Which provinces recorded flooding?',
		);
	});

	it('saves and restores completed research while converting in-flight answers to stopped', () => {
		const storage = memoryStorage();
		const session = createAskSession({
			messages: [
				{ role: 'user', text: 'Question' },
				{ role: 'assistant', loading: true, text: 'Partial' },
			],
		});

		saveAskWorkspace([session], session.id, storage);
		const restored = loadAskWorkspace(storage);

		expect(storage.getItem(ASK_WORKSPACE_STORAGE_KEY)).toContain('Question');
		expect(restored.activeSessionId).toBe(session.id);
		expect(restored.sessions[0].messages[1]).toMatchObject({
			loading: false,
			streaming: false,
			cancelled: true,
			text: 'Partial',
		});
	});

	it('exports questions, answers, sources, and graph queries as Markdown', () => {
		const markdown = exportSessionMarkdown(
			createAskSession({
				title: 'Flood research',
				messages: [
					{ role: 'user', text: 'Where were floods recorded?' },
					{
						role: 'assistant',
						text: 'Three provinces matched.',
						sparql: 'SELECT * WHERE {}',
						citations: [{ label: 'Situation report', uri: 'https://example.test/report' }],
					},
				],
			}),
		);

		expect(markdown).toContain('# Flood research');
		expect(markdown).toContain('Three provinces matched.');
		expect(markdown).toContain('Situation report');
		expect(markdown).toContain('```sparql');
	});
});
