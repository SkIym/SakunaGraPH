import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import AskAnswerMeta from '../../src/lib/features/ask/components/AskAnswerMeta.svelte';

describe('AskAnswerMeta', () => {
	it('renders retrieval provenance and citation links separately from answer prose', async () => {
		render(AskAnswerMeta, {
			retrieval: { mode: 'graphrag', sourceCount: 1, indexVersion: '2026-07' },
			citations: [
				{
					id: 'source-1',
					label: 'NDRRMC Situation Report',
					uri: 'https://example.test/report',
					excerpt: 'A bounded source excerpt.',
					sourceRecord: 'https://sakuna.ph/source/report-1',
				},
			],
		});

		await fireEvent.click(screen.getByText('How this answer was made'));
		expect(screen.getByText('Matched records from the knowledge graph')).toBeVisible();
		expect(screen.getByText(/1 source/)).toBeVisible();
		expect(screen.getByText(/Data index 2026-07/)).toBeVisible();
		expect(screen.getByRole('region', { name: 'Answer sources' })).toBeVisible();
		expect(screen.getByRole('link', { name: /NDRRMC Situation Report/ })).toHaveAttribute(
			'href',
			'https://example.test/report',
		);
	});

	it('does not create an active link for an unsafe citation URI', () => {
		render(AskAnswerMeta, {
			citations: [{ id: 'unsafe', label: 'Unsafe source', uri: 'javascript:alert(1)' }],
		});

		expect(screen.queryByRole('link', { name: 'Unsafe source' })).not.toBeInTheDocument();
		expect(screen.getByText('Unsafe source')).toBeVisible();
	});

	it.each([
		['service', 'deterministic', 'Used a prepared lookup', 'Answer calculated from the results'],
		['compiler', 'llm', 'Built a read-only graph lookup', 'Answer summarized from the results'],
		[
			'model_fallback',
			'llm',
			'Built a read-only lookup with AI',
			'Answer summarized from the results',
		],
	])(
		'labels the %s query and %s answer methods',
		async (query, answer, queryLabel, answerLabel) => {
			render(AskAnswerMeta, {
				method: { planning: 'llm', query, answer },
			});

			await fireEvent.click(screen.getByText('How this answer was made'));
			expect(screen.getByRole('region', { name: 'Answer method' })).toBeVisible();
			expect(screen.getByText('Question interpreted')).toBeVisible();
			expect(screen.getByText(queryLabel)).toBeVisible();
			expect(screen.getByText(answerLabel)).toBeVisible();
		},
	);
});
