import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import ResultsModal from '../../src/lib/components/ResultsModal.svelte';

const results = {
	head: { vars: ['event'] },
	results: {
		bindings: [
			{
				event: { type: 'uri', value: 'https://sakuna.ph/test/event-1' },
			},
		],
	},
};

describe('ResultsModal', () => {
	it('announces results and closes from its labeled button', async () => {
		const onclose = vi.fn();
		render(ResultsModal, { results, onclose });

		expect(screen.getByRole('dialog')).toHaveAttribute('aria-modal', 'true');
		expect(screen.getByText('Query Results')).toBeVisible();
		expect(screen.getByText('event-1')).toHaveAttribute('title', 'https://sakuna.ph/test/event-1');

		await fireEvent.click(screen.getByRole('button', { name: 'Close results' }));
		expect(onclose).toHaveBeenCalledOnce();
	});
});
