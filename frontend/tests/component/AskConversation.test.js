import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import AskConversation from '../../src/lib/features/ask/components/AskConversation.svelte';

describe('AskConversation', () => {
	it('announces coarse stream status without making partial prose a live region', () => {
		render(AskConversation, {
			announcement: 'Generating answer.',
			messages: [{ role: 'assistant', text: 'Partial token output', streaming: true }],
		});

		const status = screen.getByRole('status');
		expect(status).toHaveTextContent('Generating answer.');
		expect(status).toHaveAttribute('aria-live', 'polite');
		expect(screen.getByText('Partial token output').closest('[aria-live]')).toBeNull();
	});
});
