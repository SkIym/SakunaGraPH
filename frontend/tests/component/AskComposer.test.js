import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import AskComposer from '../../src/lib/features/ask/components/AskComposer.svelte';

describe('AskComposer', () => {
	it('submits with Enter and preserves Shift+Enter', async () => {
		const onSend = vi.fn();
		render(AskComposer, { input: 'Question', onSend });
		const textarea = screen.getByRole('textbox', { name: 'Question' });

		await fireEvent.keyDown(textarea, { key: 'Enter', shiftKey: true });
		expect(onSend).not.toHaveBeenCalled();

		await fireEvent.keyDown(textarea, { key: 'Enter' });
		expect(onSend).toHaveBeenCalledOnce();
	});

	it('offers cancellation while sending', async () => {
		const onCancel = vi.fn();
		render(AskComposer, { input: 'Question', sending: true, onCancel });
		expect(screen.getByRole('textbox', { name: 'Question' })).toBeEnabled();
		expect(screen.getByText(/Enter to replace the active request/)).toBeVisible();
		const cancelButton = screen.getByRole('button', { name: 'Cancel' });
		expect(cancelButton).toBeEnabled();
		await fireEvent.click(cancelButton);
		expect(onCancel).toHaveBeenCalledOnce();
	});
});
