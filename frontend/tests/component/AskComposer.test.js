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
		expect(screen.getByText(/Enter replaces the current question/)).toBeVisible();
		const cancelButton = screen.getByRole('button', { name: 'Stop answer' });
		expect(cancelButton).toBeEnabled();
		await fireEvent.click(cancelButton);
		expect(onCancel).toHaveBeenCalledOnce();
	});

	it('exposes the question length boundary to the browser', () => {
		render(AskComposer, { input: '', maxLength: 120 });
		expect(screen.getByRole('textbox', { name: 'Question' })).toHaveAttribute('maxlength', '120');
	});

	it('offers an accessible AI-query toggle', async () => {
		render(AskComposer, { input: 'Question' });
		await fireEvent.click(screen.getByText('Advanced query options'));
		const toggle = screen.getByRole('checkbox', { name: /Build a custom graph query with AI/ });

		expect(toggle).not.toBeChecked();
		await fireEvent.click(toggle);
		expect(toggle).toBeChecked();
		expect(screen.getByText(/automatic search misses/)).toBeVisible();
	});
});
