import { afterEach, describe, expect, it, vi } from 'vitest';
import { focusTrap, manageDrawerFocus } from '../../src/lib/actions/focus.js';

function makeVisible(element) {
	vi.spyOn(element, 'getClientRects').mockReturnValue([{ width: 20, height: 20 }]);
	return element;
}

afterEach(() => {
	document.body.innerHTML = '';
	vi.restoreAllMocks();
});

describe('focus management', () => {
	it('traps tab navigation and restores the opener', async () => {
		document.body.innerHTML = `
			<button id="open">Open</button>
			<div id="dialog" tabindex="-1">
				<button id="first" data-focus-first>First</button>
				<button id="last">Last</button>
			</div>`;
		const opener = document.querySelector('#open');
		const dialog = document.querySelector('#dialog');
		const first = makeVisible(document.querySelector('#first'));
		const last = makeVisible(document.querySelector('#last'));
		opener.focus();

		const action = focusTrap(dialog);
		await Promise.resolve();
		expect(first).toHaveFocus();

		last.focus();
		last.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab', bubbles: true }));
		expect(first).toHaveFocus();

		action.destroy();
		await Promise.resolve();
		expect(opener).toHaveFocus();
	});

	it('focuses a drawer without trapping subsequent tab navigation', async () => {
		document.body.innerHTML = `
			<button id="open">Open</button>
			<aside id="drawer"><button id="close" data-focus-first>Close</button></aside>`;
		const opener = document.querySelector('#open');
		const drawer = document.querySelector('#drawer');
		const close = makeVisible(document.querySelector('#close'));
		opener.focus();

		const action = manageDrawerFocus(drawer);
		await Promise.resolve();
		expect(close).toHaveFocus();
		action.destroy();
		await Promise.resolve();
		expect(opener).toHaveFocus();
	});
});
