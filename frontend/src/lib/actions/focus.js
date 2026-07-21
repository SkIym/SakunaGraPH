const FOCUSABLE_SELECTOR = [
	'a[href]',
	'button:not([disabled])',
	'input:not([disabled]):not([type="hidden"])',
	'select:not([disabled])',
	'textarea:not([disabled])',
	'[tabindex]:not([tabindex="-1"])',
].join(',');

function visibleFocusables(node) {
	return [...node.querySelectorAll(FOCUSABLE_SELECTOR)].filter((element) => {
		if (!(element instanceof HTMLElement)) return false;
		return element.getAttribute('aria-hidden') !== 'true' && element.getClientRects().length > 0;
	});
}

function focusInitial(node, selector) {
	const preferred = selector ? node.querySelector(selector) : null;
	const target = preferred ?? visibleFocusables(node)[0] ?? node;
	if (target instanceof HTMLElement) target.focus({ preventScroll: true });
}

function restore(previousFocus) {
	if (!previousFocus?.isConnected) return;
	previousFocus.focus({ preventScroll: true });
	globalThis.requestAnimationFrame?.(() => {
		if (previousFocus.isConnected && document.activeElement === document.body) {
			previousFocus.focus({ preventScroll: true });
		}
	});
}

/**
 * Keeps keyboard focus inside a modal surface and restores it to the control
 * that opened the surface when the component is destroyed.
 */
export function focusTrap(node, options = {}) {
	let settings = {
		initialFocus: '[data-focus-first]',
		restoreFocus: true,
		returnFocus: null,
		...options,
	};
	const previousFocus =
		document.activeElement instanceof HTMLElement ? document.activeElement : null;
	let disposed = false;

	function handleKeydown(event) {
		if (event.key !== 'Tab') return;
		const focusables = visibleFocusables(node);
		if (focusables.length === 0) {
			event.preventDefault();
			node.focus({ preventScroll: true });
			return;
		}

		const first = focusables[0];
		const last = focusables.at(-1);
		if (
			event.shiftKey &&
			(document.activeElement === first || !node.contains(document.activeElement))
		) {
			event.preventDefault();
			last.focus();
		} else if (!event.shiftKey && document.activeElement === last) {
			event.preventDefault();
			first.focus();
		}
	}

	node.addEventListener('keydown', handleKeydown);
	queueMicrotask(() => {
		if (!disposed) focusInitial(node, settings.initialFocus);
	});

	return {
		update(nextOptions = {}) {
			settings = { ...settings, ...nextOptions };
		},
		destroy() {
			disposed = true;
			node.removeEventListener('keydown', handleKeydown);
			const target = settings.returnFocus ?? previousFocus;
			if (settings.restoreFocus && target?.isConnected) {
				restore(target);
			}
		},
	};
}

/** Focuses a non-modal drawer on entry and restores the initiating control. */
export function manageDrawerFocus(node, options = {}) {
	const previousFocus =
		document.activeElement instanceof HTMLElement ? document.activeElement : null;
	const settings = { initialFocus: '[data-focus-first]', restoreFocus: true, ...options };
	let disposed = false;

	queueMicrotask(() => {
		if (!disposed) focusInitial(node, settings.initialFocus);
	});

	return {
		destroy() {
			disposed = true;
			if (settings.restoreFocus && previousFocus?.isConnected) {
				restore(previousFocus);
			}
		},
	};
}
