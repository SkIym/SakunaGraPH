<script>
	import { onMount } from 'svelte';

	let { value = $bindable('') } = $props();

	let editorEl;
	let view = null;

	// Sync external value changes (e.g. reset) into the editor
	$effect(() => {
		if (!view) return;
		const current = view.state.doc.toString();
		if (value !== current) {
			view.dispatch({
				changes: { from: 0, to: current.length, insert: value },
			});
		}
	});

	onMount(async () => {
		const { EditorView, basicSetup } = await import('codemirror');
		const { EditorState } = await import('@codemirror/state');
		const { StreamLanguage, HighlightStyle, syntaxHighlighting } =
			await import('@codemirror/language');
		const { tags: t } = await import('@lezer/highlight');
		const { sparql } = await import('@codemirror/legacy-modes/mode/sparql');

		// SPARQL-tuned syntax colours
		const sparqlHighlight = HighlightStyle.define([
			{ tag: t.keyword, color: '#a21caf', fontWeight: '600' }, // SELECT, WHERE, PREFIX …
			{ tag: t.variableName, color: '#6b7280' }, // ?event, ?type …
			{ tag: t.atom, color: '#6b7280' }, // :DisasterEvent, IRIs
			{ tag: t.name, color: '#6b7280' }, // FILTER, REGEX, builtins
			{ tag: t.string, color: '#6b7280' }, // "literals"
			{ tag: t.number, color: '#6b7280' },
			{ tag: t.operator, color: '#6b7280' },
			{ tag: t.punctuation, color: '#6b7280' }, // ; . , { }
			{ tag: t.comment, color: '#6b7280', fontStyle: 'italic' },
		]);

		// Visual chrome theme (borders, padding, font)
		const uiTheme = EditorView.theme({
			'&': {
				background: '#ffffff',
				color: '#4b5563',
				fontSize: '13.5px',
				height: '260px',
			},
			'.cm-scroller': {
				overflow: 'auto',
				height: '260px',
			},
			'.cm-content': {
				padding: '14px 16px',
				minHeight: '260px',
				fontFamily: '"JetBrains Mono","Fira Code","Courier New",monospace',
				caretColor: '#a21caf',
				lineHeight: '1.7',
			},
			'.cm-focused': { outline: 'none' },
			'.cm-gutters': {
				background: '#f8fafc',
				border: 'none',
				borderRight: '1px solid #e2e8f0',
				color: '#64748b',
				fontSize: '12px',
				paddingRight: '6px',
			},
			'.cm-activeLineGutter': { background: '#f0f4ff' },
			'.cm-activeLine': { background: '#f8f9ff' },
			'.cm-selectionBackground, ::selection': { background: '#fce7f3 !important' },
			'.cm-cursor': { borderLeftColor: '#a21caf' },
			'.cm-matchingBracket': {
				background: '#fce7f3',
				borderRadius: '2px',
				outline: '1px solid #fbcfe8',
			},
			'.cm-tooltip': {
				border: '1px solid #e2e8f0',
				borderRadius: '8px',
				boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
			},
		});

		view = new EditorView({
			parent: editorEl,
			state: EditorState.create({
				doc: value,
				extensions: [
					basicSetup,
					StreamLanguage.define(sparql),
					syntaxHighlighting(sparqlHighlight),
					uiTheme,
					EditorView.contentAttributes.of({
						'aria-label': 'SPARQL query editor',
					}),
					EditorView.updateListener.of((update) => {
						if (update.docChanged) {
							value = update.state.doc.toString();
						}
					}),
				],
			}),
		});
		view.scrollDOM.tabIndex = 0;
		view.scrollDOM.setAttribute('aria-label', 'Scrollable SPARQL query editor');

		return () => {
			view?.destroy();
			view = null;
		};
	});
</script>

<div
	bind:this={editorEl}
	class="w-full rounded-b-xl overflow-hidden transition-shadow duration-200"
></div>
