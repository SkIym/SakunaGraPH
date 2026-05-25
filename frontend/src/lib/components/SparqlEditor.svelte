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
				changes: { from: 0, to: current.length, insert: value }
			});
		}
	});

	onMount(async () => {
		const { EditorView, basicSetup } = await import('codemirror');
		const { EditorState } = await import('@codemirror/state');
		const { StreamLanguage, HighlightStyle, syntaxHighlighting } = await import(
			'@codemirror/language'
		);
		const { tags: t } = await import('@lezer/highlight');
		const { sparql } = await import('@codemirror/legacy-modes/mode/sparql');

		// SPARQL-tuned syntax colours
		const sparqlHighlight = HighlightStyle.define([
			{ tag: t.keyword, color: '#6d28d9', fontWeight: '600' },
			{ tag: t.variableName, color: '#0369a1' },
			{ tag: t.string, color: '#15803d' },
			{ tag: t.comment, color: '#94a3b8', fontStyle: 'italic' },
			{ tag: t.number, color: '#b45309' },
			{ tag: t.atom, color: '#0e7490' }, // IRIs <...>
			{ tag: t.operator, color: '#6366f1' },
			{ tag: t.punctuation, color: '#64748b' },
			{ tag: t.name, color: '#0284c7' } // builtins like FILTER, REGEX
		]);

		// Visual chrome theme (borders, padding, font)
		const uiTheme = EditorView.theme({
			'&': {
				background: '#ffffff',
				color: '#1e293b',
				fontSize: '13.5px'
			},
			'.cm-content': {
				padding: '14px 16px',
				minHeight: '200px',
				fontFamily: '"JetBrains Mono","Fira Code","Courier New",monospace',
				caretColor: '#6366f1',
				lineHeight: '1.7'
			},
			'.cm-focused': { outline: 'none' },
			'.cm-gutters': {
				background: '#f8fafc',
				border: 'none',
				borderRight: '1px solid #e2e8f0',
				color: '#94a3b8',
				fontSize: '12px',
				paddingRight: '6px'
			},
			'.cm-activeLineGutter': { background: '#f0f4ff' },
			'.cm-activeLine': { background: '#f8f9ff' },
			'.cm-selectionBackground, ::selection': { background: '#ddd6fe !important' },
			'.cm-cursor': { borderLeftColor: '#6366f1' },
			'.cm-matchingBracket': {
				background: '#e0e7ff',
				borderRadius: '2px',
				outline: '1px solid #a5b4fc'
			},
			'.cm-tooltip': {
				border: '1px solid #e2e8f0',
				borderRadius: '8px',
				boxShadow: '0 4px 16px rgba(0,0,0,0.08)'
			}
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
					EditorView.updateListener.of((update) => {
						if (update.docChanged) {
							value = update.state.doc.toString();
						}
					})
				]
			})
		});

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
