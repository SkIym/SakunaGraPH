<script>
	import { onDestroy, onMount, tick } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import AskComposer from './components/AskComposer.svelte';
	import AskConversation from './components/AskConversation.svelte';
	import AskSessionBar from './components/AskSessionBar.svelte';
	import {
		createAskSession,
		exportSessionMarkdown,
		loadAskWorkspace,
		saveAskWorkspace,
		titleFromMessages,
	} from './session-storage.js';
	import {
		ASK_FOLLOW_UPS,
		ASK_QUESTION_MAX_LENGTH,
		ASK_SUGGESTIONS,
		createAskState,
	} from './state.svelte.js';

	let bottomElement = $state(null);
	let sessions = $state([]);
	let activeSessionId = $state(null);
	let savedLabel = $state('Preparing saved research…');
	let scrollFrame = null;
	let persistTimer = null;
	let hydrated = false;

	function activeSession() {
		return sessions.find((session) => session.id === activeSessionId) ?? null;
	}

	function persistNow() {
		if (!hydrated || !activeSessionId) return;
		if (persistTimer !== null) {
			window.clearTimeout(persistTimer);
			persistTimer = null;
		}
		const snapshot = ask.snapshot();
		const updatedAt = new Date().toISOString();
		sessions = sessions.map((session) =>
			session.id === activeSessionId
				? {
						...session,
						title: titleFromMessages(snapshot.messages),
						updatedAt,
						messages: snapshot.messages,
						draft: snapshot.draft,
						forceLlmQuery: snapshot.forceLlmQuery,
					}
				: session,
		);
		try {
			sessions = saveAskWorkspace(sessions, activeSessionId);
			savedLabel = 'Saved on this device';
		} catch {
			savedLabel = 'Could not save in this browser';
		}
	}

	function schedulePersist() {
		if (!hydrated) return;
		savedLabel = 'Saving…';
		if (persistTimer !== null) window.clearTimeout(persistTimer);
		persistTimer = window.setTimeout(persistNow, 220);
	}

	const ask = createAskState({
		onChanged: schedulePersist,
		onUpdated: async () => {
			await tick();
			if (scrollFrame !== null) return;
			scrollFrame = window.requestAnimationFrame(() => {
				scrollFrame = null;
				const scroller = document.getElementById('messages-scroll');
				if (!scroller || !bottomElement) return;
				const distanceFromBottom =
					scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight;
				if (distanceFromBottom <= 240) {
					scroller.scrollTo({ top: scroller.scrollHeight, behavior: 'auto' });
				}
			});
		},
	});

	function startNewSession() {
		persistNow();
		const session = createAskSession();
		sessions = [session, ...sessions];
		activeSessionId = session.id;
		ask.reset();
		persistNow();
	}

	function selectSession(id) {
		if (id === activeSessionId) return;
		persistNow();
		const session = sessions.find((item) => item.id === id);
		if (!session) return;
		activeSessionId = session.id;
		ask.restore(session);
		savedLabel = 'Saved on this device';
		void tick().then(() => {
			const scroller = document.getElementById('messages-scroll');
			if (scroller) scroller.scrollTop = scroller.scrollHeight;
		});
	}

	function deleteSession(id) {
		const session = sessions.find((item) => item.id === id);
		if (!session) return;
		if (!window.confirm(`Delete "${session.title}" from this browser? This cannot be undone.`))
			return;
		sessions = sessions.filter((item) => item.id !== id);
		if (id === activeSessionId) {
			const next = sessions[0] ?? createAskSession();
			if (!sessions.length) sessions = [next];
			activeSessionId = next.id;
			ask.restore(next);
		}
		persistNow();
		ask.announce('Saved research deleted.');
	}

	async function copyText(text, label) {
		try {
			await navigator.clipboard.writeText(text);
			ask.announce(`${label} copied.`);
		} catch {
			ask.announce(`Could not copy ${label.toLowerCase()}. Select the text and copy it manually.`);
		}
	}

	function exportCurrentSession() {
		persistNow();
		const session = activeSession();
		if (!session?.messages.length) return;
		const blob = new Blob([exportSessionMarkdown(session)], {
			type: 'text/markdown;charset=utf-8',
		});
		const url = URL.createObjectURL(blob);
		const link = document.createElement('a');
		link.href = url;
		link.download = `${
			session.title
				.replace(/[^a-z0-9]+/gi, '-')
				.replace(/^-|-$/g, '')
				.toLowerCase() || 'sakunagraph-research'
		}.md`;
		link.click();
		URL.revokeObjectURL(url);
		ask.announce('Research exported as a Markdown file.');
	}

	onMount(() => {
		const restored = loadAskWorkspace();
		const initial =
			restored.sessions.find((session) => session.id === restored.activeSessionId) ??
			restored.sessions[0] ??
			createAskSession();
		sessions = restored.sessions.length ? restored.sessions : [initial];
		activeSessionId = initial.id;
		ask.restore(initial);
		hydrated = true;
		persistNow();
	});

	onDestroy(() => {
		persistNow();
		ask.cancel();
		if (scrollFrame !== null) window.cancelAnimationFrame(scrollFrame);
		if (persistTimer !== null) window.clearTimeout(persistTimer);
	});
</script>

<NodeCanvas active={ask.sending} />

<main class="ask-workspace ask-viewport">
	<div class="ask-frame">
		<AskSessionBar
			{sessions}
			{activeSessionId}
			{savedLabel}
			onNew={startNewSession}
			onSelect={selectSession}
			onDelete={deleteSession}
			onExport={exportCurrentSession}
		/>
		<AskConversation
			messages={ask.messages}
			suggestions={ASK_SUGGESTIONS}
			followUps={ASK_FOLLOW_UPS}
			announcement={ask.announcement}
			onSend={ask.send}
			onCopy={copyText}
			bind:bottomElement
		/>
		<AskComposer
			bind:input={ask.input}
			bind:forceLlmQuery={ask.forceLlmQuery}
			sending={ask.sending}
			error={ask.inputError}
			maxLength={ASK_QUESTION_MAX_LENGTH}
			onSend={ask.send}
			onCancel={ask.cancel}
		/>
	</div>
</main>

<style>
	.ask-workspace {
		position: relative;
		z-index: 1;
		display: grid;
		min-height: 0;
		place-items: stretch center;
		overflow: hidden;
		background: rgb(248 250 252 / 0.34);
	}

	.ask-frame {
		display: flex;
		width: min(100%, 72rem);
		height: 100%;
		min-height: 0;
		flex-direction: column;
		border-inline: 1px solid rgb(226 232 240 / 0.84);
		background: rgb(255 255 255 / 0.78);
		box-shadow: 0 18px 50px -40px rgb(0 56 168 / 0.45);
	}

	@media (max-width: 72rem) {
		.ask-frame {
			border-inline: 0;
			box-shadow: none;
		}
	}

	@media (forced-colors: active) {
		.ask-workspace,
		.ask-frame {
			background: Canvas;
		}
	}
</style>
