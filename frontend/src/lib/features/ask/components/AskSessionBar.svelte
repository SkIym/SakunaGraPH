<script>
	let {
		sessions = [],
		activeSessionId = null,
		savedLabel = 'Saved on this device',
		onNew = () => {},
		onSelect = () => {},
		onDelete = () => {},
		onExport = () => {},
	} = $props();

	let sessionMenu = $state(null);
	let activeSession = $derived(sessions.find((session) => session.id === activeSessionId));

	function selectSession(id) {
		onSelect(id);
		if (sessionMenu) sessionMenu.open = false;
	}

	function createNewSession() {
		onNew();
		if (sessionMenu) sessionMenu.open = false;
	}

	function exportSession() {
		onExport();
		if (sessionMenu) sessionMenu.open = false;
	}
</script>

<header class="workspace-bar" aria-label="Research workspace">
	<div class="workspace-identity">
		<strong>{activeSession?.title ?? 'New research'}</strong>
		<span>{savedLabel}</span>
	</div>
	<div class="workspace-actions">
		<details bind:this={sessionMenu} class="session-menu">
			<summary>Research <span>{sessions.length}</span></summary>
			<div class="session-panel">
				<header>
					<div>
						<strong>Saved research</strong>
						<p>Stored only in this browser.</p>
					</div>
					<div class="panel-actions">
						<button
							type="button"
							disabled={!activeSession?.messages?.length}
							onclick={exportSession}
						>
							Export current
						</button>
						<button type="button" class="new-action" onclick={createNewSession}>New research</button
						>
					</div>
				</header>
				<ul>
					{#each sessions as session (session.id)}
						<li class:active={session.id === activeSessionId}>
							<button
								type="button"
								class="session-select"
								onclick={() => selectSession(session.id)}
							>
								<span>{session.title}</span>
								<small
									>{new Intl.DateTimeFormat('en-PH', { dateStyle: 'medium' }).format(
										new Date(session.updatedAt),
									)}</small
								>
							</button>
							<button
								type="button"
								class="session-delete"
								aria-label={`Delete ${session.title}`}
								onclick={() => onDelete(session.id)}
							>
								<svg
									aria-hidden="true"
									viewBox="0 0 24 24"
									fill="none"
									stroke="currentColor"
									stroke-width="1.8"
								>
									<path d="M4 7h16M9 7V4h6v3m-8 0 1 13h8l1-13M10 11v5m4-5v5" />
								</svg>
							</button>
						</li>
					{/each}
				</ul>
			</div>
		</details>
	</div>
</header>

<style>
	.workspace-bar {
		position: relative;
		z-index: 3;
		display: flex;
		min-height: 3.75rem;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		border-block-end: 1px solid var(--color-border);
		background: var(--color-canvas);
		padding: 0.55rem clamp(1rem, 4vw, 2.5rem);
	}

	.workspace-identity {
		display: grid;
		min-width: 0;
		gap: 0.1rem;
	}

	.workspace-identity strong {
		overflow: hidden;
		font-size: 0.78rem;
		line-height: 1.35;
		text-overflow: ellipsis;
		white-space: nowrap;
		color: var(--color-text);
	}

	.workspace-identity span {
		font-size: 0.75rem;
		color: var(--color-text-secondary);
	}

	.workspace-actions {
		display: flex;
		flex: none;
		align-items: center;
		gap: 0.4rem;
	}

	.workspace-actions button,
	.session-menu summary {
		display: inline-flex;
		min-height: 2.75rem;
		align-items: center;
		justify-content: center;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding-inline: 0.8rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-text-secondary);
	}

	.workspace-actions button:hover:not(:disabled),
	.session-menu summary:hover {
		border-color: var(--color-brand-medium);
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.workspace-actions button:disabled {
		cursor: not-allowed;
		opacity: 0.5;
	}

	.panel-actions .new-action {
		border-color: var(--color-text);
		background: var(--color-text);
		color: var(--color-canvas);
	}

	.session-menu {
		position: relative;
	}

	.session-menu summary {
		cursor: pointer;
		list-style: none;
	}

	.session-menu summary::-webkit-details-marker {
		display: none;
	}

	.session-menu summary span {
		margin-inline-start: 0.45rem;
		font-family: var(--font-mono);
		font-size: 0.75rem;
		color: var(--color-brand);
	}

	.session-panel {
		position: absolute;
		inset-block-start: calc(100% + 0.45rem);
		inset-inline-end: 0;
		width: min(22rem, calc(100vw - 2rem));
		border: 1px solid var(--color-border);
		border-radius: var(--radius-surface);
		background: var(--color-canvas);
		padding: 1rem;
		box-shadow: var(--shadow-surface);
	}

	.session-panel > header {
		display: flex;
		align-items: start;
		justify-content: space-between;
		gap: 1rem;
		margin-bottom: 0.75rem;
	}

	.session-panel header strong {
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1rem;
		color: var(--color-text);
	}

	.session-panel header p {
		margin: 0.2rem 0 0;
		font-size: 0.75rem;
		color: var(--color-text-secondary);
	}

	.panel-actions {
		display: flex;
		flex: none;
		gap: 0.35rem;
	}

	.session-panel ul {
		display: grid;
		max-height: min(23rem, 55vh);
		overflow-y: auto;
		gap: 0.35rem;
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.session-panel li {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		align-items: center;
		border: 1px solid transparent;
		border-radius: var(--radius-control);
	}

	.session-panel li.active {
		border-color: #c29f00;
		background: var(--color-accent-soft);
	}

	.session-panel .session-select {
		display: grid;
		min-width: 0;
		justify-items: start;
		border: 0;
		background: transparent;
		text-align: start;
	}

	.session-select span {
		width: 100%;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.session-select small {
		font-size: 0.75rem;
		font-weight: 400;
		color: var(--color-text-secondary);
	}

	.session-panel .session-delete {
		width: 2.75rem;
		padding: 0;
		border: 0;
		background: transparent;
	}

	.session-delete svg {
		width: 1rem;
		height: 1rem;
	}

	@media (max-width: 42rem) {
		.workspace-bar {
			min-height: 3.5rem;
			gap: 0.75rem;
			padding-block: 0.4rem;
		}

		.workspace-actions {
			display: block;
		}

		.session-menu > summary {
			width: 100%;
		}

		.session-panel {
			inset-inline: auto 0;
		}
	}

	@media (max-height: 44rem) and (max-width: 42rem) {
		.workspace-identity span {
			display: none;
		}
	}

	@media (max-width: 28rem) {
		.session-panel > header {
			flex-direction: column;
		}

		.panel-actions {
			width: 100%;
		}

		.panel-actions button {
			flex: 1;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		* {
			scroll-behavior: auto;
		}
	}

	@media (forced-colors: active) {
		.session-panel li.active {
			border: 2px solid Highlight;
		}
	}
</style>
