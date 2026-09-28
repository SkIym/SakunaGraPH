export const ASK_WORKSPACE_STORAGE_KEY = 'sakunagraph.ask.workspace.v1';
export const ASK_WORKSPACE_MAX_SESSIONS = 12;

function nowIso() {
	return new Date().toISOString();
}

function sessionId() {
	return (
		globalThis.crypto?.randomUUID?.() ?? `ask-${Date.now()}-${Math.random().toString(36).slice(2)}`
	);
}

function cleanMessage(message) {
	if (!message || typeof message !== 'object') return null;
	if (message.role === 'user') return { role: 'user', text: String(message.text ?? '') };
	if (message.role !== 'assistant') return null;

	return {
		role: 'assistant',
		loading: false,
		streaming: false,
		...(message.text ? { text: String(message.text) } : {}),
		...(message.error ? { error: String(message.error) } : {}),
		...(message.cancelled || message.loading || message.streaming ? { cancelled: true } : {}),
		...(typeof message.sparql === 'string' ? { sparql: message.sparql } : {}),
		...(Array.isArray(message.rows) ? { rows: message.rows } : {}),
		...(Array.isArray(message.citations) ? { citations: message.citations } : {}),
		...(message.retrieval && typeof message.retrieval === 'object'
			? { retrieval: message.retrieval }
			: {}),
		...(message.method && typeof message.method === 'object' ? { method: message.method } : {}),
		...(message.requestId ? { requestId: String(message.requestId) } : {}),
	};
}

export function titleFromMessages(messages = []) {
	const firstQuestion = messages.find((message) => message?.role === 'user')?.text?.trim();
	if (!firstQuestion) return 'New research';
	return firstQuestion.length > 58 ? `${firstQuestion.slice(0, 57).trimEnd()}…` : firstQuestion;
}

export function createAskSession(values = {}) {
	const timestamp = nowIso();
	const messages = Array.isArray(values.messages)
		? values.messages.map(cleanMessage).filter(Boolean)
		: [];
	return {
		id: typeof values.id === 'string' && values.id ? values.id : sessionId(),
		title:
			typeof values.title === 'string' && values.title.trim()
				? values.title.trim().slice(0, 80)
				: titleFromMessages(messages),
		createdAt: typeof values.createdAt === 'string' ? values.createdAt : timestamp,
		updatedAt: typeof values.updatedAt === 'string' ? values.updatedAt : timestamp,
		messages,
		draft: typeof values.draft === 'string' ? values.draft.slice(0, 1_000) : '',
		forceLlmQuery: Boolean(values.forceLlmQuery),
	};
}

export function loadAskWorkspace(storage = globalThis.localStorage) {
	try {
		const parsed = JSON.parse(storage?.getItem(ASK_WORKSPACE_STORAGE_KEY) ?? 'null');
		const sessions = Array.isArray(parsed?.sessions)
			? parsed.sessions.slice(0, ASK_WORKSPACE_MAX_SESSIONS).map(createAskSession)
			: [];
		const activeSessionId = sessions.some((session) => session.id === parsed?.activeSessionId)
			? parsed.activeSessionId
			: (sessions[0]?.id ?? null);
		return { sessions, activeSessionId };
	} catch {
		return { sessions: [], activeSessionId: null };
	}
}

export function saveAskWorkspace(sessions, activeSessionId, storage = globalThis.localStorage) {
	const normalized = sessions
		.map(createAskSession)
		.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))
		.slice(0, ASK_WORKSPACE_MAX_SESSIONS);
	storage?.setItem(
		ASK_WORKSPACE_STORAGE_KEY,
		JSON.stringify({ version: 1, activeSessionId, sessions: normalized }),
	);
	return normalized;
}

export function exportSessionMarkdown(session) {
	const lines = [
		`# ${session.title}`,
		'',
		`Saved from SakunaGraPH on ${new Intl.DateTimeFormat('en-PH', { dateStyle: 'long', timeStyle: 'short' }).format(new Date(session.updatedAt))}.`,
		'',
	];

	for (const message of session.messages) {
		if (message.role === 'user') {
			lines.push('## Question', '', message.text, '');
			continue;
		}
		lines.push(
			'## Graph response',
			'',
			message.text || message.error || 'No answer was saved.',
			'',
		);
		if (message.citations?.length) {
			lines.push('### Sources', '');
			for (const citation of message.citations) {
				lines.push(`- ${citation.label}${citation.uri ? ` — ${citation.uri}` : ''}`);
			}
			lines.push('');
		}
		if (message.sparql) lines.push('### Graph query', '', '```sparql', message.sparql, '```', '');
	}

	return lines.join('\n');
}
