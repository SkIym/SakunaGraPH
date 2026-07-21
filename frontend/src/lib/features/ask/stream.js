export class AskStreamProtocolError extends Error {
	constructor(message, options = {}) {
		super(message, options);
		this.name = 'AskStreamProtocolError';
		this.kind = 'response';
	}
}

export class AskStreamUpstreamError extends Error {
	constructor(message, { status = 500, requestId = null } = {}) {
		super(message);
		this.name = 'AskStreamUpstreamError';
		this.kind = 'http';
		this.status = status;
		this.requestId = requestId;
	}
}

function abortError(reason) {
	const error = new Error('The request was cancelled.', { cause: reason });
	error.name = 'AbortError';
	error.kind = 'cancelled';
	return error;
}

function takeLine(buffer, flush = false) {
	for (let index = 0; index < buffer.length; index += 1) {
		const character = buffer[index];
		if (character !== '\n' && character !== '\r') continue;
		if (character === '\r' && index === buffer.length - 1 && !flush) return null;
		const separatorLength = character === '\r' && buffer[index + 1] === '\n' ? 2 : 1;
		return [buffer.slice(0, index), buffer.slice(index + separatorLength)];
	}
	return flush && buffer ? [buffer, ''] : null;
}

/**
 * Parse a ReadableStream using the Server-Sent Events line and multi-line data rules.
 * Event data remains a string here so the GraphRAG contract can validate JSON separately.
 */
export async function* parseServerSentEvents(body, { signal } = {}) {
	if (!body?.getReader) throw new AskStreamProtocolError('The server returned no response stream.');
	if (signal?.aborted) throw abortError(signal.reason);

	const reader = body.getReader();
	const decoder = new TextDecoder();
	let buffer = '';
	let eventName = 'message';
	let dataLines = [];
	let exhausted = false;
	const cancelReader = () => void reader.cancel(signal?.reason).catch(() => {});
	signal?.addEventListener('abort', cancelReader, { once: true });

	function consumeLine(line) {
		if (line === '') {
			if (!dataLines.length) {
				eventName = 'message';
				return null;
			}
			const event = { event: eventName, data: dataLines.join('\n') };
			eventName = 'message';
			dataLines = [];
			return event;
		}
		if (line.startsWith(':')) return null;

		const separator = line.indexOf(':');
		const field = separator === -1 ? line : line.slice(0, separator);
		let value = separator === -1 ? '' : line.slice(separator + 1);
		if (value.startsWith(' ')) value = value.slice(1);
		if (field === 'event') eventName = value || 'message';
		if (field === 'data') dataLines.push(value);
		return null;
	}

	try {
		while (true) {
			const { done, value } = await reader.read();
			if (signal?.aborted) throw abortError(signal.reason);
			if (done) {
				exhausted = true;
				buffer += decoder.decode();
				break;
			}
			buffer += decoder.decode(value, { stream: true });

			let extracted = takeLine(buffer);
			while (extracted) {
				const [line, remainder] = extracted;
				buffer = remainder;
				const event = consumeLine(line);
				if (event) yield event;
				extracted = takeLine(buffer);
			}
		}

		let extracted = takeLine(buffer, true);
		while (extracted) {
			const [line, remainder] = extracted;
			buffer = remainder;
			const event = consumeLine(line);
			if (event) yield event;
			extracted = takeLine(buffer, true);
		}
		const finalEvent = consumeLine('');
		if (finalEvent) yield finalEvent;
	} catch (error) {
		if (signal?.aborted || error?.name === 'AbortError') throw abortError(signal?.reason ?? error);
		if (error instanceof AskStreamProtocolError) throw error;
		throw new AskStreamProtocolError('The response stream disconnected unexpectedly.', {
			cause: error,
		});
	} finally {
		signal?.removeEventListener('abort', cancelReader);
		if (!exhausted) await reader.cancel().catch(() => {});
		reader.releaseLock();
	}
}

function requireObject(value, label) {
	if (!value || typeof value !== 'object' || Array.isArray(value)) {
		throw new AskStreamProtocolError(`${label} must be an object.`);
	}
	return value;
}

function requireString(value, label, { allowEmpty = false } = {}) {
	if (typeof value !== 'string' || (!allowEmpty && !value.trim())) {
		throw new AskStreamProtocolError(`${label} must be a ${allowEmpty ? '' : 'non-empty '}string.`);
	}
	return value;
}

export function normalizeCitation(value) {
	const citation = requireObject(value, 'Citation');
	return {
		...citation,
		id: requireString(citation.id, 'Citation id'),
		label: requireString(citation.label, 'Citation label'),
		uri: requireString(citation.uri, 'Citation URI'),
	};
}

function citationEventType(type) {
	return /^citation(?:(?:[._/-])v?\d+)?$/i.test(type);
}

function mergeCitation(citations, value) {
	const citation = normalizeCitation(value);
	const existingIndex = citations.findIndex((item) => item.id === citation.id);
	if (existingIndex === -1) return [...citations, citation];
	if (citations[existingIndex].uri !== citation.uri) {
		throw new AskStreamProtocolError(`Citation ${citation.id} was reused for a different source.`);
	}
	return citations.map((item, index) =>
		index === existingIndex ? { ...item, ...citation } : item,
	);
}

function mergeCitations(citations, values = []) {
	if (!Array.isArray(values)) throw new AskStreamProtocolError('Citations must be an array.');
	return values.reduce(mergeCitation, citations);
}

function eventPayload(event) {
	let payload;
	try {
		payload = JSON.parse(event.data);
	} catch (cause) {
		throw new AskStreamProtocolError('The server returned a malformed stream event.', { cause });
	}
	return requireObject(payload, 'Stream event');
}

/**
 * Consume the additive GraphRAG v1 contract and return the legacy-compatible answer shape.
 * Unknown event types are ignored so future additive protocol events remain compatible.
 */
export async function consumeAskStream(
	response,
	{ signal, onMeta, onToken, onCitation, onDone } = {},
) {
	const result = {
		sparql: '',
		answer: '',
		rows: [],
		citations: [],
		retrieval: null,
		requestId: null,
	};
	let seenMeta = false;

	for await (const event of parseServerSentEvents(response?.body, { signal })) {
		const payload = eventPayload(event);
		const type = String(payload.type ?? (event.event !== 'message' ? event.event : ''));

		if (type === 'error') {
			const status = payload.status;
			if (!Number.isInteger(status)) {
				throw new AskStreamProtocolError('Stream error status must be an integer.');
			}
			throw new AskStreamUpstreamError(requireString(payload.detail, 'Stream error detail'), {
				status,
				requestId: payload.requestId ?? null,
			});
		}

		if (type === 'meta') {
			if (seenMeta)
				throw new AskStreamProtocolError('The stream returned more than one meta event.');
			if (!Array.isArray(payload.rows)) {
				throw new AskStreamProtocolError('Stream meta rows must be an array.');
			}
			seenMeta = true;
			result.sparql = requireString(payload.sparql, 'Stream meta SPARQL', { allowEmpty: true });
			result.rows = payload.rows;
			result.citations = mergeCitations(result.citations, payload.citations ?? []);
			result.retrieval = payload.retrieval ?? null;
			result.requestId = payload.requestId ?? null;
			await onMeta?.({ ...result }, payload);
			continue;
		}

		if (type === 'token') {
			if (!seenMeta) throw new AskStreamProtocolError('A token arrived before stream metadata.');
			const text = requireString(payload.text, 'Stream token text');
			result.answer += text;
			await onToken?.(text, { ...result });
			continue;
		}

		if (citationEventType(type)) {
			if (!seenMeta) throw new AskStreamProtocolError('A citation arrived before stream metadata.');
			const citationValue = payload.citation ?? (event.event !== 'message' ? payload : null);
			const citation = normalizeCitation(citationValue);
			result.citations = mergeCitation(result.citations, citation);
			await onCitation?.(citation, { ...result });
			continue;
		}

		if (type === 'done') {
			if (!seenMeta) throw new AskStreamProtocolError('The stream ended before metadata arrived.');
			result.citations = mergeCitations(result.citations, payload.citations ?? []);
			if (payload.retrieval !== undefined) result.retrieval = payload.retrieval;
			await onDone?.({ ...result }, payload);
			return result;
		}
	}

	throw new AskStreamProtocolError(
		seenMeta
			? 'The answer stream disconnected before completion.'
			: 'The answer stream ended before metadata arrived.',
	);
}
