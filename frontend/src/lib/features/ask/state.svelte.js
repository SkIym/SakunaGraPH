import { ASK_MODES, askQuestion, openAskStream, preferredAskMode } from '../../api/ask.js';
import { isCancellationError } from '../../api/errors.js';
import { consumeAskStream } from './stream.js';

export const ASK_SUGGESTIONS = Object.freeze([
	'How many flood events were recorded in 2023?',
	'Which region had the most casualties from typhoons?',
	'List the top 5 disaster events by affected population.',
	'What types of disasters occurred in Mindanao?',
]);

export const ASK_QUESTION_MAX_LENGTH = 1_000;

function requestErrorMessage(error) {
	if (error?.kind === 'network') {
		return 'Could not reach the data service. Check your connection, then send the question again.';
	}
	if (error?.kind === 'timeout') {
		return 'The answer took too long to complete. Try a narrower question or a shorter date range.';
	}
	if (error?.status === 429) {
		return 'The data service is receiving too many questions. Wait a moment, then try again.';
	}
	if (error?.status === 400 || error?.status === 422) {
		return 'The question could not be processed as written. Rephrase it with a place, date, or disaster type.';
	}
	if (error?.name === 'AskStreamUpstreamError' && error?.message) return error.message;
	if (error?.status >= 500) {
		const detail = String(error?.message ?? '').trim();
		if (detail && detail.length <= 240 && !/[<>\r\n]/.test(detail)) {
			return `${detail} Try again shortly.`;
		}
		return 'The data service is temporarily unavailable. Your question is still shown above; try it again shortly.';
	}
	return 'Could not complete this answer. Rephrase the question or try again.';
}

export function createAskState({
	mode = preferredAskMode(),
	submit = askQuestion,
	openStream = openAskStream,
	onUpdated = async () => {},
} = {}) {
	let messages = $state([]);
	let input = $state('');
	let sending = $state(false);
	let announcement = $state('');
	let inputError = $state('');
	let activeRequest = null;

	function updateAssistant(index, values) {
		if (!messages[index]) return;
		messages[index] = { ...messages[index], ...values };
	}

	function finishCancelled(index) {
		const message = messages[index];
		if (!message || message.role !== 'assistant' || (!message.loading && !message.streaming))
			return;
		updateAssistant(index, {
			loading: false,
			streaming: false,
			cancelled: true,
		});
	}

	function applyLegacyResponse(index, response, { fallback = false } = {}) {
		const answer = String(response?.answer ?? '').trim();
		updateAssistant(index, {
			loading: false,
			streaming: false,
			text:
				answer ||
				'No answer was returned. Try asking about a specific place, date range, or disaster type.',
			sparql: typeof response?.sparql === 'string' ? response.sparql : '',
			rows: Array.isArray(response?.rows) ? response.rows : [],
			citations: Array.isArray(response?.citations) ? response.citations : [],
			retrieval:
				response?.retrieval && typeof response.retrieval === 'object'
					? response.retrieval
					: fallback
						? { mode: 'fallback' }
						: null,
			requestId: response?.requestId ?? null,
		});
	}

	async function runStream(question, assistantIndex, controller) {
		let receivedMeta = false;
		try {
			const response = await openStream(question, { signal: controller.signal });
			return await consumeAskStream(response, {
				signal: controller.signal,
				onMeta: async (result) => {
					receivedMeta = true;
					updateAssistant(assistantIndex, {
						loading: false,
						streaming: true,
						text: result.answer,
						sparql: result.sparql,
						rows: result.rows,
						citations: result.citations,
						retrieval: result.retrieval,
						requestId: result.requestId,
					});
					await onUpdated();
				},
				onToken: async (_text, result) => {
					updateAssistant(assistantIndex, { text: result.answer });
					await onUpdated();
				},
				onCitation: async (_citation, result) => {
					updateAssistant(assistantIndex, { citations: result.citations });
					await onUpdated();
				},
				onDone: async (result) => {
					updateAssistant(assistantIndex, {
						loading: false,
						streaming: false,
						text:
							result.answer ||
							'No answer was returned. Try asking about a specific place, date range, or disaster type.',
						citations: result.citations,
						retrieval: result.retrieval,
					});
					await onUpdated();
				},
			});
		} catch (error) {
			if (isCancellationError(error) || receivedMeta) throw error;
			const response = await submit(question, { signal: controller.signal });
			applyLegacyResponse(assistantIndex, response, { fallback: true });
			return response;
		}
	}

	async function send(question = input.trim()) {
		question = String(question ?? '').trim();
		if (!question) return;
		if (question.length > ASK_QUESTION_MAX_LENGTH) {
			inputError = `Keep the question under ${ASK_QUESTION_MAX_LENGTH.toLocaleString()} characters.`;
			announcement = inputError;
			return;
		}
		inputError = '';

		if (activeRequest) {
			const replacedRequest = activeRequest;
			replacedRequest.controller.abort();
			finishCancelled(replacedRequest.assistantIndex);
			activeRequest = null;
		}

		input = '';
		sending = true;
		announcement = 'Checking the knowledge graph.';
		messages = [...messages, { role: 'user', text: question }];
		const assistantIndex = messages.length;
		messages = [...messages, { role: 'assistant', loading: true }];
		const controller = new AbortController();
		const request = { controller, assistantIndex };
		activeRequest = request;
		await onUpdated();

		try {
			if (mode === ASK_MODES.STREAM) {
				await runStream(question, assistantIndex, controller);
			} else {
				const response = await submit(question, { signal: controller.signal });
				applyLegacyResponse(assistantIndex, response);
			}
			if (activeRequest === request) announcement = 'Answer ready.';
		} catch (requestError) {
			if (isCancellationError(requestError)) return;
			updateAssistant(assistantIndex, {
				loading: false,
				streaming: false,
				error: requestErrorMessage(requestError),
			});
			if (activeRequest === request) announcement = 'The answer could not be completed.';
		} finally {
			if (activeRequest === request) {
				sending = false;
				activeRequest = null;
				await onUpdated();
			}
		}
	}

	function cancel() {
		if (!activeRequest) return;
		const request = activeRequest;
		activeRequest = null;
		request.controller.abort();
		finishCancelled(request.assistantIndex);
		sending = false;
		announcement = 'Answer stopped.';
		void onUpdated();
	}

	return {
		get messages() {
			return messages;
		},
		get input() {
			return input;
		},
		set input(value) {
			input = value;
			if (inputError && String(value).trim().length <= ASK_QUESTION_MAX_LENGTH) inputError = '';
		},
		get sending() {
			return sending;
		},
		get announcement() {
			return announcement;
		},
		get inputError() {
			return inputError;
		},
		get mode() {
			return mode;
		},
		send,
		cancel,
	};
}
