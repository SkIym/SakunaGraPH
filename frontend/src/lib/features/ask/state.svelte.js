import { ASK_MODES, askQuestion, openAskStream, preferredAskMode } from '../../api/ask.js';
import { isCancellationError } from '../../api/errors.js';
import { consumeAskStream } from './stream.js';

export const ASK_SUGGESTIONS = Object.freeze([
	'How many flood events were recorded in 2023?',
	'Which region had the most casualties from typhoons?',
	'List the top 5 disaster events by affected population.',
	'What types of disasters occurred in Mindanao?',
]);

function requestErrorMessage(error) {
	return error?.kind === 'network'
		? 'Could not reach server.'
		: error?.message || 'Request failed.';
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
	let activeRequest = null;

	function updateAssistant(index, values) {
		messages = messages.map((message, messageIndex) =>
			messageIndex === index ? { ...message, ...values } : message,
		);
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
		updateAssistant(index, {
			loading: false,
			streaming: false,
			text: response.answer,
			sparql: response.sparql,
			rows: response.rows ?? [],
			citations: response.citations ?? [],
			retrieval: response.retrieval ?? (fallback ? { mode: 'fallback' } : null),
			requestId: response.requestId ?? null,
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
						text: result.answer || 'No answer was returned.',
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

		if (activeRequest) {
			const replacedRequest = activeRequest;
			replacedRequest.controller.abort();
			finishCancelled(replacedRequest.assistantIndex);
			activeRequest = null;
		}

		input = '';
		sending = true;
		announcement = 'Generating answer.';
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
			if (activeRequest === request) announcement = 'Answer complete.';
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
		announcement = 'Request cancelled.';
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
		},
		get sending() {
			return sending;
		},
		get announcement() {
			return announcement;
		},
		get mode() {
			return mode;
		},
		send,
		cancel,
	};
}
