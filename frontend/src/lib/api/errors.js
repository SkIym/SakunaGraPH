export class ApiError extends Error {
	constructor(
		message,
		{ kind = 'unknown', status = null, url = '', requestId = null, cause } = {},
	) {
		super(message);
		this.name = 'ApiError';
		this.kind = kind;
		this.status = status;
		this.url = url;
		this.requestId = requestId;
		if (cause !== undefined) this.cause = cause;
	}
}

export class ApiHttpError extends ApiError {
	constructor(message, options = {}) {
		super(message, { ...options, kind: options.kind ?? 'http' });
		this.name = 'ApiHttpError';
		this.body = options.body ?? null;
	}
}

export class GraphDbError extends ApiHttpError {
	constructor(message, options = {}) {
		super(message, { ...options, kind: 'graphdb' });
		this.name = 'GraphDbError';
	}
}

export class ApiResponseError extends ApiError {
	constructor(message, options = {}) {
		super(message, { ...options, kind: 'response' });
		this.name = 'ApiResponseError';
	}
}

export class ApiNetworkError extends ApiError {
	constructor(message = 'Could not reach the server.', options = {}) {
		super(message, { ...options, kind: 'network' });
		this.name = 'ApiNetworkError';
	}
}

export class ApiTimeoutError extends ApiError {
	constructor(message = 'The request timed out.', options = {}) {
		super(message, { ...options, kind: 'timeout' });
		this.name = 'ApiTimeoutError';
	}
}

export class ApiCancellationError extends ApiError {
	constructor(message = 'The request was cancelled.', options = {}) {
		super(message, { ...options, kind: 'cancelled' });
		// Preserve the platform name used by existing component cancellation guards.
		this.name = 'AbortError';
	}
}

export function isCancellationError(error) {
	return error?.name === 'AbortError' || error?.kind === 'cancelled';
}
