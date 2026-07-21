import { apiJson } from './client.js';

const ONTOLOGY_TIMEOUT_MS = 60_000;

export function getOntologyGraph(options = {}) {
	return apiJson('/api/ontology/graph', { timeoutMs: ONTOLOGY_TIMEOUT_MS, ...options });
}

export function getOntologyTaxonomy(options = {}) {
	return apiJson('/api/ontology/taxonomy', { timeoutMs: ONTOLOGY_TIMEOUT_MS, ...options });
}

export function getOntologyPsgc(options = {}) {
	return apiJson('/api/ontology/psgc', { timeoutMs: ONTOLOGY_TIMEOUT_MS, ...options });
}
