import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import OntologyWalkthrough from '../../src/lib/features/ontology/components/OntologyWalkthrough.svelte';

describe('OntologyWalkthrough', () => {
	it('switches the complete visual story and links to the same query', async () => {
		render(OntologyWalkthrough);

		expect(
			screen.getByText(/Which flash flood and riverine flood events occurred/),
		).toBeInTheDocument();

		await fireEvent.click(screen.getByRole('button', { name: /People displaced by an event/ }));

		expect(screen.getByText(/How many people and families were displaced/)).toBeInTheDocument();
		expect(screen.getByText('AffectedPopulation')).toBeInTheDocument();
		expect(screen.getByRole('link', { name: 'Open CQ06 in the query workspace' })).toHaveAttribute(
			'href',
			'/query?cq=CQ06',
		);
	});

	it('reveals the complete SPARQL query on request', async () => {
		render(OntologyWalkthrough);
		const disclosure = screen.getByText('View the complete SPARQL query');

		await fireEvent.click(disclosure);

		expect(disclosure.closest('details')).toHaveAttribute('open');
		expect(screen.getByText(/PREFIX rdfs:/)).toBeInTheDocument();
	});
});
