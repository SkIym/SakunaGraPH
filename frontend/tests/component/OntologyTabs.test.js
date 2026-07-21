import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import OntologyTabs from '../../src/lib/features/ontology/components/OntologyTabs.svelte';

const tabs = [
	{ id: 'graph', label: 'Core Ontology' },
	{ id: 'taxonomy', label: 'Disaster Taxonomy' },
];

describe('OntologyTabs', () => {
	it('exposes button semantics and reports the selected view', async () => {
		const onChange = vi.fn();
		render(OntologyTabs, { tabs, active: 'graph', onChange });

		expect(screen.getByRole('button', { name: 'Core Ontology' })).toHaveAttribute(
			'aria-pressed',
			'true',
		);
		await fireEvent.click(screen.getByRole('button', { name: 'Disaster Taxonomy' }));
		expect(onChange).toHaveBeenCalledWith('taxonomy');
	});
});
