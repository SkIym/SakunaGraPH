export function groupEventsByAlternates(events) {
	const byIri = new Map();
	for (const row of events) {
		if (row.event) byIri.set(row.event, row);
	}

	const adjacentAlternates = new Map();
	for (const row of events) {
		if (!row.event) continue;
		adjacentAlternates.set(
			row.event,
			(row.alternates ?? []).filter((alternate) => alternate && byIri.has(alternate)),
		);
	}

	const parent = new Map();
	function find(value) {
		if (!parent.has(value)) parent.set(value, value);
		if (parent.get(value) !== value) parent.set(value, find(parent.get(value)));
		return parent.get(value);
	}
	function union(left, right) {
		const leftParent = find(left);
		const rightParent = find(right);
		if (leftParent !== rightParent) parent.set(leftParent, rightParent);
	}

	for (const [iri, alternates] of adjacentAlternates) {
		for (const alternate of alternates) union(iri, alternate);
	}

	const clusters = new Map();
	for (const iri of byIri.keys()) {
		const root = find(iri);
		if (!clusters.has(root)) clusters.set(root, []);
		clusters.get(root).push(iri);
	}

	const output = [];
	const seen = new Set();
	for (const row of events) {
		const iri = row.event;
		if (!iri || seen.has(iri)) continue;
		const clusterIris = clusters.get(find(iri)) ?? [iri];
		for (const member of clusterIris) seen.add(member);

		if (clusterIris.length === 1) {
			output.push({ row, subs: [] });
			continue;
		}

		const members = clusterIris.map((member) => byIri.get(member)).filter(Boolean);
		members.sort((left, right) => {
			const leftDate = left.startDate ?? '';
			const rightDate = right.startDate ?? '';
			if (leftDate !== rightDate) return leftDate < rightDate ? -1 : 1;
			return (left.event ?? '').localeCompare(right.event ?? '');
		});
		const [representative, ...subRows] = members;
		output.push({ row: representative, subs: subRows });
	}

	return output;
}
