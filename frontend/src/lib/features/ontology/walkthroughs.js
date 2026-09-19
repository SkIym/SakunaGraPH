import { COMPETENCY_QUESTIONS } from '$lib/competency_queries.js';

function competency(id) {
	const match = COMPETENCY_QUESTIONS.find((question) => question.id === id);
	if (!match) throw new Error(`Missing competency question ${id}`);
	return match;
}

export const ONTOLOGY_WALKTHROUGHS = [
	{
		...competency('CQ02'),
		shortLabel: 'Events across place and time',
		category: 'Event, type, date, and location',
		question:
			'Which flash flood and riverine flood events occurred, when did they begin, and which Philippine region contains each location?',
		concepts: [
			{ label: 'DisasterEvent', kind: 'class' },
			{ label: 'FlashFlood', kind: 'value' },
			{ label: 'RiverineFlood', kind: 'value' },
			{ label: 'startDate', kind: 'property' },
			{ label: 'Location', kind: 'class' },
			{ label: 'Region', kind: 'class' },
		],
		relations: [
			{ subject: 'DisasterEvent', predicate: 'hasDisasterType', object: 'Flood type' },
			{ subject: 'DisasterEvent', predicate: 'startDate', object: 'Date' },
			{ subject: 'DisasterEvent', predicate: 'hasLocation', object: 'Location' },
			{ subject: 'Location', predicate: 'isPartOf*', object: 'Region' },
		],
		insight: 'The * path follows every PSGC parent until the event location reaches its region.',
		queryPreview: `VALUES ?disasterType { :FlashFlood :RiverineFlood }
?event a :DisasterEvent ;
       :hasDisasterType ?disasterType ;
       :startDate ?startDate ;
       :hasLocation ?location .
?location :isPartOf* ?region .`,
	},
	{
		...competency('CQ06'),
		shortLabel: 'People displaced by an event',
		category: 'Human impact and aggregation',
		question:
			'How many people and families were displaced by each disaster event in Western Visayas?',
		concepts: [
			{ label: 'DisasterEvent', kind: 'class' },
			{ label: 'AffectedPopulation', kind: 'class' },
			{ label: 'displacedPersons', kind: 'property' },
			{ label: 'displacedFamilies', kind: 'property' },
			{ label: 'Western Visayas', kind: 'value' },
			{ label: 'SUM', kind: 'operator' },
		],
		relations: [
			{ subject: 'DisasterEvent', predicate: 'hasAffectedPopulation', object: 'Population record' },
			{ subject: 'Population record', predicate: 'hasLocation', object: 'Location' },
			{ subject: 'Location', predicate: 'isPartOf*', object: 'Western Visayas' },
			{ subject: 'Population record', predicate: 'displacedPersons', object: 'Count' },
		],
		insight:
			'Population values stay attached to their location, then SPARQL sums them for each event.',
		queryPreview: `?event a :DisasterEvent ;
       :hasAffectedPopulation ?population .
?location :isPartOf* :0600000000 .
?population :displacedPersons ?displacedPersons ;
            :displacedFamilies ?displacedFamilies .
GROUP BY ?event ?eventName`,
	},
	{
		...competency('CQ20'),
		shortLabel: 'One event, multiple sources',
		category: 'Provenance and record alignment',
		question: 'Which disaster events were reported by more than one data source?',
		concepts: [
			{ label: 'DisasterEvent', kind: 'class' },
			{ label: 'alternateOf', kind: 'property' },
			{ label: 'Source record', kind: 'class' },
			{ label: 'Organization', kind: 'class' },
			{ label: 'wasDerivedFrom', kind: 'property' },
			{ label: 'wasAttributedTo', kind: 'property' },
		],
		relations: [
			{ subject: 'Event record A', predicate: 'prov:alternateOf', object: 'Event record B' },
			{ subject: 'Event record A', predicate: 'prov:wasDerivedFrom+', object: 'Source record' },
			{ subject: 'Source record', predicate: 'prov:wasAttributedTo', object: 'Organization' },
			{ subject: 'Event record B', predicate: 'startDate', object: 'Date' },
		],
		insight:
			'Alternate records remain distinct, so their source organizations and dates can still be compared.',
		queryPreview: `?event1 a :DisasterEvent ;
        prov:alternateOf ?event2 ;
        prov:wasDerivedFrom+/prov:wasAttributedTo ?source1 .
?event2 prov:wasDerivedFrom+/prov:wasAttributedTo ?source2 .
FILTER(STR(?event1) < STR(?event2))`,
	},
];
