<script>
	import { onMount, tick } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import { getOntologyGraph, getOntologyPsgc, getOntologyTaxonomy } from '$lib/api/ontology.js';
	import CoreOntologyPanel from './components/CoreOntologyPanel.svelte';
	import OntologyTabs from './components/OntologyTabs.svelte';
	import PsgcPanel from './components/PsgcPanel.svelte';
	import TaxonomyPanel from './components/TaxonomyPanel.svelte';

	// ── Tab state ─────────────────────────────────────────────────────────────
	let activeTab = $state('graph');
	const TABS = [
		{
			id: 'graph',
			label: 'Core Ontology',
			kicker: 'OWL class network',
			title: 'Core disaster ontology',
			description: 'Trace classes, inheritance, and object properties across the knowledge graph.',
		},
		{
			id: 'taxonomy',
			label: 'Disaster Taxonomy',
			kicker: 'Classification tree',
			title: 'Disaster type taxonomy',
			description:
				'Follow the hierarchy from broad origins to the hazard types used in event records.',
		},
		{
			id: 'psgc',
			label: 'PSGC Locations',
			kicker: 'Geographic registry',
			title: 'Philippine location graph',
			description:
				'Inspect PSGC regions, provinces, and independent cities as linked geographic entities.',
		},
	];
	const activeView = $derived(TABS.find((tab) => tab.id === activeTab) ?? TABS[0]);
	const activeViewNumber = $derived(
		String(TABS.findIndex((tab) => tab.id === activeTab) + 1).padStart(2, '0'),
	);

	// ══════════════════════════════════════════════════════════════════════════
	// CLASS GRAPH
	// ══════════════════════════════════════════════════════════════════════════
	let svgEl = $state(null);
	let loading = $state(true);
	let hoveredNode = $state(null);
	let selectedNode = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);
	// Re-center function exposed so the $effect below can call it on tab switch
	let graphFitFn = null;
	let graphReady = false;

	const GROUP_COLOR = {
		core: '#0038a8',
		impact: '#b46518',
		response: '#2f7d56',
		preparedness: '#4e6fae',
		location: '#0f766e',
		type: '#6d5c9b',
		source: '#64748b',
	};
	const GROUP_LABEL = {
		core: 'Core Event',
		impact: 'Impact',
		response: 'Response',
		preparedness: 'Preparedness',
		location: 'Location',
		type: 'Disaster Type',
		source: 'Source',
	};
	const LINK_DASH = { subClassOf: '0', objectProperty: '5 3' };
	const LINK_COLOR = { subClassOf: '#cbd5e1', objectProperty: '#93c5fd' };
	const IDLE_ALPHA = 0.05;
	const prefersReducedMotion = () =>
		typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
	const motionDuration = (duration) => (prefersReducedMotion() ? 0 : duration);
	function activateOnKeyboard(event) {
		if (!['Enter', ' '].includes(event.key)) return;
		event.preventDefault();
		event.currentTarget.dispatchEvent(new MouseEvent('click', { bubbles: true }));
	}

	function nodeR(d) {
		return d.group === 'core' ? 22 : 15;
	}

	onMount(() => {
		const controller = new AbortController();
		let cleanup = () => {};
		let disposed = false;
		void initGraph(controller.signal)
			.then((disposeGraph) => {
				if (disposed) disposeGraph();
				else cleanup = disposeGraph;
			})
			.catch((requestError) => {
				if (requestError.name !== 'AbortError') loading = false;
			});
		return () => {
			disposed = true;
			controller.abort();
			cleanup();
		};
	});

	async function initGraph(signal) {
		const [
			{ forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide },
			{ drag },
			{ zoom, zoomIdentity },
			{ select },
		] = await Promise.all([
			import('d3-force'),
			import('d3-drag'),
			import('d3-zoom'),
			import('d3-selection'),
		]);

		const graphData = await getOntologyGraph({ signal });
		if (signal.aborted) return () => {};
		const nodes = graphData.nodes.map((n) => ({ ...n }));
		const links = graphData.links.map((l) => ({ ...l }));

		const W = svgEl.clientWidth || 900;
		const H = svgEl.clientHeight || 700;

		const styleTag = document.createElement('style');
		styleTag.textContent = `
			@keyframes ont-pulse {
				0%   { transform: scale(1);   opacity: 0.65; }
				100% { transform: scale(3.4); opacity: 0;    }
			}
			.pulse-ring-anim {
				transform-box: fill-box;
				transform-origin: center;
			}
		`;
		document.head.appendChild(styleTag);

		const svg = select(svgEl);
		const g = svg.append('g');

		const zoomBehavior = zoom()
			.scaleExtent([0.2, 3])
			.filter(
				(event) =>
					event.target === svgEl || event.target.tagName === 'svg' || event.type === 'wheel',
			)
			.on('zoom', (event) => g.attr('transform', event.transform));
		svg.call(zoomBehavior).on('dblclick.zoom', null);

		// Stored so the $effect can re-center when returning to this tab
		graphFitFn = () => {
			try {
				const bounds = g.node().getBBox();
				if (!bounds.width && !bounds.height) return;
				const pad = 48;
				const s = Math.min(W / (bounds.width + pad * 2), H / (bounds.height + pad * 2), 1);
				const tx = (W - s * (bounds.width + pad * 2)) / 2 - s * (bounds.x - pad);
				const ty = (H - s * (bounds.height + pad * 2)) / 2 - s * (bounds.y - pad);
				svg
					.transition()
					.duration(motionDuration(450))
					.call(zoomBehavior.transform, zoomIdentity.translate(tx, ty).scale(s));
			} catch {}
		};

		const defs = svg.append('defs');
		['subClassOf', 'objectProperty'].forEach((type) => {
			defs
				.append('marker')
				.attr('id', `arrow-${type}`)
				.attr('viewBox', '0 -4 8 8')
				.attr('refX', 22)
				.attr('refY', 0)
				.attr('markerWidth', 6)
				.attr('markerHeight', 6)
				.attr('orient', 'auto')
				.append('path')
				.attr('d', 'M0,-4L8,0L0,4')
				.attr('fill', LINK_COLOR[type]);
		});

		const simulation = forceSimulation(nodes)
			.force(
				'link',
				forceLink(links)
					.id((d) => d.id)
					.distance(140)
					.strength(0.5),
			)
			.force('charge', forceManyBody().strength(-500))
			.force('center', forceCenter(W / 2, H / 2).strength(0.06))
			.force('collide', forceCollide(46))
			.alphaDecay(0.018)
			.velocityDecay(0.28);

		const linkSel = g
			.append('g')
			.selectAll('line')
			.data(links)
			.join('line')
			.attr('stroke', (d) => LINK_COLOR[d.type])
			.attr('stroke-width', (d) => (d.type === 'subClassOf' ? 1.2 : 1.5))
			.attr('stroke-dasharray', (d) => LINK_DASH[d.type])
			.attr('marker-end', (d) => `url(#arrow-${d.type})`);

		const linkTextSel = g
			.append('g')
			.selectAll('text')
			.data(links.filter((l) => l.type === 'objectProperty'))
			.join('text')
			.attr('font-size', 8)
			.attr('fill', '#94a3b8')
			.attr('text-anchor', 'middle')
			.attr('pointer-events', 'none')
			.attr('opacity', 0.6)
			.text((d) => d.label);

		const nodeSel = g
			.append('g')
			.selectAll('g')
			.data(nodes)
			.join('g')
			.attr('cursor', 'grab')
			.attr('role', 'button')
			.attr('tabindex', 0)
			.attr('aria-label', (d) => `${d.label}. ${d.definition ?? 'Ontology class'}`);

		nodeSel
			.append('circle')
			.attr('class', 'main-circle')
			.attr('r', (d) => nodeR(d))
			.attr('fill', (d) => GROUP_COLOR[d.group] + '22')
			.attr('stroke', (d) => GROUP_COLOR[d.group])
			.attr('stroke-width', (d) => (d.group === 'core' ? 2.5 : 1.8));

		nodeSel
			.append('text')
			.attr('text-anchor', 'middle')
			.attr('dy', '0.35em')
			.attr('font-size', (d) => (d.group === 'core' ? 13 : 9))
			.attr('font-weight', '700')
			.attr('fill', (d) => GROUP_COLOR[d.group])
			.attr('pointer-events', 'none')
			.text((d) => d.label.charAt(0));

		nodeSel
			.append('text')
			.attr('text-anchor', 'middle')
			.attr('dy', (d) => (d.group === 'core' ? 34 : 27))
			.attr('font-size', (d) => (d.group === 'core' ? 10 : 9))
			.attr('font-weight', (d) => (d.group === 'core' ? '700' : '500'))
			.attr('fill', '#1e293b')
			.attr('pointer-events', 'none')
			.text((d) => d.label);

		let selectedD = null;

		function addPulseRings(grp, d) {
			if (prefersReducedMotion()) return;
			if (!grp.selectAll('.pulse-ring-anim').empty()) return;
			[0, 400, 800].forEach((delay) => {
				grp
					.append('circle')
					.attr('class', 'pulse-ring-anim')
					.attr('r', nodeR(d))
					.attr('fill', 'none')
					.attr('stroke', GROUP_COLOR[d.group])
					.attr('stroke-width', 2)
					.attr('opacity', 0.7)
					.attr('pointer-events', 'none')
					.style('animation', `ont-pulse 1.3s ease-out ${delay}ms infinite`);
			});
		}

		function applyDim(d) {
			linkSel.attr('opacity', (l) => (l.source.id === d.id || l.target.id === d.id ? 1 : 0.18));
			nodeSel.attr('opacity', (n) => {
				if (n.id === d.id) return 1;
				return links.some(
					(l) =>
						(l.source.id === d.id && l.target.id === n.id) ||
						(l.target.id === d.id && l.source.id === n.id),
				)
					? 1
					: 0.38;
			});
			linkTextSel.attr('opacity', (l) => (l.source.id === d.id || l.target.id === d.id ? 1 : 0));
		}

		function restoreAll() {
			linkSel.attr('opacity', 1);
			nodeSel.attr('opacity', 1);
			linkTextSel.attr('opacity', 0.6);
		}

		function clearSelectedD3() {
			if (!selectedD) return;
			nodeSel
				.filter((n) => n.id === selectedD.id)
				.selectAll('.pulse-ring-anim')
				.remove();
			nodeSel
				.filter((n) => n.id === selectedD.id)
				.select('.main-circle')
				.transition()
				.duration(motionDuration(200))
				.attr('r', nodeR(selectedD))
				.attr('fill', GROUP_COLOR[selectedD.group] + '22');
			selectedD = null;
			selectedNode = null;
			nodeSel.attr('aria-pressed', 'false');
			restoreAll();
		}

		const dragBehavior = drag()
			.on('start', (event, d) => {
				if (!event.active) simulation.alphaTarget(0.3).restart();
				d.fx = d.x;
				d.fy = d.y;
				select(event.sourceEvent.target.closest('g[cursor]')).attr('cursor', 'grabbing');
			})
			.on('drag', (event, d) => {
				d.fx = event.x;
				d.fy = event.y;
			})
			.on('end', (event, d) => {
				if (!event.active) simulation.alphaTarget(prefersReducedMotion() ? 0 : IDLE_ALPHA);
				d.fx = null;
				d.fy = null;
				select(event.sourceEvent.target.closest('g[cursor]')).attr('cursor', 'grab');
			});
		nodeSel.call(dragBehavior);

		nodeSel
			.on('mouseenter', (event, d) => {
				hoveredNode = d;
				tooltipX = event.clientX;
				tooltipY = event.clientY;
				if (selectedD?.id === d.id) return;
				const grp = select(event.currentTarget);
				grp
					.select('.main-circle')
					.transition()
					.duration(motionDuration(180))
					.attr('r', nodeR(d) * 1.45)
					.attr('fill', GROUP_COLOR[d.group] + '3a');
				addPulseRings(grp, d);
				if (!selectedD) applyDim(d);
			})
			.on('mousemove', (event) => {
				tooltipX = event.clientX;
				tooltipY = event.clientY;
			})
			.on('mouseleave', (event, d) => {
				hoveredNode = null;
				if (selectedD?.id === d.id) return;
				const grp = select(event.currentTarget);
				grp
					.select('.main-circle')
					.transition()
					.duration(motionDuration(200))
					.attr('r', nodeR(d))
					.attr('fill', GROUP_COLOR[d.group] + '22');
				grp.selectAll('.pulse-ring-anim').remove();
				if (selectedD) applyDim(selectedD);
				else restoreAll();
			})
			.on('click', (event, d) => {
				event.stopPropagation();
				if (selectedD?.id === d.id) {
					clearSelectedD3();
					return;
				}
				if (selectedD) {
					nodeSel
						.filter((n) => n.id === selectedD.id)
						.selectAll('.pulse-ring-anim')
						.remove();
					nodeSel
						.filter((n) => n.id === selectedD.id)
						.select('.main-circle')
						.transition()
						.duration(motionDuration(200))
						.attr('r', nodeR(selectedD))
						.attr('fill', GROUP_COLOR[selectedD.group] + '22');
				}
				selectedD = d;
				selectedNode = d;
				nodeSel.attr('aria-pressed', (node) => String(node.id === d.id));
				const grp = select(event.currentTarget);
				grp
					.select('.main-circle')
					.transition()
					.duration(motionDuration(180))
					.attr('r', nodeR(d) * 1.45)
					.attr('fill', GROUP_COLOR[d.group] + '3a');
				addPulseRings(grp, d);
				applyDim(d);
			})
			.on('keydown', activateOnKeyboard);

		svg.on('click', () => clearSelectedD3());

		function renderGraphPositions() {
			linkSel
				.attr('x1', (d) => d.source.x)
				.attr('y1', (d) => d.source.y)
				.attr('x2', (d) => d.target.x)
				.attr('y2', (d) => d.target.y);
			linkTextSel
				.attr('x', (d) => (d.source.x + d.target.x) / 2)
				.attr('y', (d) => (d.source.y + d.target.y) / 2);
			nodeSel.attr('transform', (d) => `translate(${d.x},${d.y})`);
		}
		simulation.on('tick', renderGraphPositions);
		if (prefersReducedMotion()) {
			simulation.stop();
			for (let index = 0; index < 300; index += 1) simulation.tick();
			renderGraphPositions();
		}

		loading = false;

		const fitTimer = setTimeout(
			() => {
				graphReady = true;
				graphFitFn();
				simulation.alphaTarget(prefersReducedMotion() ? 0 : IDLE_ALPHA);
			},
			prefersReducedMotion() ? 0 : 2400,
		);

		return () => {
			clearTimeout(fitTimer);
			simulation.stop();
			styleTag.remove();
		};
	}

	// Re-fit core ontology graph whenever the user returns to the graph tab
	$effect(() => {
		if (activeTab === 'graph' && graphReady && graphFitFn) {
			requestAnimationFrame(() => graphFitFn());
		}
	});

	// ══════════════════════════════════════════════════════════════════════════
	// DISASTER TAXONOMY TREE
	// ══════════════════════════════════════════════════════════════════════════
	let taxSvgEl = $state(null);
	let taxLoading = $state(true);
	let taxSelected = $state(null);

	const TAX_COLOR = {
		root: '#1e293b',
		natural: '#16a34a',
		tech: '#dc2626',
		biological: '#0d9488',
		climatological: '#d97706',
		extraterrestrial: '#7c3aed',
		geophysical: '#78716c',
		hydrological: '#2563eb',
		meteorological: '#0ea5e9',
		armedconflict: '#be123c',
		industrial: '#ea580c',
		miscellaneous: '#ca8a04',
		transport: '#475569',
	};

	function taxColor(d) {
		return TAX_COLOR[d.data?.group] ?? '#94a3b8';
	}

	$effect(() => {
		if (taxSvgEl) {
			const controller = new AbortController();
			taxLoading = true;
			taxSelected = null;
			void initTaxonomy(controller.signal).catch((requestError) => {
				if (requestError.name !== 'AbortError') taxLoading = false;
			});
			return () => controller.abort();
		}
	});

	async function initTaxonomy(signal) {
		const [{ hierarchy, tree }, { zoom, zoomIdentity }, { select }] = await Promise.all([
			import('d3-hierarchy'),
			import('d3-zoom'),
			import('d3-selection'),
		]);

		// Pulse keyframe — injected once, reused across tab switches
		if (!document.querySelector('#tax-pulse-style')) {
			const st = document.createElement('style');
			st.id = 'tax-pulse-style';
			st.textContent = `
				@keyframes tax-pulse {
					0%   { transform: scale(1);   opacity: 0.55; }
					100% { transform: scale(3.4); opacity: 0;    }
				}
				.tax-pr { transform-box: fill-box; transform-origin: center; }
			`;
			document.head.appendChild(st);
		}

		const treeData = await getOntologyTaxonomy({ signal });
		if (signal.aborted) return;
		taxLoading = false;

		const rect = taxSvgEl.getBoundingClientRect();
		const W = rect.width || 1100;
		const H = rect.height || 700;

		const root = hierarchy(treeData);
		// nodeSize gives each node a fixed slot regardless of total count
		// separation gives 2× gap when a leaf is adjacent to a branch node (e.g. ArmedConflict beside Transport)
		const treeLayout = tree()
			.nodeSize([32, 130])
			.separation((a, b) => (a.parent !== b.parent ? 2 : !a.children !== !b.children ? 2 : 1));
		treeLayout(root);

		// find horizontal extent so we can center the whole tree
		let xMin = Infinity,
			xMax = -Infinity;
		root.each((d) => {
			if (d.x < xMin) xMin = d.x;
			if (d.x > xMax) xMax = d.x;
		});

		const svg = select(taxSvgEl);
		const g = svg.append('g').attr('transform', `translate(${W / 2 - (xMin + xMax) / 2}, 60)`);

		const zoomBehavior = zoom()
			.scaleExtent([0.15, 3])
			.on('zoom', (e) => g.attr('transform', e.transform));
		svg.call(zoomBehavior).on('dblclick.zoom', null);

		// Links — cubic bezier vertical
		g.append('g')
			.selectAll('path')
			.data(root.links())
			.join('path')
			.attr('fill', 'none')
			.attr('stroke', (d) => taxColor(d.target) + '66')
			.attr('stroke-width', 1.5)
			.attr('d', (d) => {
				const my = (d.source.y + d.target.y) / 2;
				return `M${d.source.x},${d.source.y} C${d.source.x},${my} ${d.target.x},${my} ${d.target.x},${d.target.y}`;
			});

		// Nodes
		const nodeSel = g
			.append('g')
			.selectAll('g')
			.data(root.descendants())
			.join('g')
			.attr('transform', (d) => `translate(${d.x},${d.y})`)
			.attr('cursor', (d) => (d.data.id === 'root' ? 'default' : 'pointer'));

		const rScale = (d) => (d.depth === 0 ? 20 : d.depth === 1 ? 15 : d.depth === 2 ? 10 : 7);

		nodeSel
			.append('circle')
			.attr('class', 'tax-main')
			.attr('r', rScale)
			.attr('fill', (d) => taxColor(d) + '28')
			.attr('stroke', (d) => taxColor(d))
			.attr('stroke-width', (d) => (d.depth <= 1 ? 2.5 : 1.8));

		// Labels for branch nodes and shallow leaves (depth ≤ 2) — centered below circle
		// Armed Conflict is a leaf at depth 2, so it uses horizontal labels like its siblings
		nodeSel
			.filter((d) => !!d.children || d.depth <= 2)
			.append('text')
			.attr('text-anchor', 'middle')
			.attr('dy', (d) => rScale(d) + 12)
			.attr('font-size', (d) => (d.depth === 0 ? 11 : d.depth === 1 ? 10 : 9))
			.attr('font-weight', (d) => (d.depth <= 1 ? '700' : '600'))
			.attr('fill', (d) => taxColor(d))
			.attr('pointer-events', 'none')
			.text((d) => d.data.label);

		// Diagonal labels only for leaf nodes deeper than depth 2
		nodeSel
			.filter((d) => !d.children && d.depth > 2)
			.append('text')
			.attr('text-anchor', 'end')
			.attr('x', 0)
			.attr('y', (d) => rScale(d) + 4)
			.attr('transform', (d) => `rotate(-45, 0, ${rScale(d) + 4})`)
			.attr('font-size', 8)
			.attr('font-weight', '500')
			.attr('fill', (d) => taxColor(d))
			.attr('pointer-events', 'none')
			.text((d) => d.data.label);

		// Pulse helper
		function addTaxPulse(grp, d) {
			if (prefersReducedMotion()) return;
			if (!grp.selectAll('.tax-pr').empty()) return;
			[0, 380, 760].forEach((delay) =>
				grp
					.append('circle')
					.attr('class', 'tax-pr')
					.attr('r', rScale(d))
					.attr('fill', 'none')
					.attr('stroke', taxColor(d))
					.attr('stroke-width', 1.8)
					.attr('opacity', 0.55)
					.attr('pointer-events', 'none')
					.style('animation', `tax-pulse 1.3s ease-out ${delay}ms infinite`),
			);
		}

		function clearTaxSel(sel, d) {
			sel
				.select('circle.tax-main')
				.transition()
				.duration(motionDuration(180))
				.attr('r', rScale(d))
				.attr('fill', taxColor(d) + '28');
			sel.selectAll('.tax-pr').remove();
		}

		let taxSelectedD = null;

		// All nodes are interactive (including root which now has a definition)
		nodeSel
			.attr('cursor', 'pointer')
			.attr('role', 'button')
			.attr('tabindex', 0)
			.attr('aria-label', (d) => `${d.data.label}. ${d.data.definition ?? 'Disaster type'}`)
			.on('mouseenter', (event, d) => {
				if (taxSelectedD?.data.id === d.data.id) return;
				const grp = select(event.currentTarget);
				grp
					.select('circle.tax-main')
					.transition()
					.duration(motionDuration(160))
					.attr('r', rScale(d) * 1.4)
					.attr('fill', taxColor(d) + '50');
				addTaxPulse(grp, d);
			})
			.on('mouseleave', (event, d) => {
				if (taxSelectedD?.data.id === d.data.id) return;
				clearTaxSel(select(event.currentTarget), d);
			})
			.on('click', (event, d) => {
				event.stopPropagation();
				if (taxSelectedD?.data.id === d.data.id) {
					clearTaxSel(select(event.currentTarget), d);
					taxSelectedD = null;
					taxSelected = null;
					nodeSel.attr('aria-pressed', 'false');
					return;
				}
				if (taxSelectedD) {
					clearTaxSel(
						nodeSel.filter((n) => n.data.id === taxSelectedD.data.id),
						taxSelectedD,
					);
				}
				taxSelectedD = d;
				taxSelected = d.data;
				nodeSel.attr('aria-pressed', (node) => String(node.data.id === d.data.id));
				const grp = select(event.currentTarget);
				grp
					.select('circle.tax-main')
					.transition()
					.duration(motionDuration(160))
					.attr('r', rScale(d) * 1.4)
					.attr('fill', taxColor(d) + '50');
				addTaxPulse(grp, d);
			})
			.on('keydown', activateOnKeyboard);

		svg.on('click', () => {
			if (!taxSelectedD) return;
			clearTaxSel(
				nodeSel.filter((n) => n.data.id === taxSelectedD.data.id),
				taxSelectedD,
			);
			taxSelectedD = null;
			taxSelected = null;
			nodeSel.attr('aria-pressed', 'false');
		});

		// Auto-fit — extra bottom pad for diagonal leaf labels
		await tick();
		try {
			const bounds = g.node().getBBox();
			const padX = 60,
				padTop = 160,
				padBottom = 60;
			const scale = Math.min(
				W / (bounds.width + padX * 2),
				H / (bounds.height + padTop + padBottom),
				0.9,
			);
			const tx = (W - scale * bounds.width) / 2 - scale * bounds.x;
			const ty = padTop - scale * bounds.y;
			svg
				.transition()
				.duration(motionDuration(600))
				.call(zoomBehavior.transform, zoomIdentity.translate(tx, ty).scale(scale));
		} catch {}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// PSGC LOCATIONS FORCE GRAPH
	// ══════════════════════════════════════════════════════════════════════════
	let psgcSvgEl = $state(null);
	let psgcLoading = $state(true);
	let psgcSelected = $state(null);
	let psgcSim = null;

	const ISLAND_COLOR = {
		Luzon: '#0038a8',
		NCR: '#6d5c9b',
		Visayas: '#2f7d56',
		Mindanao: '#b46518',
	};

	$effect(() => {
		if (psgcSvgEl) {
			const controller = new AbortController();
			psgcLoading = true;
			psgcSelected = null;
			void initPsgc(controller.signal).catch((requestError) => {
				if (requestError.name !== 'AbortError') psgcLoading = false;
			});
			return () => {
				controller.abort();
				if (psgcSim) {
					psgcSim.stop();
					psgcSim = null;
				}
			};
		}
	});

	async function initPsgc(signal) {
		const [
			{ forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide, forceX, forceY },
			{ drag },
			{ zoom, zoomIdentity },
			{ select },
		] = await Promise.all([
			import('d3-force'),
			import('d3-drag'),
			import('d3-zoom'),
			import('d3-selection'),
		]);

		const data = await getOntologyPsgc({ signal });
		if (signal.aborted) return;
		psgcLoading = false;

		const nodes = data.nodes.map((n) => ({ ...n }));
		const nodeById = new Map(nodes.map((n) => [n.id, n]));
		const links = data.links.map((l) => ({
			...l,
			isCity: nodeById.get(l.source)?.level === 'City',
		}));

		const rect = psgcSvgEl.getBoundingClientRect();
		const W = rect.width || 900;
		const H = rect.height || 700;

		const svg = select(psgcSvgEl);
		const g = svg.append('g');

		const zoomBehavior = zoom()
			.scaleExtent([0.2, 4])
			.filter((e) => e.target === psgcSvgEl || e.target.tagName === 'svg' || e.type === 'wheel')
			.on('zoom', (e) => g.attr('transform', e.transform));
		svg.call(zoomBehavior).on('dblclick.zoom', null);

		// Island X anchors — cluster by island group
		const ISLAND_X = { Luzon: W * 0.22, NCR: W * 0.36, Visayas: W * 0.55, Mindanao: W * 0.78 };
		const ISLAND_Y = { Luzon: H * 0.45, NCR: H * 0.45, Visayas: H * 0.45, Mindanao: H * 0.45 };

		psgcSim = forceSimulation(nodes)
			.force(
				'link',
				forceLink(links)
					.id((d) => d.id)
					.distance((l) => (l.isCity ? 45 : 60))
					.strength(0.7),
			)
			.force(
				'charge',
				forceManyBody().strength((d) =>
					d.level === 'Region' ? -350 : d.level === 'City' ? -40 : -80,
				),
			)
			.force('center', forceCenter(W / 2, H / 2).strength(0.02))
			.force(
				'collide',
				forceCollide((d) => (d.level === 'Region' ? 32 : d.level === 'City' ? 11 : 18)),
			)
			.force('x', forceX((d) => ISLAND_X[d.island] ?? W / 2).strength(0.25))
			.force('y', forceY((d) => ISLAND_Y[d.island] ?? H / 2).strength(0.1))
			.alphaDecay(0.02)
			.velocityDecay(0.35);
		const simulation = psgcSim;

		// Links — city links are thinner and dashed to show direct-to-region independence
		const linkSel = g
			.append('g')
			.selectAll('line')
			.data(links)
			.join('line')
			.attr('stroke', (l) => (l.isCity ? '#94a3b8' : '#cbd5e1'))
			.attr('stroke-width', (l) => (l.isCity ? 0.5 : 0.8))
			.attr('stroke-dasharray', (l) => (l.isCity ? '3,2' : '0'))
			.attr('opacity', (l) => (l.isCity ? 0.3 : 0.5));

		// Node groups
		const nodeSel = g
			.append('g')
			.selectAll('g')
			.data(nodes)
			.join('g')
			.attr('cursor', 'pointer')
			.attr('role', 'button')
			.attr('tabindex', 0)
			.attr('aria-label', (d) => `${d.fullName ?? d.label}. ${d.level ?? 'PSGC location'}`);

		const psgcR = (d) => (d.level === 'Region' ? 20 : d.level === 'City' ? 6 : 10);
		const psgcNodeColor = (d) => ISLAND_COLOR[d.island] ?? '#94a3b8';

		nodeSel
			.append('circle')
			.attr('class', 'psgc-circle')
			.attr('r', psgcR)
			.attr('fill', (d) => psgcNodeColor(d) + (d.level === 'Region' ? '30' : '20'))
			.attr('stroke', (d) => psgcNodeColor(d))
			.attr('stroke-width', (d) => (d.level === 'Region' ? 2.5 : d.level === 'City' ? 1.2 : 1.5))
			.attr('stroke-dasharray', (d) => (d.level === 'City' ? '3,2' : '0'));

		nodeSel
			.append('text')
			.attr('text-anchor', 'middle')
			.attr('dy', (d) => psgcR(d) + (d.level === 'City' ? 9 : 11))
			.attr('font-size', (d) => (d.level === 'Region' ? 9 : d.level === 'City' ? 7 : 8))
			.attr('font-weight', (d) => (d.level === 'Region' ? '700' : '400'))
			.attr('font-style', (d) => (d.level === 'City' ? 'italic' : 'normal'))
			.attr('fill', (d) => (d.level === 'Region' ? psgcNodeColor(d) : '#475569'))
			.attr('pointer-events', 'none')
			.text((d) => d.label);

		// Drag
		let psgcSelectedD = null;

		const dragBehavior = drag()
			.on('start', (event, d) => {
				if (!event.active) simulation.alphaTarget(0.3).restart();
				d.fx = d.x;
				d.fy = d.y;
			})
			.on('drag', (event, d) => {
				d.fx = event.x;
				d.fy = event.y;
			})
			.on('end', (event, d) => {
				if (!event.active) simulation.alphaTarget(0);
				d.fx = null;
				d.fy = null;
			});
		nodeSel.call(dragBehavior);

		// Hover + click
		nodeSel
			.on('mouseenter', (event, d) => {
				if (psgcSelectedD?.id === d.id) return;
				select(event.currentTarget)
					.select('.psgc-circle')
					.transition()
					.duration(motionDuration(150))
					.attr('r', psgcR(d) * 1.4)
					.attr('fill', psgcNodeColor(d) + '55');
				// Dim others
				nodeSel.attr('opacity', (n) => (n.id === d.id ? 1 : 0.38));
				const pid = d.regionId ?? d.id;
				linkSel.attr('opacity', (l) =>
					l.source.id === pid || l.target.id === pid || l.source.id === d.id || l.target.id === d.id
						? 0.9
						: 0.08,
				);
			})
			.on('mouseleave', (event, d) => {
				if (psgcSelectedD?.id === d.id) return;
				select(event.currentTarget)
					.select('.psgc-circle')
					.transition()
					.duration(motionDuration(150))
					.attr('r', psgcR(d))
					.attr('fill', psgcNodeColor(d) + (d.level === 'Region' ? '30' : '20'));
				nodeSel.attr('opacity', 1);
				linkSel.attr('opacity', 0.5);
			})
			.on('click', (event, d) => {
				event.stopPropagation();
				if (psgcSelectedD?.id === d.id) {
					select(event.currentTarget)
						.select('.psgc-circle')
						.transition()
						.duration(motionDuration(150))
						.attr('r', psgcR(d))
						.attr('fill', psgcNodeColor(d) + (d.level === 'Region' ? '30' : '20'));
					psgcSelectedD = null;
					psgcSelected = null;
					nodeSel.attr('aria-pressed', 'false');
					nodeSel.attr('opacity', 1);
					linkSel.attr('opacity', 0.5);
					return;
				}
				if (psgcSelectedD) {
					nodeSel
						.filter((n) => n.id === psgcSelectedD.id)
						.select('.psgc-circle')
						.transition()
						.duration(motionDuration(150))
						.attr('r', psgcR(psgcSelectedD))
						.attr(
							'fill',
							psgcNodeColor(psgcSelectedD) + (psgcSelectedD.level === 'Region' ? '30' : '20'),
						);
				}
				psgcSelectedD = d;
				psgcSelected = d;
				nodeSel.attr('aria-pressed', (node) => String(node.id === d.id));
				select(event.currentTarget)
					.select('.psgc-circle')
					.transition()
					.duration(motionDuration(150))
					.attr('r', psgcR(d) * 1.4)
					.attr('fill', psgcNodeColor(d) + '55');
			})
			.on('keydown', activateOnKeyboard);

		svg.on('click', () => {
			if (psgcSelectedD) {
				nodeSel
					.filter((n) => n.id === psgcSelectedD.id)
					.select('.psgc-circle')
					.transition()
					.duration(motionDuration(150))
					.attr('r', psgcR(psgcSelectedD))
					.attr(
						'fill',
						psgcNodeColor(psgcSelectedD) + (psgcSelectedD.level === 'Region' ? '30' : '20'),
					);
				psgcSelectedD = null;
				psgcSelected = null;
				nodeSel.attr('aria-pressed', 'false');
				nodeSel.attr('opacity', 1);
				linkSel.attr('opacity', 0.5);
			}
		});

		function renderPsgcPositions() {
			linkSel
				.attr('x1', (d) => d.source.x)
				.attr('y1', (d) => d.source.y)
				.attr('x2', (d) => d.target.x)
				.attr('y2', (d) => d.target.y);
			nodeSel.attr('transform', (d) => `translate(${d.x},${d.y})`);
		}
		simulation.on('tick', renderPsgcPositions);
		if (prefersReducedMotion()) {
			simulation.stop();
			for (let index = 0; index < 300; index += 1) simulation.tick();
			renderPsgcPositions();
		}

		// Auto-fit after settling
		setTimeout(
			() => {
				simulation.alphaTarget(0);
				try {
					const bounds = g.node().getBBox();
					const pad = 40;
					const scale = Math.min(W / (bounds.width + pad * 2), H / (bounds.height + pad * 2), 1);
					const tx = (W - scale * (bounds.width + pad * 2)) / 2 - scale * (bounds.x - pad);
					const ty = (H - scale * (bounds.height + pad * 2)) / 2 - scale * (bounds.y - pad);
					svg
						.transition()
						.duration(motionDuration(700))
						.call(zoomBehavior.transform, zoomIdentity.translate(tx, ty).scale(scale));
				} catch {}
			},
			prefersReducedMotion() ? 0 : 3000,
		);
	}
</script>

<svelte:head>
	<title>Ontology · SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

<!-- Cursor tooltip for class graph -->
{#if activeTab === 'graph' && hoveredNode && selectedNode?.id !== hoveredNode?.id}
	<div class="ontology-tooltip" style="left:{tooltipX + 14}px; top:{tooltipY - 10}px;">
		<p>{hoveredNode.label}</p>
		<small>{hoveredNode.definition}</small>
	</div>
{/if}

<main class="ontology-page">
	<header class="ontology-intro">
		<div class="intro-copy">
			<p class="workspace-kicker">Schema atlas · OWL 2</p>
			<h1>See how disaster knowledge connects.</h1>
			<p class="intro-summary">
				Explore classes, hazard categories, and Philippine places—and the links that make every
				record explainable.
			</p>
		</div>

		<aside class="atlas-note" aria-label="About the ontology explorer">
			<span aria-hidden="true">03</span>
			<div>
				<p>Connected lenses</p>
				<small>Class model, disaster taxonomy, and PSGC geography in one research view.</small>
			</div>
		</aside>
	</header>

	<section class="ontology-workbench" aria-labelledby="ontology-view-title">
		<header class="workbench-header">
			<div class="view-copy">
				<p><span>{activeViewNumber}</span> {activeView.kicker}</p>
				<h2 id="ontology-view-title">{activeView.title}</h2>
				<p>{activeView.description}</p>
			</div>

			<div class="workbench-controls">
				<OntologyTabs tabs={TABS} active={activeTab} onChange={(tab) => (activeTab = tab)} />
				<p class="interaction-guide">
					<span>
						<svg aria-hidden="true" viewBox="0 0 24 24" fill="none">
							<path d="m7 3 10 8-5 1.5L10 18Z" />
						</svg>
						Select a node
					</span>
					<i aria-hidden="true"></i>
					<span>Drag or scroll to navigate</span>
				</p>
			</div>
		</header>

		<div class="graph-stage">
			<CoreOntologyPanel
				active={activeTab === 'graph'}
				{loading}
				{selectedNode}
				groupColors={GROUP_COLOR}
				groupLabels={GROUP_LABEL}
				bind:svgElement={svgEl}
			/>

			<TaxonomyPanel
				active={activeTab === 'taxonomy'}
				loading={taxLoading}
				selectedNode={taxSelected}
				colors={TAX_COLOR}
				bind:svgElement={taxSvgEl}
			/>

			<PsgcPanel
				active={activeTab === 'psgc'}
				loading={psgcLoading}
				selectedNode={psgcSelected}
				islandColors={ISLAND_COLOR}
				bind:svgElement={psgcSvgEl}
			/>
		</div>
	</section>
</main>

<style>
	.ontology-page {
		position: relative;
		z-index: 1;
		width: min(100%, 90rem);
		min-height: calc(100dvh - var(--app-nav-height));
		margin: 0 auto;
		padding: clamp(1.5rem, 3.5vw, 3.5rem) clamp(1rem, 3vw, 2.5rem) 3rem;
	}

	.ontology-intro {
		display: grid;
		grid-template-columns: minmax(0, 1.45fr) minmax(16rem, 0.55fr);
		align-items: end;
		gap: clamp(2rem, 6vw, 7rem);
		padding: 0.5rem clamp(0rem, 2vw, 1.5rem) clamp(2rem, 4vw, 3.5rem);
	}

	.intro-copy {
		max-width: 52rem;
	}

	.intro-copy h1 {
		max-width: 56rem;
		margin: 0.65rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(2.5rem, 4.3vw, 4rem);
		font-weight: 900;
		line-height: 0.98;
		letter-spacing: -0.045em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.intro-summary {
		max-width: 44rem;
		margin: 1.25rem 0 0;
		font-size: clamp(0.875rem, 1.4vw, 1rem);
		line-height: 1.7;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.atlas-note {
		display: grid;
		grid-template-columns: auto minmax(0, 1fr);
		gap: 1rem;
		border-top: 1px solid var(--color-border);
		padding-top: 1rem;
	}

	.atlas-note > span {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 1.5rem;
		font-weight: 600;
		line-height: 1;
		color: var(--color-brand);
	}

	.atlas-note p,
	.atlas-note small {
		margin: 0;
	}

	.atlas-note p {
		font-size: 0.75rem;
		font-weight: 700;
		line-height: 1.3;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-text);
	}

	.atlas-note small {
		display: block;
		margin-top: 0.35rem;
		font-size: 0.75rem;
		line-height: 1.55;
		color: var(--color-text-muted);
	}

	.ontology-workbench {
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-surface);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
	}

	.workbench-header {
		display: grid;
		grid-template-columns: minmax(17rem, 0.8fr) minmax(28rem, 1.2fr);
		align-items: end;
		gap: 2rem;
		border-bottom: 1px solid var(--color-border);
		background: var(--color-canvas);
		padding: 1.25rem 1.5rem 1.35rem;
	}

	.view-copy > p:first-child {
		margin: 0;
		font-size: 0.6875rem;
		font-weight: 700;
		line-height: 1rem;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.view-copy > p:first-child span {
		margin-right: 0.45rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
	}

	.view-copy h2 {
		margin: 0.25rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.45rem, 2.2vw, 2rem);
		font-weight: 900;
		line-height: 1.1;
		letter-spacing: -0.025em;
		color: var(--color-text);
	}

	.view-copy > p:last-child {
		max-width: 34rem;
		margin: 0.55rem 0 0;
		font-size: 0.75rem;
		line-height: 1.55;
		color: var(--color-text-muted);
		text-wrap: pretty;
	}

	.workbench-controls {
		display: grid;
		gap: 0.75rem;
		justify-items: end;
	}

	.interaction-guide {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		margin: 0;
		font-size: 0.6875rem;
		line-height: 1.4;
		color: var(--color-text-muted);
	}

	.interaction-guide span {
		display: inline-flex;
		align-items: center;
		gap: 0.35rem;
	}

	.interaction-guide svg {
		width: 0.9rem;
		height: 0.9rem;
		color: var(--color-brand);
	}

	.interaction-guide path {
		stroke: currentColor;
		stroke-width: 1.7;
		stroke-linecap: round;
		stroke-linejoin: round;
	}

	.interaction-guide i {
		width: 1px;
		height: 0.75rem;
		background: var(--color-border);
	}

	.graph-stage {
		position: relative;
		height: clamp(34rem, 64dvh, 48rem);
		min-height: 34rem;
		overflow: hidden;
		background-color: #fcfdff;
		background-image:
			linear-gradient(var(--color-brand-soft) 1px, transparent 1px),
			linear-gradient(90deg, var(--color-brand-soft) 1px, transparent 1px);
		background-size: 2.5rem 2.5rem;
	}

	.graph-stage :global(g[role='button']:focus) {
		outline: none !important;
		outline-offset: 0 !important;
		box-shadow: none !important;
	}

	.graph-stage :global(g[role='button']:focus > circle:first-of-type) {
		stroke: var(--color-focus) !important;
		stroke-width: 3.5px !important;
	}

	.graph-stage :global(g[role='button'][aria-pressed='true'] > circle:first-of-type) {
		fill: var(--color-accent-soft) !important;
		stroke: var(--color-brand) !important;
		stroke-width: 3px !important;
	}

	.ontology-tooltip {
		position: fixed;
		z-index: 50;
		max-width: 19rem;
		pointer-events: none;
		border: 1px solid #334155;
		border-radius: var(--radius-control);
		background: #1e293b;
		padding: 0.65rem 0.8rem;
		box-shadow: var(--shadow-control);
		color: white;
	}

	.ontology-tooltip p,
	.ontology-tooltip small {
		margin: 0;
	}

	.ontology-tooltip p {
		font-size: 0.75rem;
		font-weight: 700;
	}

	.ontology-tooltip small {
		display: block;
		margin-top: 0.2rem;
		font-size: 0.6875rem;
		line-height: 1.45;
		color: #cbd5e1;
	}

	@media (max-width: 900px) {
		.ontology-intro {
			grid-template-columns: 1fr;
			gap: 1.5rem;
		}

		.atlas-note {
			max-width: 34rem;
		}

		.workbench-header {
			grid-template-columns: 1fr;
			gap: 1.25rem;
		}

		.workbench-controls {
			justify-items: stretch;
		}

		.interaction-guide {
			justify-content: flex-end;
		}
	}

	@media (max-width: 639px) {
		.ontology-page {
			padding: 1.25rem 0.75rem 1.5rem;
		}

		.ontology-intro {
			padding: 0.25rem 0.5rem 2rem;
		}

		.intro-copy h1 {
			font-size: clamp(2.35rem, 11vw, 3rem);
			line-height: 1;
		}

		.intro-summary {
			margin-top: 1rem;
		}

		.atlas-note {
			display: none;
		}

		.workbench-header {
			gap: 1rem;
			padding: 1rem 0.875rem;
		}

		.view-copy {
			padding-inline: 0.125rem;
		}

		.view-copy > p:last-child {
			display: none;
		}

		.interaction-guide {
			justify-content: flex-start;
			padding-inline: 0.125rem;
		}

		.interaction-guide span:last-child,
		.interaction-guide i {
			display: none;
		}

		.graph-stage {
			height: max(34rem, 68dvh);
			background-size: 2rem 2rem;
		}
	}

	@media (max-height: 560px) and (orientation: landscape) {
		.graph-stage {
			height: 30rem;
			min-height: 30rem;
		}
	}

	@media (forced-colors: active) {
		.ontology-workbench,
		.graph-stage {
			border-color: CanvasText;
			background: Canvas;
		}

		.graph-stage :global(g[role='button']:focus > circle:first-of-type),
		.graph-stage :global(g[role='button'][aria-pressed='true'] > circle:first-of-type) {
			fill: Highlight !important;
			stroke: HighlightText !important;
		}
	}
</style>
