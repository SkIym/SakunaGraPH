<script>
	import { onMount, tick } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import { apiUrl } from '$lib/api.js';

	// ── Tab state ─────────────────────────────────────────────────────────────
	let activeTab = $state('graph');
	const TABS = [
		{ id: 'graph',    label: 'Core Ontology' },
		{ id: 'taxonomy', label: 'Disaster Taxonomy' },
		{ id: 'psgc',     label: 'PSGC Locations' }
	];

	// ══════════════════════════════════════════════════════════════════════════
	// CLASS GRAPH
	// ══════════════════════════════════════════════════════════════════════════
	let svgEl        = $state(null);
	let loading      = $state(true);
	let hoveredNode  = $state(null);
	let selectedNode = $state(null);
	let tooltipX     = $state(0);
	let tooltipY     = $state(0);
	// Re-center function exposed so the $effect below can call it on tab switch
	let graphFitFn = null;
	let graphReady  = false;

	const GROUP_COLOR = {
		core:         '#ef4444',
		impact:       '#f97316',
		response:     '#22c55e',
		preparedness: '#3b82f6',
		location:     '#14b8a6',
		type:         '#a855f7',
		source:       '#94a3b8'
	};
	const GROUP_LABEL = {
		core:         'Core Event',
		impact:       'Impact',
		response:     'Response',
		preparedness: 'Preparedness',
		location:     'Location',
		type:         'Disaster Type',
		source:       'Source'
	};
	const LINK_DASH  = { subClassOf: '0', objectProperty: '5 3' };
	const LINK_COLOR = { subClassOf: '#cbd5e1', objectProperty: '#93c5fd' };
	const IDLE_ALPHA = 0.05;

	function nodeR(d) { return d.group === 'core' ? 22 : 15; }

	onMount(async () => {
		const [
			{ forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide },
			{ drag },
			{ zoom, zoomIdentity },
			{ select }
		] = await Promise.all([
			import('d3-force'),
			import('d3-drag'),
			import('d3-zoom'),
			import('d3-selection')
		]);

		const res = await fetch(apiUrl('/api/ontology/graph'));
		const graphData = await res.json();
		const nodes = graphData.nodes.map(n => ({ ...n }));
		const links = graphData.links.map(l => ({ ...l }));

		const W = svgEl.clientWidth  || 900;
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
		const g   = svg.append('g');

		const zoomBehavior = zoom()
			.scaleExtent([0.2, 3])
			.filter(event => event.target === svgEl || event.target.tagName === 'svg' || event.type === 'wheel')
			.on('zoom', event => g.attr('transform', event.transform));
		svg.call(zoomBehavior).on('dblclick.zoom', null);

		// Stored so the $effect can re-center when returning to this tab
		graphFitFn = () => {
			try {
				const bounds = g.node().getBBox();
				if (!bounds.width && !bounds.height) return;
				const pad = 48;
				const s  = Math.min(W / (bounds.width + pad * 2), H / (bounds.height + pad * 2), 1);
				const tx = (W - s * (bounds.width  + pad * 2)) / 2 - s * (bounds.x - pad);
				const ty = (H - s * (bounds.height + pad * 2)) / 2 - s * (bounds.y - pad);
				svg.transition().duration(450)
					.call(zoomBehavior.transform, zoomIdentity.translate(tx, ty).scale(s));
			} catch {}
		};

		const defs = svg.append('defs');
		['subClassOf', 'objectProperty'].forEach(type => {
			defs.append('marker')
				.attr('id',          `arrow-${type}`)
				.attr('viewBox',     '0 -4 8 8')
				.attr('refX',        22).attr('refY', 0)
				.attr('markerWidth', 6).attr('markerHeight', 6)
				.attr('orient',      'auto')
				.append('path')
				.attr('d',    'M0,-4L8,0L0,4')
				.attr('fill', LINK_COLOR[type]);
		});

		const simulation = forceSimulation(nodes)
			.force('link',    forceLink(links).id(d => d.id).distance(140).strength(0.5))
			.force('charge',  forceManyBody().strength(-500))
			.force('center',  forceCenter(W / 2, H / 2).strength(0.06))
			.force('collide', forceCollide(46))
			.alphaDecay(0.018)
			.velocityDecay(0.28);

		const linkSel = g.append('g')
			.selectAll('line').data(links).join('line')
			.attr('stroke',           d => LINK_COLOR[d.type])
			.attr('stroke-width',     d => d.type === 'subClassOf' ? 1.2 : 1.5)
			.attr('stroke-dasharray', d => LINK_DASH[d.type])
			.attr('marker-end',       d => `url(#arrow-${d.type})`);

		const linkTextSel = g.append('g')
			.selectAll('text')
			.data(links.filter(l => l.type === 'objectProperty'))
			.join('text')
			.attr('font-size', 8).attr('fill', '#94a3b8')
			.attr('text-anchor', 'middle').attr('pointer-events', 'none')
			.attr('opacity', 0.6).text(d => d.label);

		const nodeSel = g.append('g')
			.selectAll('g').data(nodes).join('g')
			.attr('cursor', 'grab');

		nodeSel.append('circle').attr('class', 'main-circle')
			.attr('r',            d => nodeR(d))
			.attr('fill',         d => GROUP_COLOR[d.group] + '22')
			.attr('stroke',       d => GROUP_COLOR[d.group])
			.attr('stroke-width', d => d.group === 'core' ? 2.5 : 1.8);

		nodeSel.append('text')
			.attr('text-anchor', 'middle').attr('dy', '0.35em')
			.attr('font-size',   d => d.group === 'core' ? 13 : 9)
			.attr('font-weight', '700').attr('fill', d => GROUP_COLOR[d.group])
			.attr('pointer-events', 'none').text(d => d.label.charAt(0));

		nodeSel.append('text')
			.attr('text-anchor', 'middle')
			.attr('dy',          d => d.group === 'core' ? 34 : 27)
			.attr('font-size',   d => d.group === 'core' ? 10 : 9)
			.attr('font-weight', d => d.group === 'core' ? '700' : '500')
			.attr('fill', '#1e293b').attr('pointer-events', 'none')
			.text(d => d.label);

		let selectedD = null;

		function addPulseRings(grp, d) {
			if (!grp.selectAll('.pulse-ring-anim').empty()) return;
			[0, 400, 800].forEach(delay => {
				grp.append('circle')
					.attr('class',        'pulse-ring-anim')
					.attr('r',            nodeR(d))
					.attr('fill',         'none')
					.attr('stroke',       GROUP_COLOR[d.group])
					.attr('stroke-width', 2)
					.attr('opacity',      0.7)
					.attr('pointer-events', 'none')
					.style('animation',   `ont-pulse 1.3s ease-out ${delay}ms infinite`);
			});
		}

		function applyDim(d) {
			linkSel.attr('opacity', l =>
				l.source.id === d.id || l.target.id === d.id ? 1 : 0.18);
			nodeSel.attr('opacity', n => {
				if (n.id === d.id) return 1;
				return links.some(l =>
					(l.source.id === d.id && l.target.id === n.id) ||
					(l.target.id === d.id && l.source.id === n.id)) ? 1 : 0.38;
			});
			linkTextSel.attr('opacity', l =>
				l.source.id === d.id || l.target.id === d.id ? 1 : 0);
		}

		function restoreAll() {
			linkSel.attr('opacity', 1);
			nodeSel.attr('opacity', 1);
			linkTextSel.attr('opacity', 0.6);
		}

		function clearSelectedD3() {
			if (!selectedD) return;
			nodeSel.filter(n => n.id === selectedD.id)
				.selectAll('.pulse-ring-anim').remove();
			nodeSel.filter(n => n.id === selectedD.id)
				.select('.main-circle')
				.transition().duration(200)
				.attr('r',    nodeR(selectedD))
				.attr('fill', GROUP_COLOR[selectedD.group] + '22');
			selectedD = null;
			selectedNode = null;
			restoreAll();
		}

		const dragBehavior = drag()
			.on('start', (event, d) => {
				if (!event.active) simulation.alphaTarget(0.3).restart();
				d.fx = d.x; d.fy = d.y;
				select(event.sourceEvent.target.closest('g[cursor]'))
					.attr('cursor', 'grabbing');
			})
			.on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
			.on('end', (event, d) => {
				if (!event.active) simulation.alphaTarget(IDLE_ALPHA);
				d.fx = null; d.fy = null;
				select(event.sourceEvent.target.closest('g[cursor]'))
					.attr('cursor', 'grab');
			});
		nodeSel.call(dragBehavior);

		nodeSel
			.on('mouseenter', (event, d) => {
				hoveredNode = d;
				tooltipX = event.clientX;
				tooltipY = event.clientY;
				if (selectedD?.id === d.id) return;
				const grp = select(event.currentTarget);
				grp.select('.main-circle')
					.transition().duration(180)
					.attr('r', nodeR(d) * 1.45)
					.attr('fill', GROUP_COLOR[d.group] + '3a');
				addPulseRings(grp, d);
				if (!selectedD) applyDim(d);
			})
			.on('mousemove', event => {
				tooltipX = event.clientX;
				tooltipY = event.clientY;
			})
			.on('mouseleave', (event, d) => {
				hoveredNode = null;
				if (selectedD?.id === d.id) return;
				const grp = select(event.currentTarget);
				grp.select('.main-circle')
					.transition().duration(200)
					.attr('r',    nodeR(d))
					.attr('fill', GROUP_COLOR[d.group] + '22');
				grp.selectAll('.pulse-ring-anim').remove();
				if (selectedD) applyDim(selectedD);
				else restoreAll();
			})
			.on('click', (event, d) => {
				event.stopPropagation();
				if (selectedD?.id === d.id) { clearSelectedD3(); return; }
				if (selectedD) {
					nodeSel.filter(n => n.id === selectedD.id)
						.selectAll('.pulse-ring-anim').remove();
					nodeSel.filter(n => n.id === selectedD.id)
						.select('.main-circle')
						.transition().duration(200)
						.attr('r',    nodeR(selectedD))
						.attr('fill', GROUP_COLOR[selectedD.group] + '22');
				}
				selectedD = d;
				selectedNode = d;
				const grp = select(event.currentTarget);
				grp.select('.main-circle')
					.transition().duration(180)
					.attr('r', nodeR(d) * 1.45)
					.attr('fill', GROUP_COLOR[d.group] + '3a');
				addPulseRings(grp, d);
				applyDim(d);
			});

		svg.on('click', () => clearSelectedD3());

		simulation.on('tick', () => {
			linkSel
				.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
				.attr('x2', d => d.target.x).attr('y2', d => d.target.y);
			linkTextSel
				.attr('x', d => (d.source.x + d.target.x) / 2)
				.attr('y', d => (d.source.y + d.target.y) / 2);
			nodeSel.attr('transform', d => `translate(${d.x},${d.y})`);
		});

		loading = false;

		setTimeout(() => {
			graphReady = true;
			graphFitFn();
			simulation.alphaTarget(IDLE_ALPHA);
		}, 2400);

		return () => { simulation.stop(); styleTag.remove(); };
	});

	// Re-fit core ontology graph whenever the user returns to the graph tab
	$effect(() => {
		if (activeTab === 'graph' && graphReady && graphFitFn) {
			requestAnimationFrame(() => graphFitFn());
		}
	});

	// ══════════════════════════════════════════════════════════════════════════
	// DISASTER TAXONOMY TREE
	// ══════════════════════════════════════════════════════════════════════════
	let taxSvgEl    = $state(null);
	let taxLoading  = $state(true);
	let taxSelected = $state(null);

	const TAX_COLOR = {
		root:            '#1e293b',
		natural:         '#16a34a',
		tech:            '#dc2626',
		biological:      '#0d9488',
		climatological:  '#d97706',
		extraterrestrial:'#7c3aed',
		geophysical:     '#78716c',
		hydrological:    '#2563eb',
		meteorological:  '#0ea5e9',
		armedconflict:   '#be123c',
		industrial:      '#ea580c',
		miscellaneous:   '#ca8a04',
		transport:       '#475569'
	};

	function taxColor(d) { return TAX_COLOR[d.data?.group] ?? '#94a3b8'; }

	$effect(() => {
		if (taxSvgEl) {
			taxLoading = true;
			taxSelected = null;
			initTaxonomy();
		}
	});

	async function initTaxonomy() {
		const [
			{ hierarchy, tree },
			{ zoom, zoomIdentity },
			{ select }
		] = await Promise.all([
			import('d3-hierarchy'),
			import('d3-zoom'),
			import('d3-selection')
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

		const res = await fetch(apiUrl('/api/ontology/taxonomy'));
		const treeData = await res.json();
		taxLoading = false;

		const rect = taxSvgEl.getBoundingClientRect();
		const W = rect.width  || 1100;
		const H = rect.height || 700;

		const root = hierarchy(treeData);
		// nodeSize gives each node a fixed slot regardless of total count
		// separation gives 2× gap when a leaf is adjacent to a branch node (e.g. ArmedConflict beside Transport)
		const treeLayout = tree()
			.nodeSize([32, 130])
			.separation((a, b) => a.parent !== b.parent ? 2 : (!a.children !== !b.children) ? 2 : 1);
		treeLayout(root);

		// find horizontal extent so we can center the whole tree
		let xMin = Infinity, xMax = -Infinity;
		root.each(d => { if (d.x < xMin) xMin = d.x; if (d.x > xMax) xMax = d.x; });

		const svg = select(taxSvgEl);
		const g   = svg.append('g').attr('transform', `translate(${W / 2 - (xMin + xMax) / 2}, 60)`);

		const zoomBehavior = zoom()
			.scaleExtent([0.15, 3])
			.on('zoom', e => g.attr('transform', e.transform));
		svg.call(zoomBehavior).on('dblclick.zoom', null);

		// Links — cubic bezier vertical
		g.append('g').selectAll('path')
			.data(root.links())
			.join('path')
			.attr('fill', 'none')
			.attr('stroke', d => taxColor(d.target) + '66')
			.attr('stroke-width', 1.5)
			.attr('d', d => {
				const my = (d.source.y + d.target.y) / 2;
				return `M${d.source.x},${d.source.y} C${d.source.x},${my} ${d.target.x},${my} ${d.target.x},${d.target.y}`;
			});

		// Nodes
		const nodeSel = g.append('g')
			.selectAll('g')
			.data(root.descendants())
			.join('g')
			.attr('transform', d => `translate(${d.x},${d.y})`)
			.attr('cursor', d => d.data.id === 'root' ? 'default' : 'pointer');

		const rScale = d => d.depth === 0 ? 20 : d.depth === 1 ? 15 : d.depth === 2 ? 10 : 7;

		nodeSel.append('circle').attr('class', 'tax-main')
			.attr('r',            rScale)
			.attr('fill',         d => taxColor(d) + '28')
			.attr('stroke',       d => taxColor(d))
			.attr('stroke-width', d => d.depth <= 1 ? 2.5 : 1.8);

		// Labels for branch nodes and shallow leaves (depth ≤ 2) — centered below circle
		// Armed Conflict is a leaf at depth 2, so it uses horizontal labels like its siblings
		nodeSel.filter(d => !!d.children || d.depth <= 2)
			.append('text')
			.attr('text-anchor', 'middle')
			.attr('dy', d => rScale(d) + 12)
			.attr('font-size',   d => d.depth === 0 ? 11 : d.depth === 1 ? 10 : 9)
			.attr('font-weight', d => d.depth <= 1 ? '700' : '600')
			.attr('fill', d => taxColor(d))
			.attr('pointer-events', 'none')
			.text(d => d.data.label);

		// Diagonal labels only for leaf nodes deeper than depth 2
		nodeSel.filter(d => !d.children && d.depth > 2)
			.append('text')
			.attr('text-anchor', 'end')
			.attr('x', 0)
			.attr('y', d => rScale(d) + 4)
			.attr('transform', d => `rotate(-45, 0, ${rScale(d) + 4})`)
			.attr('font-size', 8)
			.attr('font-weight', '500')
			.attr('fill', d => taxColor(d))
			.attr('pointer-events', 'none')
			.text(d => d.data.label);

		// Pulse helper
		function addTaxPulse(grp, d) {
			if (!grp.selectAll('.tax-pr').empty()) return;
			[0, 380, 760].forEach(delay =>
				grp.append('circle').attr('class', 'tax-pr')
					.attr('r',            rScale(d))
					.attr('fill',         'none')
					.attr('stroke',       taxColor(d))
					.attr('stroke-width', 1.8)
					.attr('opacity',      0.55)
					.attr('pointer-events', 'none')
					.style('animation', `tax-pulse 1.3s ease-out ${delay}ms infinite`)
			);
		}

		function clearTaxSel(sel, d) {
			sel.select('circle.tax-main')
				.transition().duration(180)
				.attr('r',    rScale(d))
				.attr('fill', taxColor(d) + '28');
			sel.selectAll('.tax-pr').remove();
		}

		let taxSelectedD = null;

		// All nodes are interactive (including root which now has a definition)
		nodeSel.attr('cursor', 'pointer')
			.on('mouseenter', (event, d) => {
				if (taxSelectedD?.data.id === d.data.id) return;
				const grp = select(event.currentTarget);
				grp.select('circle.tax-main')
					.transition().duration(160)
					.attr('r',    rScale(d) * 1.4)
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
					taxSelected  = null;
					return;
				}
				if (taxSelectedD) {
					clearTaxSel(
						nodeSel.filter(n => n.data.id === taxSelectedD.data.id),
						taxSelectedD
					);
				}
				taxSelectedD = d;
				taxSelected  = d.data;
				const grp = select(event.currentTarget);
				grp.select('circle.tax-main')
					.transition().duration(160)
					.attr('r',    rScale(d) * 1.4)
					.attr('fill', taxColor(d) + '50');
				addTaxPulse(grp, d);
			});

		svg.on('click', () => {
			if (!taxSelectedD) return;
			clearTaxSel(
				nodeSel.filter(n => n.data.id === taxSelectedD.data.id),
				taxSelectedD
			);
			taxSelectedD = null;
			taxSelected  = null;
		});

		// Auto-fit — extra bottom pad for diagonal leaf labels
		await tick();
		try {
			const bounds = g.node().getBBox();
			const padX = 60, padTop = 160, padBottom = 60;
			const scale = Math.min(
				W  / (bounds.width  + padX * 2),
				H  / (bounds.height + padTop + padBottom),
				0.9
			);
			const tx = (W - scale * bounds.width)  / 2 - scale * bounds.x;
			const ty = padTop - scale * bounds.y;
			svg.transition().duration(600)
				.call(zoomBehavior.transform, zoomIdentity.translate(tx, ty).scale(scale));
		} catch {}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// PSGC LOCATIONS FORCE GRAPH
	// ══════════════════════════════════════════════════════════════════════════
	let psgcSvgEl    = $state(null);
	let psgcLoading  = $state(true);
	let psgcSelected = $state(null);
	let psgcSim      = null;

	const ISLAND_COLOR = {
		Luzon:    '#3b82f6',
		NCR:      '#8b5cf6',
		Visayas:  '#22c55e',
		Mindanao: '#f97316'
	};

	$effect(() => {
		if (psgcSvgEl) {
			psgcLoading = true;
			psgcSelected = null;
			initPsgc();
			return () => {
				if (psgcSim) { psgcSim.stop(); psgcSim = null; }
			};
		}
	});

	async function initPsgc() {
		const [
			{ forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide, forceX, forceY },
			{ drag },
			{ zoom, zoomIdentity },
			{ select }
		] = await Promise.all([
			import('d3-force'),
			import('d3-drag'),
			import('d3-zoom'),
			import('d3-selection')
		]);

		const res = await fetch(apiUrl('/api/ontology/psgc'));
		const data = await res.json();
		psgcLoading = false;

		const nodes = data.nodes.map(n => ({ ...n }));
		const nodeById = new Map(nodes.map(n => [n.id, n]));
		const links = data.links.map(l => ({
			...l,
			isCity: nodeById.get(l.source)?.level === 'City'
		}));

		const rect = psgcSvgEl.getBoundingClientRect();
		const W = rect.width  || 900;
		const H = rect.height || 700;

		const svg = select(psgcSvgEl);
		const g   = svg.append('g');

		const zoomBehavior = zoom()
			.scaleExtent([0.2, 4])
			.filter(e => e.target === psgcSvgEl || e.target.tagName === 'svg' || e.type === 'wheel')
			.on('zoom', e => g.attr('transform', e.transform));
		svg.call(zoomBehavior).on('dblclick.zoom', null);

		// Island X anchors — cluster by island group
		const ISLAND_X = { Luzon: W * 0.22, NCR: W * 0.36, Visayas: W * 0.55, Mindanao: W * 0.78 };
		const ISLAND_Y = { Luzon: H * 0.45, NCR: H * 0.45, Visayas: H * 0.45, Mindanao: H * 0.45 };

		psgcSim = forceSimulation(nodes)
			.force('link',    forceLink(links).id(d => d.id).distance(l => l.isCity ? 45 : 60).strength(0.7))
			.force('charge',  forceManyBody().strength(d => d.level === 'Region' ? -350 : d.level === 'City' ? -40 : -80))
			.force('center',  forceCenter(W / 2, H / 2).strength(0.02))
			.force('collide', forceCollide(d => d.level === 'Region' ? 32 : d.level === 'City' ? 11 : 18))
			.force('x',       forceX(d => ISLAND_X[d.island] ?? W / 2).strength(0.25))
			.force('y',       forceY(d => ISLAND_Y[d.island] ?? H / 2).strength(0.1))
			.alphaDecay(0.02)
			.velocityDecay(0.35);
		const simulation = psgcSim;

		// Links — city links are thinner and dashed to show direct-to-region independence
		const linkSel = g.append('g')
			.selectAll('line').data(links).join('line')
			.attr('stroke',           l => l.isCity ? '#94a3b8' : '#cbd5e1')
			.attr('stroke-width',     l => l.isCity ? 0.5 : 0.8)
			.attr('stroke-dasharray', l => l.isCity ? '3,2' : '0')
			.attr('opacity',          l => l.isCity ? 0.3 : 0.5);

		// Node groups
		const nodeSel = g.append('g')
			.selectAll('g').data(nodes).join('g')
			.attr('cursor', 'pointer');

		const psgcR = d => d.level === 'Region' ? 20 : d.level === 'City' ? 6 : 10;
		const psgcNodeColor = d => ISLAND_COLOR[d.island] ?? '#94a3b8';

		nodeSel.append('circle').attr('class', 'psgc-circle')
			.attr('r',                psgcR)
			.attr('fill',             d => psgcNodeColor(d) + (d.level === 'Region' ? '30' : '20'))
			.attr('stroke',           d => psgcNodeColor(d))
			.attr('stroke-width',     d => d.level === 'Region' ? 2.5 : d.level === 'City' ? 1.2 : 1.5)
			.attr('stroke-dasharray', d => d.level === 'City' ? '3,2' : '0');

		nodeSel.append('text')
			.attr('text-anchor',  'middle')
			.attr('dy',           d => psgcR(d) + (d.level === 'City' ? 9 : 11))
			.attr('font-size',    d => d.level === 'Region' ? 9 : d.level === 'City' ? 7 : 8)
			.attr('font-weight',  d => d.level === 'Region' ? '700' : '400')
			.attr('font-style',   d => d.level === 'City' ? 'italic' : 'normal')
			.attr('fill',         d => d.level === 'Region' ? psgcNodeColor(d) : '#475569')
			.attr('pointer-events', 'none')
			.text(d => d.label);

		// Drag
		let psgcSelectedD = null;

		const dragBehavior = drag()
			.on('start', (event, d) => {
				if (!event.active) simulation.alphaTarget(0.3).restart();
				d.fx = d.x; d.fy = d.y;
			})
			.on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
			.on('end', (event, d) => {
				if (!event.active) simulation.alphaTarget(0);
				d.fx = null; d.fy = null;
			});
		nodeSel.call(dragBehavior);

		// Hover + click
		nodeSel
			.on('mouseenter', (event, d) => {
				if (psgcSelectedD?.id === d.id) return;
				select(event.currentTarget).select('.psgc-circle')
					.transition().duration(150)
					.attr('r', psgcR(d) * 1.4)
					.attr('fill', psgcNodeColor(d) + '55');
				// Dim others
				nodeSel.attr('opacity', n => n.id === d.id ? 1 : 0.38);
				const pid = d.regionId ?? d.id;
				linkSel.attr('opacity', l =>
					l.source.id === pid || l.target.id === pid ||
					l.source.id === d.id || l.target.id === d.id ? 0.9 : 0.08);
			})
			.on('mouseleave', (event, d) => {
				if (psgcSelectedD?.id === d.id) return;
				select(event.currentTarget).select('.psgc-circle')
					.transition().duration(150)
					.attr('r', psgcR(d))
					.attr('fill', psgcNodeColor(d) + (d.level === 'Region' ? '30' : '20'));
				nodeSel.attr('opacity', 1);
				linkSel.attr('opacity', 0.5);
			})
			.on('click', (event, d) => {
				event.stopPropagation();
				if (psgcSelectedD?.id === d.id) {
					select(event.currentTarget).select('.psgc-circle')
						.transition().duration(150)
						.attr('r', psgcR(d))
						.attr('fill', psgcNodeColor(d) + (d.level === 'Region' ? '30' : '20'));
					psgcSelectedD = null;
					psgcSelected = null;
					nodeSel.attr('opacity', 1);
					linkSel.attr('opacity', 0.5);
					return;
				}
				if (psgcSelectedD) {
					nodeSel.filter(n => n.id === psgcSelectedD.id)
						.select('.psgc-circle')
						.transition().duration(150)
						.attr('r', psgcR(psgcSelectedD))
						.attr('fill', psgcNodeColor(psgcSelectedD) + (psgcSelectedD.level === 'Region' ? '30' : '20'));
				}
				psgcSelectedD = d;
				psgcSelected = d;
				select(event.currentTarget).select('.psgc-circle')
					.transition().duration(150)
					.attr('r', psgcR(d) * 1.4)
					.attr('fill', psgcNodeColor(d) + '55');
			});

		svg.on('click', () => {
			if (psgcSelectedD) {
				nodeSel.filter(n => n.id === psgcSelectedD.id)
					.select('.psgc-circle')
					.transition().duration(150)
					.attr('r', psgcR(psgcSelectedD))
					.attr('fill', psgcNodeColor(psgcSelectedD) + (psgcSelectedD.level === 'Region' ? '30' : '20'));
				psgcSelectedD = null;
				psgcSelected = null;
				nodeSel.attr('opacity', 1);
				linkSel.attr('opacity', 0.5);
			}
		});

		simulation.on('tick', () => {
			linkSel
				.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
				.attr('x2', d => d.target.x).attr('y2', d => d.target.y);
			nodeSel.attr('transform', d => `translate(${d.x},${d.y})`);
		});

		// Auto-fit after settling
		setTimeout(() => {
			simulation.alphaTarget(0);
			try {
				const bounds = g.node().getBBox();
				const pad = 40;
				const scale = Math.min(W / (bounds.width + pad * 2), H / (bounds.height + pad * 2), 1);
				const tx = (W - scale * (bounds.width + pad * 2)) / 2 - scale * (bounds.x - pad);
				const ty = (H - scale * (bounds.height + pad * 2)) / 2 - scale * (bounds.y - pad);
				svg.transition().duration(700)
					.call(zoomBehavior.transform, zoomIdentity.translate(tx, ty).scale(scale));
			} catch {}
		}, 3000);
	}
</script>

<svelte:head>
	<title>Ontology · SakunaGraPH</title>
</svelte:head>

<NodeCanvas interactive={false} />

<!-- Cursor tooltip for class graph -->
{#if activeTab === 'graph' && hoveredNode && selectedNode?.id !== hoveredNode?.id}
	<div
		class="fixed z-50 pointer-events-none rounded-xl bg-slate-800/95 px-3 py-2 shadow-xl max-w-xs"
		style="left:{tooltipX + 14}px; top:{tooltipY - 10}px; backdrop-filter:blur(6px);"
	>
		<p class="text-[11px] font-bold text-white">{hoveredNode.label}</p>
		<p class="mt-0.5 text-[10px] leading-snug text-slate-300">{hoveredNode.definition}</p>
	</div>
{/if}

<div class="relative" style="height: calc(100vh - 52px); z-index: 1; overflow: hidden;">

	<!-- ── Tab bar ───────────────────────────────────────────────────────────── -->
	<div
		class="absolute top-4 left-1/2 -translate-x-1/2 z-30 flex items-center gap-1 rounded-full border border-slate-200/70 bg-white/88 px-2 py-1.5 shadow-md"
		style="backdrop-filter:blur(12px);"
	>
		<span class="pl-2 pr-3 text-[10px] font-bold uppercase tracking-widest text-slate-600 border-r border-slate-200">SakunaGraPH</span>
		{#each TABS as tab}
			<button
				onclick={() => activeTab = tab.id}
				class="rounded-full px-4 py-1.5 text-[11px] font-semibold transition-all whitespace-nowrap"
				style={activeTab === tab.id
					? 'background:#1e293b; color:white;'
					: 'color:#64748b;'}
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- ── CLASS GRAPH pane ───────────────────────────────────────────────────── -->
	<div class="absolute inset-0" class:hidden={activeTab !== 'graph'}>
		{#if loading}
			<div class="absolute inset-0 flex items-center justify-center z-10">
				<div class="text-center">
					<svg class="animate-spin mx-auto mb-3 text-slate-400" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
						<path d="M21 12a9 9 0 1 1-6.219-8.56"/>
					</svg>
					<p class="text-sm text-slate-400">Rendering ontology graph…</p>
				</div>
			</div>
		{/if}

		<svg bind:this={svgEl} class="w-full h-full" style="cursor:default;"></svg>

		<p class="absolute top-16 right-5 text-[11px] font-medium text-slate-500 pointer-events-none">
			Hover · Click · Drag
		</p>

		<!-- Legend -->
		<div
			class="absolute bottom-6 left-6 rounded-2xl bg-white/85 px-5 py-4 shadow-2xl"
			style="backdrop-filter:blur(12px);"
		>
			<p class="mb-3 text-[10px] font-bold uppercase tracking-widest text-slate-400">Legend</p>
			<div class="flex flex-col gap-2">
				{#each Object.entries(GROUP_LABEL) as [group, label]}
					<div class="flex items-center gap-2.5">
						<div class="h-3.5 w-3.5 rounded-full flex-shrink-0" style="background:{GROUP_COLOR[group]}33; border:2.5px solid {GROUP_COLOR[group]};"></div>
						<span class="text-xs text-slate-600 font-medium">{label}</span>
					</div>
				{/each}
				<div class="mt-2 pt-2 border-t border-slate-100 flex flex-col gap-1.5">
					<div class="flex items-center gap-2.5">
						<div class="h-px w-7 flex-shrink-0 bg-slate-300"></div>
						<span class="text-xs text-slate-400">subClassOf</span>
					</div>
					<div class="flex items-center gap-2.5">
						<svg width="28" height="7" style="flex-shrink:0;"><line x1="0" y1="3.5" x2="28" y2="3.5" stroke="#93c5fd" stroke-width="1.8" stroke-dasharray="5 3"/></svg>
						<span class="text-xs text-slate-400">object property</span>
					</div>
				</div>
			</div>
			<p class="mt-3 text-[10px] text-slate-300">Scroll · Drag canvas · Drag nodes</p>
		</div>

		<!-- Info panel -->
		{#if selectedNode}
			<div
				class="absolute bottom-6 right-6 rounded-2xl border border-slate-200/60 bg-white/92 shadow-xl"
				style="backdrop-filter:blur(18px); width:420px; max-height:72vh; overflow-y:auto;"
			>
				<div class="px-8 py-7">
					<p class="text-[13px] font-bold uppercase tracking-widest mb-2" style="color:{GROUP_COLOR[selectedNode.group]};">
						{GROUP_LABEL[selectedNode.group]}
					</p>
					<h2 class="font-black text-slate-800 leading-tight" style="font-family:'Playfair Display', Georgia,serif; font-weight:900; font-size:1.8rem;">
						{selectedNode.label}
					</h2>
					<div class="mt-3 h-0.5 rounded-full" style="width:40px; background:{GROUP_COLOR[selectedNode.group]};"></div>
					<p class="mt-4 text-[15px] text-slate-500 leading-relaxed">{selectedNode.definition}</p>
					{#if selectedNode.dataProperties?.length}
						<div class="mt-5 pt-4 border-t border-slate-100">
							<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-2">Data Properties</p>
							<div class="flex flex-col gap-1.5">
								{#each selectedNode.dataProperties as prop}
									<div class="flex items-center justify-between gap-3">
										<span class="text-[13px] text-slate-600">{prop.label || prop.range}</span>
										<span class="text-[11px] font-mono text-slate-400 bg-slate-50 rounded-md px-2 py-0.5 flex-shrink-0">{prop.range}</span>
									</div>
								{/each}
							</div>
						</div>
					{/if}
					<p class="mt-5 text-[12px] uppercase tracking-widest text-slate-300">Click node or background to deselect</p>
				</div>
			</div>
		{/if}
	</div>

	<!-- ── TAXONOMY TREE pane ─────────────────────────────────────────────────── -->
	{#if activeTab === 'taxonomy'}
		<div class="absolute inset-0">
			{#if taxLoading}
				<div class="absolute inset-0 flex items-center justify-center z-10">
					<div class="text-center">
						<svg class="animate-spin mx-auto mb-3 text-slate-400" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
							<path d="M21 12a9 9 0 1 1-6.219-8.56"/>
						</svg>
						<p class="text-sm text-slate-400">Building taxonomy tree…</p>
					</div>
				</div>
			{/if}

			<svg bind:this={taxSvgEl} class="w-full h-full" style="cursor:default;"></svg>

			<p class="absolute top-16 right-5 text-[11px] font-medium text-slate-500 pointer-events-none">
				Scroll to zoom · Drag canvas · Click node
			</p>

			<!-- Taxonomy legend -->
			<div
				class="absolute bottom-6 left-6 rounded-2xl bg-white/85 px-4 py-4 shadow-2xl"
				style="backdrop-filter:blur(12px); max-width:300px; min-width: 200px;"
			>
				<p class="mb-3 text-[10px] font-bold uppercase tracking-widest text-slate-400">Category</p>
				<div class="flex flex-col gap-1.5">
					{#each [
						['natural',          'Natural'],
						['biological',       '· Biological'],
						['climatological',   '· Climatological'],
						['extraterrestrial', '· Extraterrestrial'],
						['geophysical',      '· Geophysical'],
						['hydrological',     '· Hydrological'],
						['meteorological',   '· Meteorological'],
						['tech',             'Technological'],
						['armedconflict',    '· Armed Conflict'],
						['industrial',       '· Industrial'],
						['miscellaneous',    '· Miscellaneous'],
						['transport',        '· Transport']
					] as [key, label]}
						<div class="flex items-center gap-2">
							<div class="h-2.5 w-2.5 rounded-full flex-shrink-0" style="background:{TAX_COLOR[key]};"></div>
							<span class="text-[12px] text-slate-600">{label}</span>
						</div>
					{/each}
				</div>
			</div>

			<!-- Taxonomy info panel — bottom-right, vertical card -->
			{#if taxSelected}
				<div
					class="absolute bottom-6 right-6 rounded-2xl border border-slate-200/60 bg-white/92 shadow-xl"
					style="backdrop-filter:blur(18px); width:420px; max-height:72vh; overflow-y:auto;"
				>
					<div class="px-8 py-7">
						<p class="text-[13px] font-bold uppercase tracking-widest mb-2" style="color:{TAX_COLOR[taxSelected.group]};">
							{taxSelected.group}
						</p>
						<h2 class="font-black text-slate-800 leading-tight" style="font-family:'Playfair Display', Georgia,serif; font-weight:900; font-size:1.8rem;">
							{taxSelected.label}
						</h2>
						<div class="mt-3 h-0.5 rounded-full" style="width:40px; background:{TAX_COLOR[taxSelected.group]};"></div>
						<p class="mt-4 text-[15px] text-slate-500 leading-relaxed">{taxSelected.definition}</p>
						<p class="mt-5 text-[12px] uppercase tracking-widest text-slate-300">Click node or canvas to deselect</p>
					</div>
				</div>
			{/if}
		</div>
	{/if}

	<!-- ── PSGC LOCATIONS pane ────────────────────────────────────────────────── -->
	{#if activeTab === 'psgc'}
		<div class="absolute inset-0">
			{#if psgcLoading}
				<div class="absolute inset-0 flex items-center justify-center z-10">
					<div class="text-center">
						<svg class="animate-spin mx-auto mb-3 text-slate-400" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
							<path d="M21 12a9 9 0 1 1-6.219-8.56"/>
						</svg>
						<p class="text-sm text-slate-400">Loading PSGC graph…</p>
					</div>
				</div>
			{/if}

			<svg bind:this={psgcSvgEl} class="w-full h-full" style="cursor:default;"></svg>

			<p class="absolute top-16 right-5 text-[11px] font-medium text-slate-500 pointer-events-none">
				Hover · Click · Drag · Scroll
			</p>

			<!-- Island legend -->
			<div
				class="absolute bottom-6 left-6 rounded-2xl  bg-white/85 px-5 py-4 shadow-2xl"
				style="backdrop-filter:blur(12px);"
			>
				<p class="mb-3 text-[10px] font-bold uppercase tracking-widest text-slate-400">Island Group</p>
				<div class="flex flex-col gap-2">
					{#each Object.entries(ISLAND_COLOR) as [island, color]}
						<div class="flex items-center gap-2.5">
							<div class="h-3.5 w-3.5 rounded-full flex-shrink-0" style="background:{color}30; border:2.5px solid {color};"></div>
							<span class="text-xs text-slate-600 font-medium">{island}</span>
						</div>
					{/each}
				</div>
				<div class="mt-3 pt-3 border-t border-slate-100 flex flex-col gap-1.5">
					<div class="flex items-center gap-2.5">
						<div class="h-5 w-5 rounded-full flex-shrink-0 bg-slate-100 border-2 border-slate-400"></div>
						<span class="text-xs text-slate-400">Region</span>
					</div>
					<div class="flex items-center gap-2.5">
						<div class="h-3 w-3 rounded-full flex-shrink-0 bg-slate-100 border border-slate-400"></div>
						<span class="text-xs text-slate-400">Province</span>
					</div>
					<div class="flex items-center gap-2.5">
						<div class="h-2.5 w-2.5 rounded-full flex-shrink-0" style="background:transparent; border: 1.2px dashed #94a3b8;"></div>
						<span class="text-xs text-slate-400 italic">HUC / ICC (independent)</span>
					</div>
				</div>
				<p class="mt-3 text-[10px] text-slate-300">HUCs link directly to region</p>
			</div>

			<!-- PSGC info panel — bottom-right, vertical card -->
			{#if psgcSelected}
				<div
					class="absolute bottom-6 right-6 rounded-2xl border border-slate-200/60 bg-white/92 shadow-xl"
					style="backdrop-filter:blur(18px); width:420px; max-height:72vh; overflow-y:auto;"
				>
					<div class="px-8 py-7 flex flex-col gap-3">
						<p class="text-[13px] font-bold uppercase tracking-widest" style="color:{ISLAND_COLOR[psgcSelected.island]};">
							{psgcSelected.cityType ?? psgcSelected.level} · {psgcSelected.regionLabel ?? psgcSelected.island}
						</p>
						<h2 class="font-black text-slate-800 leading-tight" style="font-family:'Playfair Display', Georgia,serif; font-weight:900; font-size:1.8rem;">
							{psgcSelected.fullName ?? psgcSelected.label}
						</h2>
						<div class="h-0.5 rounded-full" style="width:40px; background:{ISLAND_COLOR[psgcSelected.island]};"></div>
						<div class="flex flex-col gap-3 mt-1">
							<div>
								<p class="text-[10px] uppercase tracking-wider text-slate-400 font-bold">PSGC Code</p>
								<p class="text-[14px] text-slate-600 font-mono">{psgcSelected.psgcCode}</p>
							</div>
							<div>
								<p class="text-[10px] uppercase tracking-wider text-slate-400 font-bold">Geographic Level</p>
								<p class="text-[14px] text-slate-600">{psgcSelected.level}</p>
							</div>
							{#if psgcSelected.cityType}
								<div>
									<p class="text-[10px] uppercase tracking-wider text-slate-400 font-bold">City Classification</p>
									<p class="text-[14px] text-slate-600">
										{psgcSelected.cityType === 'HUC' ? 'Highly Urbanized City' : psgcSelected.cityType === 'ICC' ? 'Independent Component City' : psgcSelected.cityType}
									</p>
								</div>
							{/if}
							{#if psgcSelected.incomeClass}
								<div>
									<p class="text-[10px] uppercase tracking-wider text-slate-400 font-bold">Income Classification</p>
									<p class="text-[14px] text-slate-600">{psgcSelected.incomeClass} class</p>
								</div>
							{/if}
							<div>
								<p class="text-[10px] uppercase tracking-wider text-slate-400 font-bold">Population (2020)</p>
								<p class="text-[14px] text-slate-600">{psgcSelected.population.toLocaleString()}</p>
							</div>
						</div>
						{#if psgcSelected.note}
							<div class="pt-2 border-t border-slate-100">
								<p class="text-[10px] uppercase tracking-wider text-slate-400 font-bold mb-0.5">Note</p>
								<p class="text-[14px] text-slate-500 italic leading-snug">{psgcSelected.note}</p>
							</div>
						{/if}
						<p class="mt-1 text-[12px] uppercase tracking-widest text-slate-300">Click node or canvas to deselect</p>
					</div>
				</div>
			{/if}
		</div>
	{/if}

</div>
