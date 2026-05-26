<script>
	import { onMount } from 'svelte';

	let { interactive = true } = $props();

	let canvas;
	let animFrame;

	const NODE_COUNT = 42;
	const CONNECT_DIST = 180;
	const HOVER_RADIUS = 140; // px — mouse influence radius
	const GRID_SIZE = 55; // grid cell size in px
	const node_color = "rgba(41,118,158"

	let mouseX = -9999;
	let mouseY = -9999;

	function makeNodes(w, h) {
		return Array.from({ length: NODE_COUNT }, () => ({
			x: Math.random() * w,
			y: Math.random() * h,
			vx: (Math.random() - 0.5) * 0.45,
			vy: (Math.random() - 0.5) * 0.45,
			r: Math.random() * 4 + 3.5, // bigger: 3.5–7.5 px base radius
			phase: Math.random() * Math.PI * 2
		}));
	}

	onMount(() => {
		const ctx = canvas.getContext('2d');
		let nodes = [];

		// Track mouse in viewport coords — canvas is fixed full-screen so they match
		const onMouseMove = (e) => {
			mouseX = e.clientX;
			mouseY = e.clientY;
		};
		const onMouseLeave = () => {
			mouseX = -9999;
			mouseY = -9999;
		};

		const resize = () => {
			canvas.width = window.innerWidth;
			canvas.height = window.innerHeight;
			nodes = makeNodes(canvas.width, canvas.height);
		};

		resize();
		window.addEventListener('resize', resize);
		if (interactive) {
			window.addEventListener('mousemove', onMouseMove);
			document.addEventListener('mouseleave', onMouseLeave);
		}

		function loop(ts) {
			const w = canvas.width;
			const h = canvas.height;

			// Move nodes
			for (const n of nodes) {
				n.x += n.vx;
				n.y += n.vy;
				if (n.x < 0 || n.x > w) n.vx *= -1;
				if (n.y < 0 || n.y > h) n.vy *= -1;
			}

			ctx.clearRect(0, 0, w, h);

			// ── Grid ─────────────────────────────────────────────────────────────
			ctx.beginPath();
			ctx.strokeStyle = node_color +',0.08)';
			ctx.lineWidth = 0.6;
			for (let x = 0.5; x <= w; x += GRID_SIZE) {
				ctx.moveTo(x, 0);
				ctx.lineTo(x, h);
			}
			for (let y = 0.5; y <= h; y += GRID_SIZE) {
				ctx.moveTo(0, y);
				ctx.lineTo(w, y);
			}
			ctx.stroke();

			// ── Edges ────────────────────────────────────────────────────────────
			for (let i = 0; i < nodes.length; i++) {
				for (let j = i + 1; j < nodes.length; j++) {
					const dx = nodes[i].x - nodes[j].x;
					const dy = nodes[i].y - nodes[j].y;
					const d = Math.sqrt(dx * dx + dy * dy);
					if (d < CONNECT_DIST) {
						const alpha = 0.14 * (1 - d / CONNECT_DIST);
						ctx.beginPath();
						ctx.moveTo(nodes[i].x, nodes[i].y);
						ctx.lineTo(nodes[j].x, nodes[j].y);
						ctx.strokeStyle = `${node_color},${alpha})`;
						ctx.lineWidth = 0.9;
						ctx.stroke();
					}
				}
			}

			// ── Nodes ────────────────────────────────────────────────────────────
			for (const n of nodes) {
				// How close is the mouse? — smooth 0→1 influence factor
				const mdx = mouseX - n.x;
				const mdy = mouseY - n.y;
				const mouseDist = Math.sqrt(mdx * mdx + mdy * mdy);
				const hoverT = Math.max(0, 1 - mouseDist / HOVER_RADIUS);
				// Ease in: slow ramp at edges, full glow near centre
				const t = hoverT * hoverT;

				const pulse = Math.sin(ts * 0.0008 + n.phase) * 0.5 + 0.5;
				const baseAlpha = 0.14 + pulse * 0.16;
				const alpha = baseAlpha + t * 0.55;
				const r = n.r + t * n.r * 2.0; // up to 3× base on hover

				// Soft radial glow halo on hover
				if (t > 0) {
					const glowR = r + 22 * t;
					const grd = ctx.createRadialGradient(n.x, n.y, r * 0.4, n.x, n.y, glowR);
					grd.addColorStop(0, node_color + `,${0.32 * t})`);
					grd.addColorStop(1, node_color + ',0)');
					ctx.beginPath();
					ctx.arc(n.x, n.y, glowR, 0, Math.PI * 2);
					ctx.fillStyle = grd;
					ctx.fill();
				}

				// Core dot
				ctx.beginPath();
				ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
				ctx.fillStyle = node_color + `,${alpha})`;
				ctx.fill();
			}

			animFrame = requestAnimationFrame(loop);
		}

		animFrame = requestAnimationFrame(loop);

		return () => {
			window.removeEventListener('resize', resize);
			if (interactive) {
				window.removeEventListener('mousemove', onMouseMove);
				document.removeEventListener('mouseleave', onMouseLeave);
			}
			cancelAnimationFrame(animFrame);
		};
	});
</script>

<canvas
	bind:this={canvas}
	style="position:fixed;inset:0;width:100%;height:100%;pointer-events:none;z-index:0;"
></canvas>
