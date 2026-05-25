<script>
	import { onMount } from 'svelte';

	let canvas;
	let animFrame;
	const NODE_COUNT = 42;
	const CONNECT_DIST = 170;

	function makeNodes(w, h) {
		return Array.from({ length: NODE_COUNT }, () => ({
			x: Math.random() * w,
			y: Math.random() * h,
			vx: (Math.random() - 0.5) * 0.45,
			vy: (Math.random() - 0.5) * 0.45,
			r: Math.random() * 2.5 + 1.5,
			phase: Math.random() * Math.PI * 2
		}));
	}

	onMount(() => {
		const ctx = canvas.getContext('2d');
		let nodes = [];

		const resize = () => {
			canvas.width = window.innerWidth;
			canvas.height = window.innerHeight;
			nodes = makeNodes(canvas.width, canvas.height);
		};

		resize();
		window.addEventListener('resize', resize);

		function loop(ts) {
			const w = canvas.width;
			const h = canvas.height;

			// Update positions
			for (const n of nodes) {
				n.x += n.vx;
				n.y += n.vy;
				if (n.x < 0 || n.x > w) n.vx *= -1;
				if (n.y < 0 || n.y > h) n.vy *= -1;
			}

			ctx.clearRect(0, 0, w, h);

			// Draw edges
			for (let i = 0; i < nodes.length; i++) {
				for (let j = i + 1; j < nodes.length; j++) {
					const dx = nodes[i].x - nodes[j].x;
					const dy = nodes[i].y - nodes[j].y;
					const d = Math.sqrt(dx * dx + dy * dy);
					if (d < CONNECT_DIST) {
						const alpha = 0.13 * (1 - d / CONNECT_DIST);
						ctx.beginPath();
						ctx.moveTo(nodes[i].x, nodes[i].y);
						ctx.lineTo(nodes[j].x, nodes[j].y);
						ctx.strokeStyle = `rgba(99,102,241,${alpha})`;
						ctx.lineWidth = 0.9;
						ctx.stroke();
					}
				}
			}

			// Draw nodes
			for (const n of nodes) {
				const pulse = Math.sin(ts * 0.0008 + n.phase) * 0.5 + 0.5;
				const alpha = 0.12 + pulse * 0.18;
				ctx.beginPath();
				ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2);
				ctx.fillStyle = `rgba(99,102,241,${alpha})`;
				ctx.fill();

				// Soft glow ring on larger nodes
				if (n.r > 3) {
					ctx.beginPath();
					ctx.arc(n.x, n.y, n.r + 3, 0, Math.PI * 2);
					ctx.fillStyle = `rgba(99,102,241,${alpha * 0.25})`;
					ctx.fill();
				}
			}

			animFrame = requestAnimationFrame(loop);
		}

		animFrame = requestAnimationFrame(loop);

		return () => {
			window.removeEventListener('resize', resize);
			cancelAnimationFrame(animFrame);
		};
	});
</script>

<canvas bind:this={canvas} style="position:fixed;inset:0;width:100%;height:100%;pointer-events:none;z-index:0;"></canvas>
