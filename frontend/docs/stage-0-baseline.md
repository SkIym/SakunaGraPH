# Stage 0 frontend baseline

This baseline freezes the observable frontend behavior before route or API-client restructuring.
The recorded source revision is `2886a80c56946176ad13758b367cf6b1c6b1cf8f` (2026-07-17),
tagged locally as `frontend-stage0-baseline-2026-07-17`.
The repository had unrelated uncommitted ETL work, so the revision is recorded here and in the visual
manifest; the protected ask page has not been moved.

## Production build

The baseline `npm run build` completed successfully with Node 22.16.0, Svelte 5.45.6,
SvelteKit 2.49.1, Vite 7.3.3, and `@sveltejs/adapter-node`.

- 190 SSR modules and 549 client modules were transformed.
- The main generated CSS asset was 51.92 kB (9.53 kB gzip).
- The largest generated client JavaScript chunk was 230.01 kB (75.39 kB gzip).
- The original warnings were six icon-only social links without accessible labels, an interactive
  listener on the `ResultsModal` document panel, and map paths without keyboard handlers.
- Stage 1 fixes those warnings without changing routes, requests, or visual styling.

## Route and visual baseline

The visual manifest covers `/`, `/map`, `/ontology`, `/ask`, `/analysis`, `/analysis/events`,
`/analysis/metrics`, and `/analysis/timeline` at desktop (1440x900) and Pixel 7 mobile widths.
Run `npm run baseline:update` to regenerate the committed JPEG evidence in
`tests/visual-baselines/` using deterministic API fixtures. Canvas backgrounds are included; minor
decorative-node movement is expected between captures because their force layout is nondeterministic.

## Ask states

The protected `/ask` route currently has these states, reproduced by `tests/e2e/ask-states.spec.js`:

| State           | Current visible behavior                                                                    |
| --------------- | ------------------------------------------------------------------------------------------- |
| Initial         | Heading, explanatory copy, four question suggestions, textarea, and disabled Send button.   |
| Loading         | User question plus an assistant card saying “Querying knowledge graph…” with animated dots. |
| Success         | Answer prose, expandable SPARQL, and an expandable row table when rows are present.         |
| Empty           | Answer prose plus “No matching records in the knowledge graph.”                             |
| API failure     | The API `detail` string in a red assistant card.                                            |
| Network failure | “Could not reach server.” in a red assistant card.                                          |

## Reproduction gates

`npm run check`, `npm run lint`, `npm run format:check`, `npm test`, `npm run test:contract:live`,
`npm run test:e2e`, `npm run test:a11y`, and `npm run build` form the Stage 0–1 baseline. The live
contract command requires the API development dependencies. The E2E suite covers navigation, map
loading and keyboard selection, ontology datasets, analysis views, SPARQL results, event details,
and ask behavior. The existing API suite remains an independent gate.

## Verified cleanup

- `frontend/ph-regions.json` had no repository reference outside the staged plan, while the map
  explicitly loads `static/data/regions.geojson`. It was tracked in revision `e1f3e2c` and remains
  recoverable with Git, so the unused 15.8 MB duplicate was removed.
- The repository root has no `package.json` or npm workspace declaration. Its 96-byte empty
  `package-lock.json` was removed; `frontend/package-lock.json` remains the authoritative lockfile.
- `@sveltejs/adapter-auto` was unused because `svelte.config.js` explicitly imports the Node adapter,
  so it was removed from the frontend dependency graph.
