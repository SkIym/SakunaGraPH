# Stage 5 accessibility, performance, and asset policy

## Enforced budgets

`performance-budgets.json` is the single source of truth. `npm run build` checks compressed initial
JavaScript for every route, the largest lazy chunk, CSS, the map payload, and team images.
`npm run test:performance` runs the production adapter and enforces hydration, first contentful
paint, browser transfer, and map parse/projection/render limits. Raise a budget only with a measured
reason in the same change.

The map records `sakunagraph:map-download`, `sakunagraph:map-parse`,
`sakunagraph:map-projection`, and `sakunagraph:map-initial-render` Performance API measures. This
baseline must be captured before changing topology, coordinate precision, or file format.

## Accessibility behavior

- All eight routes run full axe WCAG A/AA checks, including color contrast, at desktop and mobile
  widths. Keyboard tests cover map selection, ontology nodes, dialogs, the mobile filter surface,
  and streamed-answer focus.
- Modal dialogs move focus to their close control, trap Tab/Shift+Tab, close on Escape, and restore
  the initiating control. The date-event drawer receives focus and restores it without becoming a
  modal trap. Streamed answers announce bounded status changes and leave focus in the composer.
- Reduced motion stops the ambient canvas, settles ontology force layouts synchronously, removes
  scripted transition duration, and disables decorative CSS motion. Forced-colors mode preserves
  visible focus and selected graph/map states; information remains available in labels and tables.

## Asset lineage and update policy

| Asset                         | Repository lineage                                                                                                                                                                                 | Current identity                                                                                                  | Delivery policy                                                                                                             |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `static/data/regions.geojson` | Added in `0dc30d5` as merged province geometry with PSGC region/province properties. The original upstream URL/license was not recorded; do not claim a more specific provenance without evidence. | 1,737,986 bytes; SHA-256 `17ECDE6704279A1314E998223F89704A17EBE69C738083193086893128EF9E24`; version `2026-06-23` | Revalidated updateable static data. The version query changes for every content release and is intentionally not immutable. |
| `static/Elle.jpg`             | Project team headshot committed by the project authors; source for the responsive derivatives.                                                                                                     | 1352 x 1413; 289,110 bytes                                                                                        | Source asset only. The page serves 208 px (11,857 bytes) or 416 px (37,364 bytes) JPEG derivatives through `srcset`.        |
| `static/Abram.jpg`            | Project team headshot committed by the project authors; source for the responsive derivatives.                                                                                                     | 768 x 921; 19,019 bytes                                                                                           | Source asset only. The page serves 208 px (5,301 bytes) or 416 px (13,024 bytes) JPEG derivatives through `srcset`.         |

SvelteKit's adapter-node handler serves content-hashed `/_app/immutable/*` files with a one-year
immutable cache header; the production performance test verifies this contract. The GeoJSON is
kept revalidatable because its public filename is stable. To update it:

1. Record the upstream source, license, acquisition date, and transformation command here.
2. Preserve `adm1_psgc`, `adm2_psgc`, and `adm2_en`, then run unit, browser-performance, map E2E,
   accessibility, and visual checks.
3. Change `MAP_ASSET_VERSION` in `src/lib/features/map/geometry.js`, update byte count and SHA-256
   above, and keep the payload within budget (or justify a reviewed budget change).
4. Purge the old query-version URL at any CDN only after the new asset is deployed.

The headshot derivatives are JPEG quality 82, generated with high-quality bicubic resampling. They
are lazy, async-decoded, declare a 208 x 208 rendered box, and carry a 208 px `sizes` hint; the
50 KB image budget applies to each delivered derivative, rather than the archival source.

CodeMirror, map projection, and ontology D3 engines use dynamic imports. Results/event-detail
dialogs load on first open, secondary ontology graphs initialize only when selected, and offscreen
analysis visualization cards use `content-visibility` with an intrinsic placeholder.
