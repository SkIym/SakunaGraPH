/**
 * Regenerates gadm41_PHL_2_paths.json using the same Mercator projection
 * as the runtime province paths (fitSize([700,800], regions.geojson)).
 *
 * Run from the frontend/ directory:
 *   node scripts/gen-city-paths.mjs
 */
import { readFileSync, writeFileSync } from 'fs';
import { execSync } from 'child_process';
import { geoMercator, geoPath } from 'd3-geo';

const SVG_W = 700;
const SVG_H = 800;

// ── 1. Load the province GeoJSON to derive the canonical projection ──────────
console.log('Loading regions.geojson...');
const provinces = JSON.parse(readFileSync('./static/data/regions.geojson', 'utf8'));
const projection = geoMercator().fitSize([SVG_W, SVG_H], provinces);
const pathGen = geoPath(projection);
console.log('Projection ready.');

// ── 2. Load the raw municipality GeoJSON from git history ────────────────────
console.log('Extracting municipality GeoJSON from git...');
const raw = execSync(
  'git show d1403b5:sakuna.ph/src/lib/map/gadm41_PHL_2.json',
  { cwd: '../..', maxBuffer: 64 * 1024 * 1024 }
).toString('utf8');
const municipalities = JSON.parse(raw);
console.log(`Loaded ${municipalities.features.length} municipality features.`);

// ── 3. Build paths ────────────────────────────────────────────────────────────
function regionPsgcFromCC2(cc2) {
  // CC_2 = "140101" → first 2 digits = region code → pad to 10-digit PSGC
  return cc2.slice(0, 2).padEnd(10, '0');
}

const output = [];
const seenGids = new Set();
let skipped = 0;

for (const f of municipalities.features) {
  const p = f.properties;
  const cc2 = p.CC_2;
  // Skip water bodies, features with non-numeric CC_2, and GADM data duplicates
  if (!cc2 || cc2 === '0' || cc2 === 'NA' || !/^\d+$/.test(cc2)) { skipped++; continue; }
  if (seenGids.has(cc2)) { skipped++; continue; }
  seenGids.add(cc2);

  const d = pathGen(f);
  if (!d) { skipped++; continue; }

  let bounds;
  try {
    bounds = pathGen.bounds(f);
  } catch {
    bounds = [[0, 0], [0, 0]];
  }

  output.push({
    d,
    gid: cc2,
    name: p.CITY_MUNICIPALITY,
    province: p.PROVINCE,
    regionName: p.REGION,
    regionPsgc: regionPsgcFromCC2(cc2),
    bounds
  });
}

console.log(`Generated ${output.length} paths (skipped ${skipped} with null paths).`);

// ── 4. Write ──────────────────────────────────────────────────────────────────
const outPath = './src/lib/data/gadm41_PHL_2_paths.json';
writeFileSync(outPath, JSON.stringify(output));
console.log(`Written to ${outPath}`);
