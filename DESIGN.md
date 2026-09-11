---
name: SakunaGraPH
description: An editorial research interface for Philippine disaster knowledge graphs.
colors:
  flag-blue: "#0038a8"
  flag-blue-hover: "#002b82"
  flag-blue-soft: "#edf2ff"
  flag-blue-medium: "#d7e3fb"
  sun-yellow: "#fcd116"
  sun-yellow-soft: "#fff8cf"
  execute-red: "#ce1126"
  execute-red-hover: "#a50e1f"
  paper: "#ffffff"
  paper-soft: "rgba(248, 250, 252, 0.88)"
  ink: "#1e293b"
  ink-secondary: "#475569"
  ink-muted: "#64748b"
  rule: "#e2e8f0"
  danger-paper: "#fff1f2"
typography:
  display:
    fontFamily: "Playfair Display, Georgia, serif"
    fontSize: "clamp(2.35rem, 11vw, 6rem)"
    fontWeight: 900
    lineHeight: 1.05
    letterSpacing: "0.02em"
  title:
    fontFamily: "Playfair Display, Georgia, serif"
    fontSize: "1.5rem"
    fontWeight: 900
    lineHeight: 1.2
  body:
    fontFamily: "Inter, system-ui, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.625
  label:
    fontFamily: "Inter, system-ui, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 600
    lineHeight: 1.25
  mono:
    fontFamily: "JetBrains Mono, Fira Code, Courier New, monospace"
    fontSize: "0.8125rem"
    fontWeight: 400
    lineHeight: 1.7
rounded:
  control: "12px"
  surface: "16px"
  pill: "9999px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "16px"
  lg: "24px"
  xl: "32px"
components:
  button-primary:
    backgroundColor: "{colors.execute-red}"
    textColor: "{colors.paper}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "10px 20px"
    height: "44px"
  button-dark:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "10px 16px"
    height: "44px"
  input:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.control}"
    padding: "10px 16px"
    height: "44px"
  card:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink-secondary}"
    rounded: "{rounded.surface}"
    padding: "16px 20px"
  chip:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink-secondary}"
    typography: "{typography.mono}"
    rounded: "{rounded.pill}"
    padding: "6px 12px"
    height: "44px"
---

# Design System: SakunaGraPH

## Overview

**Creative North Star: "The Civic Data Workbench"**

SakunaGraPH should feel like a trusted Philippine research instrument: editorial enough to make complex disaster knowledge approachable, and technical enough to preserve the precision of RDF, SPARQL, evidence, and provenance. White paper-like space and dark slate ink form the dominant field. Philippine flag blue signs the product, sun yellow marks the current selection, and flag red is reserved for deliberate query actions and genuine failure.

The interface is quiet, legible, and evidence-first. Depth is used to establish working surfaces rather than decorate every container. Motion must communicate active work, spatial relation, or state change; perpetual ambient effects must never compete with reading or task completion.

**Key Characteristics:**

- Editorial serif display type paired with restrained sans-serif UI text.
- Paper-like surfaces, precise rules, and one measured layer of elevation.
- Philippine blue for identity, links, maps, and focus; yellow for selection; red for consequential query actions.
- Monospace reserved for queries, identifiers, measurements, and data.
- Responsive, touch-capable controls with visible keyboard focus.

## Colors

The palette translates the Philippine flag into a quiet civic interface. White and dark ink carry most of every page; the three chromatic colors are semantic signals, not equal-area decoration.

### Primary

- **Flag Blue (`#0038a8`):** Product signature, linked graph concepts, map structure, and focus indication.
- **Sun Yellow (`#fcd116`):** Current navigation, toggle, filter, and geographic selection. Pair it with dark ink or blue, never white text.
- **Execute Red (`#ce1126`):** Primary query execution and destructive urgency only.
- **Deep Flag Blue / Deep Execute Red:** Hover states for their respective controls.
- **Soft Blue / Soft Yellow:** Low-chroma atmosphere for hover, selection, and text-selection surfaces; exact flag hues remain the primitive anchors.

### Neutral

- **Paper:** The default canvas and opaque control surface.
- **Soft Paper:** Subtle toolbars and secondary working surfaces.
- **Research Ink:** Headings, active navigation, and primary copy.
- **Secondary Ink:** Descriptions, controls, and helper text that must retain normal-text contrast.
- **Muted Ink:** Nonessential metadata at compliant sizes.
- **Rule:** Dividers, borders, and graph-workbench structure.
- **Danger Paper:** Error background paired with dark red text.

**The Blue Signature Rule.** Flag blue identifies SakunaGraPH, links graph concepts, outlines maps, and carries keyboard focus; it does not wash whole panels or compete with data.

**The Red Means Execute Rule.** Red belongs to running a query or communicating genuine failure, never routine navigation.

**The Yellow Means Current Rule.** Yellow identifies the active or selected item and always keeps a non-color cue such as position, outline, label weight, or `aria-current`/`aria-pressed` state.

**The Flag Is a Source, Not a Pattern Rule.** Do not distribute blue, red, and yellow evenly, build literal flag stripes, or force all three colors into one component. Data visualizations may retain additional categorical hues when distinction requires them.

## Typography

**Display Font:** Playfair Display with Georgia fallback  
**Body Font:** Inter with system-ui fallback  
**Label/Mono Font:** JetBrains Mono with Fira Code and Courier New fallbacks

**Character:** The high-contrast serif gives the project an editorial, archival voice. The sans-serif keeps dense interaction legible, while monospace makes machine-readable material unmistakable.

### Hierarchy

- **Display:** Heavy editorial wordmark and major product identity only; fluid and capped at 6rem.
- **Title:** Page and feature titles with compact line-height.
- **Body:** Operational copy and answers, normally 14px with generous leading; focused mobile inputs use 16px to prevent viewport zoom.
- **Label:** Controls and compact metadata, never below compliant contrast.
- **Mono:** SPARQL, code, identifiers, measurements, and small data chips only.

**The Serif Has Authority Rule.** Playfair marks identity and major destinations; it never appears as routine control text.

**The Mono Must Mean Data Rule.** Monospace is functional notation, not a generic technology costume.

## Layout

The application uses a centered workbench capped near 48rem, surrounded by generous paper space. The persistent navigation is 52px high, and full-height task surfaces subtract that shared token using dynamic viewport units. Primary controls have a 44px minimum target.

Home uses two stacked viewport-scale sections: the query workbench first, then team attribution. Team profiles stack on phones and align horizontally when their content fits. Ask is a bounded vertical workspace with a scrollable conversation and a safe-area-aware composer.

**The Content Decides the Breakpoint Rule.** Columns collapse before their content compresses, and wordmarks scale to the available inline size rather than clipping.

## Elevation & Depth

The system is flat by default. Working surfaces may use one diffuse, downward shadow and a subtle border; internal hierarchy comes from paper tones and rules. Translucency is acceptable only when it preserves text contrast and expresses a foreground work surface.

### Shadow Vocabulary

- **Workbench Lift** (`0 18px 45px -24px rgba(30, 41, 59, 0.32)`): The main query workbench and other singular foreground tools.
- **Control Rest** (`0 1px 2px rgba(15, 23, 42, 0.08)`): Inputs and action controls requiring separation from paper.

**The One Lifted Plane Rule.** Elevate the active tool, not every nested region inside it.

## Shapes

Controls use gently curved 12px corners, primary surfaces use 16px corners, and compact chips or icon targets may be circular or pill-shaped. Fine neutral borders define structure. Small corner reductions may indicate message direction, but should not become speech-bubble ornament.

## Components

### Buttons

- **Shape:** 12px corners with a 44px minimum target.
- **Primary:** Execute red, white label, and 20px horizontal padding for consequential execution.
- **Dark action:** Research Ink for neutral actions such as Send.
- **Hover / Focus:** Darken the semantic fill on hover; show the shared three-pixel blue focus ring without shifting layout.
- **Disabled:** Preserve the label while reducing emphasis; never rely on color alone to explain why an action is unavailable.

### Chips

- **Style:** White paper, fine rule border, secondary ink, and functional monospace.
- **State:** Hover strengthens border and text; every chip keeps a 44px hit area.

### Cards / Containers

- **Corner Style:** 16px.
- **Background:** Paper or translucent paper when layered over a meaningful canvas.
- **Shadow Strategy:** Only the principal working surface receives Workbench Lift.
- **Border:** One fine Rule border; avoid combining heavy borders and shadows.
- **Internal Padding:** 16px to 20px for compact tools and messages.

### Inputs / Fields

- **Style:** White paper, Rule border, 12px corners, and Research Ink.
- **Focus:** Graph Blue border and focus ring.
- **Error / Disabled:** Danger Paper with dark red text; disabled state remains readable.

### Navigation

Navigation is centered, transparent, and compact, with a dark active label and a small clean yellow dot underneath. Each link retains a 44px target on every viewport, uses `aria-current` as a non-color cue, and exposes the navigation landmark by name.

### SPARQL Workbench

The workbench combines a quiet title bar, preset selector, syntax editor, and a single flag-red execution action. CodeMirror owns syntax color but inherits the shared flag-blue focus ring, yellow selection surface, and query typography.

### Ask Conversation

User prompts use dark ink bubbles; graph answers use paper surfaces with progressively disclosed query, row, citation, and provenance details. Streaming state remains textual and accessible before any animation is considered.

### Schema Field

The shared background is a deterministic set of graph fragments, held static at rest. Query execution may send one 900ms trace through the central relation path; it never drifts, follows the pointer, or loops. On compact screens the central spine is omitted so content remains dominant.

### Maps

Province geometry uses flag-blue outlines and pale blue fills where color supports orientation. On the home preview, hover and keyboard focus change the current province to sun yellow. Up to five flag-red region-centroid markers may show the highest real linked-record counts; omit them when ranking data is unavailable rather than fabricating hotspots. A visible legend and accessible text must explain all three signals. A selected province on the full map changes to sun yellow with a visible outline and an explicit name or pressed state. Region-level data may use a broader categorical palette when adjacent areas must remain distinguishable.

## Do's and Don'ts

### Do:

- **Do** keep evidence, provenance, and query details available through progressive disclosure.
- **Do** maintain at least 4.5:1 contrast for body, helper, placeholder, and status text.
- **Do** preserve 44px targets and visible blue keyboard focus across input methods.
- **Do** pair yellow with dark ink or blue and reinforce selection with shape, outline, text, or state attributes.
- **Do** stack content before it clips, and use dynamic viewport units for full-height task surfaces.
- **Do** stop nonessential work when motion is reduced or the page is not visible.

### Don't:

- **Don't** use perpetual ambient motion as a substitute for product identity.
- **Don't** use low-contrast gray to make essential instructions look secondary.
- **Don't** apply monospace to ordinary prose or serif type to routine controls.
- **Don't** manufacture depth with nested shadows, decorative blur, and repeated glass cards.
- **Don't** hide graph evidence to make an answer appear simpler than its provenance permits.
- **Don't** use equal doses of flag blue, red, and yellow or turn the interface into a literal flag composition.
