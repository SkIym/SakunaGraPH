from typing import TypeAlias

ClassificationRule: TypeAlias = (
    tuple[list[str], str] |
    tuple[list[str], str, list[str]]
)

# Each entry: ([trigger_tokens, ...], class_label)
#         or: ([trigger_tokens, ...], class_label, [context_tokens, ...])
# Matching: case-insensitive substring.
#   - Any trigger token must appear in the text.
#   - If context_tokens is present, at least one must also appear (AND requirement).
#
# ORDERING NOTE: ordering no longer affects correctness for labels listed in
# AMBIGUOUS_GROUPS — _rule_candidates() collects ALL firing rules, and the
# transformer resolves among them.  Ordering still matters for label groups
# NOT in AMBIGUOUS_GROUPS (first-match semantics retained there).

# Water indicators required to confirm a "wet" mass-movement classification.
_WET_CONTEXT = ["rain", "rainfall", "water", "wet", "flood", "saturated",
                "runoff", "storm", "monsoon", "downpour", "moisture", "typhoon", "tropical"]

# ── Ambiguous groups ──────────────────────────────────────────────────────────
# Sets of labels that must NEVER be hard-won against each other.
# When every candidate label fired by a text belongs to the same group, the
# transformer resolves the final label (candidate set = that group's labels).
# If candidates span multiple groups, the transformer resolves over the full
# union of those groups.
AMBIGUOUS_GROUPS: list[frozenset[str]] = [

    # Mass-movement wet/dry pairs — context tokens help but aren't definitive
    frozenset({"LandslideWet", "LandslideDry"}),
    frozenset({"RockfallWet", "RockfallDry"}),
    frozenset({"AvalancheWet", "AvalancheDry"}),
    frozenset({"SuddenSubsidenceWet", "SuddenSubsidenceDry"}),

    # Industrial vs Miscellaneous splits
    frozenset({"FireIndustrial", "FireMiscellaneous", "ForestFire", "LandFire", "WildfireGeneral"}),
    frozenset({"ExplosionIndustrial", "ExplosionMiscellaneous"}),
    frozenset({"CollapseIndustrial", "CollapseMiscellaneous"}),
]

# Flat lookup: label → its ambiguity group (built at import time)
LABEL_TO_GROUP: dict[str, frozenset[str]] = {
    label: group
    for group in AMBIGUOUS_GROUPS
    for label in group
}


def labels_are_ambiguous(candidates: list[str]) -> bool:
    """Return True if all candidates share at least one ambiguity group."""
    if len(candidates) <= 1:
        return False
    groups = [LABEL_TO_GROUP.get(c) for c in candidates]
    if any(g is None for g in groups):
        return False
    # All candidates must share a common group
    return bool(groups[0].intersection(*groups[1:]))  # type: ignore[arg-type]


def ambiguous_candidate_set(candidates: list[str]) -> list[str]:
    """
    Return the full label set the transformer should choose among when
    candidates are ambiguous.  This is the union of all their groups,
    so the transformer sees every plausible sibling — not just the ones
    that happened to fire.
    """
    union: set[str] = set()
    for label in candidates:
        group = LABEL_TO_GROUP.get(label)
        if group:
            union |= group
    return sorted(union)


CLASSIFICATION_RULES: list[ClassificationRule] = [
    # ── Hydrological ─────────────────────────────────────────────────────────
    (["tsunami"],                                                                "Tsunami"),
    (["flash flood", "flashflood", "flash floods"],                             "FlashFlood"),
    (["coastal flood", "coastal inundation", "tidal flood", "coastal",
      "sea water intrusion", "saltwater intrusion", "coastal surge"],           "CoastalFlood"),
    (["riverine flood", "river overflow", "overflowing river",
      "swollen river", "overflow", "riverine"],                                             "RiverineFlood"),
    (["ice jam"],                                                                "IceJamFlood"),
    (["flood", "flooding", "inundation", "flooded"],                            "FloodGeneral"),

    # ── Mass movement ─────────────────────────────────────────────────────────
    (["mudslide", "mudflow", "mud slide"],                                       "Mudslide"),
    (["landslide", "land slide"],                                                "LandslideWet",         _WET_CONTEXT),
    (["landslide", "land slide"],                                                "LandslideDry"),
    (["rockfall", "rock fall", "boulder", "rockslide"],                         "RockfallWet",          _WET_CONTEXT),
    (["rockfall", "rock fall", "boulder", "rockslide"],                         "RockfallDry"),
    (["avalanche (dry)", "dry avalanche"],                                       "AvalancheDry"),
    (["avalanche"],                                                              "AvalancheWet",         _WET_CONTEXT),
    (["avalanche"],                                                              "AvalancheDry"),
    (["subsidence"],                                                             "SuddenSubsidenceWet",  _WET_CONTEXT),
    (["subsidence", "sinkhole", "liquefaction"],                                "SuddenSubsidenceDry"),

    # ── Wave action ───────────────────────────────────────────────────────────
    (["rogue wave", "rogue"],                                                             "RogueWave"),
    (["seiche"],                                                                 "Seiche"),
    (["big waves", "large waves", "big wave"],                                   "WaveActionGeneral"),

    # ── Transport ─────────────────────────────────────────────────────────────
    (["vehicular accident", "road accident", "road crash",
      "road collision", "collision"],                                            "Road"),
    (["boat sinking", "capsized", "motorbanca", "maritime accident",
      "ferry accident", "vessel accident", "maritime incident"],                 "Water"),
    (["plane crash", "aircraft accident", "aviation accident", "plane", "airplane", "aircraft"],                  "Air"),
    (["train accident", "rail accident", "derailment"],                          "Rail"),

    # ── Technological ─────────────────────────────────────────────────────────
    (["drowning", "firework", "miscellaneous accident", "observance", "holiday",
      "election", "boga", "firecracker", "celebration", "year-end"],            "MiscellaneousAccidentGeneral"),
    (["armed conflict", "armed encounter", "shooting", "ambush",
      "gunfire", "gun fire", "gun battle", "disorganization"],                  "ArmedConflict"),

    (["explosion", "bomb", "grenade", "blast"],                                 "ExplosionIndustrial"),
    (["explosion", "bomb", "grenade", "blast"],                                  "ExplosionMiscellaneous"),
    (["fire", "industrial fire", "factory fire", "warehouse fire"],              "FireIndustrial"),
    (["fire", "blaze", "conflagration"],                                         "FireMiscellaneous"),
    (["industrial collapse", "bridge collapse", "tower collapse"],               "CollapseIndustrial"),
    (["building collapse", "structure collapse", "collapse",
      "demolition", "fallen tree", "fallen debris", "uprooted", "fallen"],       "CollapseMiscellaneous"),
    (["oil spill", "oilspill", "oil leak"],                                      "OilSpill"),
    (["chemical spill", "hazmat", "chemical leak"],                              "ChemicalSpill"),
    (["gas leak", "lpg leak"],                                                   "GasLeak"),
    (["radiation", "nuclear"],                                                   "Radiation"),
    (["poisoning", "food poisoning", "contamination"],                           "Poisoning"),
    (["industrial accident"],                                                    "IndustrialAccidentGeneral"),

    # ── Geophysical ───────────────────────────────────────────────────────────
    (["earthquake", "aftershock", "tremor", "seismic"],                          "GroundMovement"),
    (["pyroclastic"],                                                             "PyroclasticFlow"),
    (["lava flow", "lava"],                                                       "LavaFlow"),
    (["lahar"],                                                                   "Lahar"),
    (["ashfall", "ash fall", "ash cloud"],                                        "Ashfall"),
    (["volcanic", "eruption", "volcano"],                                         "VolcanicActivityGeneral"),

    # ── Meteorological ────────────────────────────────────────────────────────
    (["storm surge"],                                                             "StormSurge"),
    (["tornado", "waterspout", "whirlwind"],                                      "Tornado"),
    (["thunderstorm", "lightning", "thunder"],                                    "Thunderstorms"),
    (["monsoon", "habagat", "amihan", "shear line",
      "low pressure area", "LPA", "SWM"],                                        "StormGeneral"),
    (["typhoon", "tropical storm", "tropical cyclone",
      "tropical depression", "super typhoon", "landfall"],                       "TropicalCyclone"),
    (["strong wind", "severe weather", "gale", "squall", "continuous"],           "SevereWeather"),
    (["hail", "hailstorm"],                                                       "Hail"),
    (["sand storm", "sandstorm", "dust storm"],                                   "SandStorm"),
    (["blizzard"],                                                                "BlizzardStorm"),
    (["derecho"],                                                                 "Derecho"),
    (["extratropical storm", "extra-tropical storm"],                             "ExtratropicalStorm"),
    (["heat wave", "heat stroke", "extreme heat"],                                "HeatWave"),
    (["cold wave", "cold spell", "cold snap"],                                    "ColdWave"),
    (["severe winter", "snowstorm", "freezing rain"],                             "SevereWinterConditions"),
    (["fog"],                                                                     "Fog"),

    # ── Climatological ────────────────────────────────────────────────────────
    (["drought", "dry spell", "el nino"],                                         "Drought"),
    (["glacial lake outburst", "glacial"],                                        "Glacial"),
    (["forest fire"],                                                             "ForestFire"),
    (["land fire", "grass fire", "grassfire", "brush fire"],                      "LandFire"),
    (["wildfire", "bush fire", "wild fire"],                                                   "WildfireGeneral"),

    # ── Biological ────────────────────────────────────────────────────────────
    (["animal attack", "snake bite", "dog bite"],                                 "AnimalAccident"),
    (["cholera", "salmonella", "bacterial disease"],                              "BacterialDisease"),
    (["dengue", "covid", "influenza", "flu", "viral disease", "rabies"],          "ViralDisease"),
    (["malaria", "parasitic disease"],                                            "ParasiticDisease"),
    (["fungal disease", "fungal infection"],                                      "FungalDisase"),
    (["prion disease"],                                                           "PrionDisease"),
    (["epidemic", "disease outbreak", "infectious disease"],                      "InfectiousDiseaseGeneral"),
    (["locust"],                                                                  "LocustInfestation"),
    (["grasshopper"],                                                 "GrasshopperInfestation"),
    (["worm infestation", "worms infestation", "worms"],                                   "WormsInfestation"),
    (["infestation", "pest infestation", "red tide", "pest"],                             "InfestationGeneral"),

    # ── Extraterrestrial ──────────────────────────────────────────────────────
    (["meteorite", "asteroid", "space impact"],                                   "SpaceImpact"),
    (["geomagnetic storm", "solar flare", "space weather"],                       "SpaceWeather"),
]