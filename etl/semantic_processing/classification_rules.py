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
# Rules are checked in order; first match wins.
#
# ORDERING RULE: when token A is a substring of token B (e.g. "flood" ⊂ "flash flood"),
# the more specific entry (B) must appear BEFORE the generic one (A).

# Water indicators required to confirm a "wet" mass-movement classification.
_WET_CONTEXT = ["rain", "rainfall", "water", "wet", "flood", "saturated",
                "runoff", "storm", "monsoon", "downpour", "moisture", "typhoon"]

CLASSIFICATION_RULES: list[ClassificationRule] = [
    # ── Hydrological ─────────────────────────────────────────────────────────
    # Specific flood subtypes before the generic "flood" token
    (["tsunami"],                                                                    "Tsunami"),
    (["flash flood", "flashflood"],                                                  "FlashFlood"),
    (["coastal flood"],                                                              "CoastalFlood"),
    (["riverine flood", "river overflow", "overflowing river", "swollen river", "overflow"],     "RiverineFlood"),
    (["ice jam"],                                                                    "IceJamFlood"),
    (["flood", "flooding", "inundation"],                                            "FloodGeneral"),
    # Specific mass-movement subtypes before generic tokens
    (["mudslide", "mudflow", "mud slide"],                                                        "Mudslide"),
    (["landslide", "land slide"],                                                                  "LandslideWet",        _WET_CONTEXT),
    (["land slide", "landslide"],                                           "LandslideDry"),
    (["rockfall", "rock fall", "boulder", "rockslide"],                                           "RockfallWet",         _WET_CONTEXT),
    (["rockslide", "rockfall", "rock fall", "boulder"],                                             "RockfallDry"),
    (["avalanche (dry)", "dry avalanche"],                                           "AvalancheDry"),
    (["avalanche"],                                                                  "AvalancheWet",        _WET_CONTEXT),
    (["subsidence"],                                                    "SuddenSubsidenceDry"),
    (["subsidence", "sinkhole", "liquefaction"],                              "SuddenSubsidenceWet"),
    # Wave action — specific subtypes first, then generic parent
    (["rogue wave"],                                                                 "RogueWave"),
    (["seiche"],                                                                     "Seiche"),
    (["big waves", "large waves", "big wave"],                                                   "WaveActionGeneral"),

     # ── Transport ─────────────────────────────────────────────────────────────
    (["vehicular accident", "road accident", "road crash", "road collision", "collision"],        "Road"),
    (["boat sinking", "capsized", "motorbanca",
      "maritime accident", "ferry accident", "vessel accident", "maritime incident"],                      "Water"),
    (["plane crash", "aircraft accident", "aviation accident"],                      "Air"),
    (["train accident", "rail accident", "derailment"],                              "Rail"),
    
    # ── Technological ─────────────────────────────────────────────────────────
    (["armed conflict", "armed encounter", "shooting", "ambush",
      "gunfire", "gun fire", "gun battle", "disorganization"],                                           "ArmedConflict"),
    (["industrial explosion", "factory explosion"],                                  "ExplosionIndustrial"),
    (["explosion", "bomb", "grenade", "blast"],                                      "ExplosionMiscellaneous"),
    (["industrial fire", "factory fire", "warehouse fire"],                          "FireIndustrial"),
    (["fire", "blaze", "conflagration"],                                             "FireMiscellaneous"),
    (["industrial collapse", "bridge collapse", "tower collapse"],                   "CollapseIndustrial"),
    (["building collapse", "structure collapse",
      "collapse", "demolition", "fallen tree", "fallen debris", "uprooted"],                      "CollapseMiscellaneous"),
    (["oil spill", "oilspill", "oil leak"],                                                                  "OilSpill"),
    (["chemical spill", "hazmat", "chemical leak"],                                                   "ChemicalSpill"),
    (["gas leak", "lpg leak"],                                                       "GasLeak"),
    (["radiation", "nuclear"],                                                       "Radiation"),
    (["poisoning", "food poisoning", "contamination"],                               "Poisoning"),
    (["industrial accident"],                                                        "IndustrialAccidentGeneral"),
    (["drowning", "fireworks", "miscellaneous accident", "observance", "holiday", "election", "boga"],                            "MiscellaneousAccidentGeneral"),

    # ── Geophysical ───────────────────────────────────────────────────────────
    (["earthquake", "aftershock", "tremor", "seismic"],                              "GroundMovement"),
    (["pyroclastic"],                                                                "PyroclasticFlow"),
    (["lava flow", "lava"],                                                          "LavaFlow"),
    (["lahar"],                                                                      "Lahar"),
    (["ashfall", "ash fall", "ash cloud"],                                           "Ashfall"),
    (["volcanic", "eruption", "volcano"],                                            "VolcanicActivityGeneral"),

    # ── Meteorological ────────────────────────────────────────────────────────
    (["storm surge"],                                                                "StormSurge"),
    (["tornado", "waterspout", "whirlwind"],                                         "Tornado"),
    (["thunderstorm", "lightning", "thunder"],                                       "Thunderstorms"),
    (["monsoon", "habagat", "amihan", "shear line", "low pressure area", "LPA", "SWM"],     "StormGeneral"),
    (["typhoon", "tropical storm", "tropical cyclone",
      "tropical depression", "super typhoon", "landfall"],                                       "TropicalCyclone"),
    (["strong wind", "severe weather", "gale", "squall", "continuous"],                            "SevereWeather"),
    (["hail", "hailstorm"],                                                          "Hail"),
    (["sand storm", "sandstorm", "dust storm"],                                      "SandStorm"),
    (["blizzard"],                                                                   "BlizzardStorm"),
    (["derecho"],                                                                    "Derecho"),
    (["extratropical storm", "extra-tropical storm"],                                "ExtratropicalStorm"),
    (["heat wave", "heat stroke", "extreme heat"],                                   "HeatWave"),
    (["cold wave", "cold spell", "cold snap"],                                       "ColdWave"),
    (["severe winter", "snowstorm", "freezing rain"],                                "SevereWinterConditions"),
    (["fog"],                                                                        "Fog"),

    # ── Climatological ────────────────────────────────────────────────────────
    (["drought", "dry spell", "el nino"],                                            "Drought"),
    (["glacial lake outburst", "glacial"],                                           "Glacial"),
    # Wildfire subtypes before generic "fire" token in Technological section
    (["forest fire"],                                                                "ForestFire"),
    (["land fire", "grass fire", "grassfire", "brush fire"],                         "LandFire"),
    (["wildfire", "bush fire"],                                                      "WildfireGeneral"),

    # ── Biological ────────────────────────────────────────────────────────────
    (["animal attack", "snake bite", "dog bite"],                                    "AnimalAccident"),
    (["cholera", "salmonella", "bacterial disease"],                                 "BacterialDisease"),
    (["dengue", "covid", "influenza", "flu", "viral disease", "rabies"],             "ViralDisease"),
    (["malaria", "parasitic disease"],                                               "ParasiticDisease"),
    (["fungal disease", "fungal infection"],                                         "FungalDisase"),
    (["prion disease"],                                                              "PrionDisease"),
    (["epidemic", "disease outbreak", "infectious disease"],                         "InfectiousDiseaseGeneral"),
    (["locust"],                                                                     "LocustInfestation"),
    (["grasshopper infestation"],                                                    "GrasshopperInfestation"),
    (["worm infestation", "worms infestation"],                                      "WormsInfestation"),
    (["infestation", "pest infestation", "red tide"],                                "InfestationGeneral"),

    # ── Extraterrestrial ──────────────────────────────────────────────────────
    (["meteorite", "asteroid", "space impact"],                                      "SpaceImpact"),
    (["geomagnetic storm", "solar flare", "space weather"],                          "SpaceWeather"),
]
