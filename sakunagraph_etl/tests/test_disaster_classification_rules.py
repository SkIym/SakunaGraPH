from __future__ import annotations

import unittest

from sakunagraph_etl.enrichment._classification_rules import CLASSIFICATION_RULES


def matching_labels(text: str) -> list[str]:
    text_lower = text.lower()
    labels: list[str] = []
    for rule in CLASSIFICATION_RULES:
        tokens, label = rule[0], rule[1]
        context = rule[2] if len(rule) > 2 else None
        if not any(token.lower() in text_lower for token in tokens):
            continue
        if context and not any(token.lower() in text_lower for token in context):
            continue
        if label not in labels:
            labels.append(label)
    return labels


class DisasterClassificationRuleTests(unittest.TestCase):
    def test_gastroenteritis_terms_classify_as_general_infectious_disease(self) -> None:
        examples = (
            "Acute gastroenteritis outbreak",
            "Gastro-entritis cases",
            "Gastroentritis incident",
            "Diarrhea outbreak",
            "Diarrhoea outbreak",
        )

        for example in examples:
            with self.subTest(example=example):
                self.assertEqual(
                    matching_labels(example),
                    ["InfectiousDiseaseGeneral"],
                )

    def test_named_bacterial_cause_remains_more_specific(self) -> None:
        self.assertEqual(
            matching_labels("Cholera diarrhea outbreak"),
            ["BacterialDisease", "InfectiousDiseaseGeneral"],
        )


if __name__ == "__main__":
    unittest.main()
