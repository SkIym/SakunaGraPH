import unittest
from unittest.mock import AsyncMock, patch

from src.services.ontology.graph import (
    _GRAPH_DATAPROPS_QUERY,
    _build_graph,
    get_ontology_graph,
)


def _binding(**values: str) -> dict[str, dict[str, str]]:
    return {key: {"value": value} for key, value in values.items()}


class OntologyGraphTests(unittest.IsolatedAsyncioTestCase):
    def test_datatype_query_rejects_dual_typed_object_properties(self) -> None:
        self.assertIn(
            "FILTER NOT EXISTS { ?prop a owl:ObjectProperty }",
            _GRAPH_DATAPROPS_QUERY,
        )

    def test_data_properties_stay_on_their_declared_class(self) -> None:
        graph = _build_graph(
            class_bindings=[
                _binding(
                    **{
                        "class": "https://sakuna.ph/DisasterEvent",
                        "label": "Disaster Event",
                    }
                ),
                _binding(
                    **{
                        "class": "https://sakuna.ph/AffectedPopulation",
                        "label": "Affected Population",
                    }
                ),
            ],
            subclassof_bindings=[],
            objprop_bindings=[],
            dataprop_bindings=[
                _binding(
                    **{
                        "class": "https://sakuna.ph/DisasterEvent",
                        "propLabel": "event name",
                        "range": "http://www.w3.org/2001/XMLSchema#string",
                    }
                ),
                _binding(
                    **{
                        "class": "https://sakuna.ph/AffectedPopulation",
                        "propLabel": "affected persons",
                        "range": "http://www.w3.org/2001/XMLSchema#int",
                    }
                ),
            ],
        )

        nodes = {node.id: node for node in graph.nodes}
        self.assertEqual(
            [prop.label for prop in nodes["DisasterEvent"].dataProperties or []],
            ["event name"],
        )
        self.assertEqual(
            [prop.label for prop in nodes["AffectedPopulation"].dataProperties or []],
            ["affected persons"],
        )

    @patch("src.services.ontology.graph.execute_sparql", new_callable=AsyncMock)
    async def test_datatype_property_query_uses_only_asserted_domains(self, execute) -> None:
        execute.side_effect = [
            {"results": {"bindings": []}},
            {"results": {"bindings": []}},
            {"results": {"bindings": []}},
            {"results": {"bindings": []}},
        ]

        await get_ontology_graph()

        self.assertEqual(execute.await_count, 4)
        self.assertEqual(execute.await_args_list[3].kwargs, {"include_inferred": False})


if __name__ == "__main__":
    unittest.main()
