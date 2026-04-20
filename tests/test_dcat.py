import unittest

from app.harvester import build_dcat_catalog


class BuildDcatCatalogTests(unittest.TestCase):
    def test_build_dcat_catalog_adds_catalog_metadata_and_distributions(self) -> None:
        catalog = build_dcat_catalog(
            [
                {
                    "thing_id": 5,
                    "thing_name": "Station Alpha",
                    "datastream_name": "Air Temperature",
                    "description": "Temperature observations from a fixed station.",
                    "location": {"lat": 46.2, "lon": 6.1},
                    "sensor_type": "Thermometer",
                    "observed_property": "Temperature",
                    "unit_of_measurement": "degC",
                    "observation_type": "http://example.com/observation-type",
                    "sampling_frequency": "PT5M",
                    "time_range": "2026-01-01T00:00:00Z/2026-01-31T23:59:59Z",
                    "start_time": "2026-01-01T00:00:00Z",
                    "end_time": "2026-01-31T23:59:59Z",
                    "last_observation_time": "2026-01-31T23:59:59Z",
                    "datastream_id": 42,
                }
            ],
            metadata={
                "catalog_url": "https://catalog.example.test/dcat/catalog",
                "title": "Example Sensor Catalog",
                "description": "Interoperable sensor datasets",
                "homepage": "https://catalog.example.test/",
                "endpoint": "https://api.example.test/istsos4/v1.1",
                "language": "en",
                "theme_taxonomy": "https://example.test/themes",
                "publisher_name": "Example Publisher",
                "publisher_homepage": "https://example.test/org",
                "publisher_mbox": "data@example.test",
            },
        )

        self.assertEqual(catalog["@id"], "https://catalog.example.test/dcat/catalog")
        self.assertEqual(catalog["dct:title"], "Example Sensor Catalog")
        self.assertEqual(catalog["dct:publisher"]["foaf:name"], "Example Publisher")
        self.assertEqual(catalog["dcat:themeTaxonomy"]["@id"], "https://example.test/themes")

        datasets = catalog["dcat:dataset"]
        self.assertEqual(len(datasets), 1)

        dataset = datasets[0]
        self.assertEqual(dataset["dct:identifier"], "datastream-42")
        self.assertEqual(dataset["dct:issued"], "2026-01-01T00:00:00Z")
        self.assertEqual(dataset["dct:modified"], "2026-01-31T23:59:59Z")
        self.assertEqual(
            dataset["dcat:landingPage"]["@id"],
            "https://api.example.test/istsos4/v1.1/Datastreams(42)",
        )
        self.assertIn("Temperature", dataset["dcat:keyword"])
        self.assertIn("degC", dataset["dcat:keyword"])
        self.assertEqual(dataset["dct:publisher"]["foaf:mbox"], "mailto:data@example.test")
        self.assertEqual(dataset["dct:conformsTo"]["@id"], "http://example.com/observation-type")

        distributions = dataset["dcat:distribution"]
        self.assertEqual(len(distributions), 2)
        self.assertEqual(
            distributions[0]["dcat:accessURL"]["@id"],
            "https://api.example.test/istsos4/v1.1/Datastreams(42)",
        )
        self.assertEqual(
            distributions[1]["dcat:downloadURL"]["@id"],
            "https://api.example.test/istsos4/v1.1/Datastreams(42)/Observations",
        )


if __name__ == "__main__":
    unittest.main()
