from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def config_dir(fixtures_dir):
    return join("src", "hdx", "scraper", "pcodes", "config")


@pytest.fixture(scope="function")
def read_dataset(monkeypatch):
    def read_from_hdx(dataset_name):
        if dataset_name == "cod-ab-mmr":
            return None
        return Dataset.load_from_json(
            join(
                "tests",
                "fixtures",
                "input",
                f"dataset-{dataset_name}.json",
            )
        )

    monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))


@pytest.fixture(scope="session")
def configuration(config_dir):
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=join(config_dir, "project_configuration.yaml"),
    )
    # Change locations below to match those needed in tests
    Locations.set_validlocations(
        [
            {"name": "afg", "title": "Afghanistan"},
            {"name": "arm", "title": "Armenia"},
            {"name": "bes", "title": "Bonaire, Sint Eustatius and Saba"},
            {"name": "idn", "title": "Indonesia"},
            {"name": "mkd", "title": "North Macedonia"},
            {"name": "world", "title": "World"},
        ]
    )
    Country.countriesdata(False)
    Vocabulary._approved_vocabulary = {
        "tags": [
            {"name": tag}
            # Change tags below to match those needed in tests
            for tag in (
                "administrative boundaries-divisions",
                "hxl",
            )
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return Configuration.read()
