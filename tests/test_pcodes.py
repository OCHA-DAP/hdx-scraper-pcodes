from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import read_list_from_csv
from hdx.utilities.useragent import UserAgent
from pcodes import *


class TestPCodes:
    country = "SOM"
    som_pcodes = read_list_from_csv(
        join("tests", "fixtures", "som_pcodes.csv"),
        headers=1,
        dict_form=True,
    )

    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def retriever(self):
        class Retrieve:
            @staticmethod
            def download_file(url):
                return join(
                    "tests", "fixtures", "download-som-adminboundaries-tabulardata.xlsx"
                )

        return Retrieve()

    def test_pcodes(self, configuration, retriever):
        dataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ab-som.json"))
        gazetteer = find_gazetteer(dataset, self.country)
        assert gazetteer["name"] == "SOM_AdminBoundaries_TabularData.xlsx"
        assert gazetteer["url"] == "https://data.humdata.org/dataset/ec140a63-5330-4376-a3df-c7ebf73cfc3c/resource/2bb93a9e-bb50-42e3-bd30-5b0f86b16ee5/download/som_adminboundaries_tabulardata.xlsx"

        open_gazetteer = get_data(gazetteer, retriever, self.country)
        assert list(open_gazetteer.keys()) == ["ADM1", "ADM2"]

        pcodes = get_pcodes(open_gazetteer, configuration["global_pcodes"]["headers"], self.country)
        assert pcodes == self.som_pcodes
