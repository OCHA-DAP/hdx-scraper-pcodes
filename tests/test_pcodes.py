from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import read_list_from_csv
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from pcodes import *


class TestPCodes:
    country = "AFG"
    som_pcodes = read_list_from_csv(
        join("tests", "fixtures", "afg_pcodes.csv"),
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
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_pcodes(self, configuration, fixtures, input_folder):
        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )

                dataset = Dataset.load_from_json(join(input_folder, "dataset-cod-ab-afg.json"))
                gazetteer = find_gazetteer(dataset, self.country)
                assert gazetteer["name"] == "AFG_AdminBoundaries_TabularData.xlsx"
                assert gazetteer["url"] == "https://data.humdata.org/dataset/4c303d7b-8eae-4a5a-a3aa-b2331fa39d74/resource/0238eb07-4f98-4f71-9a03-905c4414f476/download/afg_adminboundaries_tabulardata.xlsx"

                open_gazetteer = get_data(gazetteer, retriever, self.country)
                assert list(open_gazetteer.keys()) == ["ADM1", "ADM2"]

                pcodes = get_pcodes(open_gazetteer, configuration["global_pcodes"]["headers"], self.country)
                assert pcodes == self.som_pcodes
