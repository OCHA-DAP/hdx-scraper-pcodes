from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import read_list_from_csv
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from pcodes import get_global_pcodes, get_pcodes, get_pcode_lengths


class TestPCodes:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def Dataset(self):
        class Dataset:
            @staticmethod
            def read_from_hdx(dataset_name):
                if dataset_name == "cod-ab-mmr":
                    return None
                return Dataset.load_from_json(join("tests", "input", f"dataset-{dataset_name}.json"))

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_pcodes(self, configuration, fixtures, input_folder):
        afg_pcodes = read_list_from_csv(
            join("tests", "fixtures", "afg_pcodes.csv"),
            headers=1,
            dict_form=True,
        )
        arm_pcodes = read_list_from_csv(
            join("tests", "fixtures", "arm_pcodes.csv"),
            headers=1,
            dict_form=True,
        )
        idn_pcodes = read_list_from_csv(
            join("tests", "fixtures", "idn_pcodes.csv"),
            headers=1,
            dict_form=True,
        )
        global_pcodes = read_list_from_csv(
            join("tests", "fixtures", "input", "download-global-pcodes.csv"),
            headers=1,
            dict_form=True,
        )
        pcode_lengths = read_list_from_csv(
            join("tests", "fixtures", "global_pcode_lengths.csv"),
            headers=1,
            dict_form=True,
        )

        with temp_dir() as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                global_dataset = Dataset.load_from_json(join(input_folder, "dataset-global-pcodes.json"))
                global_test_pcodes = get_global_pcodes(
                    global_dataset,
                    configuration["resource_name"]["all"],
                    retriever,
                )
                assert global_test_pcodes == global_pcodes

                test_pcode_lengths = get_pcode_lengths(global_pcodes)
                assert test_pcode_lengths == pcode_lengths

                pcodes = get_pcodes(retriever, "AFG", configuration)
                assert pcodes == afg_pcodes

                pcodes = get_pcodes(retriever, "ARM", configuration)
                assert pcodes == arm_pcodes

                pcodes = get_pcodes(retriever, "IDN", configuration)
                assert pcodes == idn_pcodes

                pcodes = get_pcodes(retriever, "MMR", configuration)
                assert pcodes == list()

                pcodes = get_pcodes(retriever, "BES", configuration)
                assert pcodes == list()

                pcodes = get_pcodes(retriever, "MKD", configuration)
                assert pcodes == list()
