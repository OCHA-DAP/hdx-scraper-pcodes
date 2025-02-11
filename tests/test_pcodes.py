from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import read_list_from_csv
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.pcodes.pcodes import (
    get_global_pcodes,
    get_pcode_lengths,
    get_pcodes,
)


class TestPCodes:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
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

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "pcodes", "config")

    def test_pcodes(
        self,
        configuration,
        read_dataset,
        fixtures_dir,
        input_dir,
    ):
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

        with temp_dir(
            "TestPcodes",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                global_dataset = Dataset.load_from_json(
                    join(input_dir, "dataset-global-pcodes.json")
                )
                global_test_pcodes = get_global_pcodes(
                    global_dataset,
                    configuration["resource_name"]["all"],
                    retriever,
                )
                assert global_test_pcodes == global_pcodes

                test_pcode_lengths = get_pcode_lengths(global_pcodes)
                assert test_pcode_lengths == pcode_lengths

                pcodes = get_pcodes(retriever, "AFG", configuration, set())
                assert pcodes == afg_pcodes

                pcodes = get_pcodes(retriever, "ARM", configuration, set())
                assert pcodes == arm_pcodes

                pcodes = get_pcodes(retriever, "IDN", configuration, set())
                assert pcodes == idn_pcodes

                pcodes = get_pcodes(retriever, "MMR", configuration, set())
                assert pcodes == list()

                pcodes = get_pcodes(retriever, "BES", configuration, set())
                assert pcodes == list()

                pcodes = get_pcodes(retriever, "MKD", configuration, set())
                assert pcodes == list()
