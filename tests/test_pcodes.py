from os.path import join

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.pcodes.pcodes import Pcodes


class TestPCodes:
    def test_pcodes(
        self,
        configuration,
        read_dataset,
        fixtures_dir,
        input_dir,
        config_dir,
    ):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestPcodes",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir:
                with Download(user_agent="test") as downloader:
                    retriever = Retrieve(
                        downloader=downloader,
                        fallback_dir=tempdir,
                        saved_dir=input_dir,
                        temp_dir=tempdir,
                        save=False,
                        use_saved=True,
                    )

                    pcodes = Pcodes(
                        configuration=configuration,
                        retriever=retriever,
                        temp_folder=tempdir,
                        error_handler=error_handler,
                    )
                    countries = ["AFG", "ARM", "BES", "IDN", "MKD"]
                    for country in countries:
                        pcodes.get_pcodes(country)
                        pcodes.check_parents(country)
                        pcodes.get_pcode_lengths(country)

                    assert len(pcodes.pcodes) == 3
                    assert len(pcodes.pcode_lengths) == 3

                    dataset = pcodes.generate_dataset()
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
                    assert dataset == {
                        "name": "global-pcodes",
                        "title": "Global P-Code List",
                        "groups": [{"name": "world"}],
                        "tags": [
                            {
                                "name": "administrative boundaries-divisions",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "dataset_date": "[2014-10-01T00:00:00 TO *]",
                        "license_id": "cc-by",
                        "methodology": "Other",
                        "methodology_other": "Pulled from COD gazetteers",
                        "caveats": "",
                        "dataset_source": "HDX",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                        "owner_org": "hdx",
                        "data_update_frequency": 7,
                        "notes": "CSV containing subnational p-codes, their "
                        "corresponding administrative names, parent p-codes, and "
                        "reference dates for the world (where available). These are "
                        "constructed using the COD gazetteers.\nEnglish names are used "
                        "where available, followed by names written in Latin alphabets."
                        "\nNote that Indonesia admin4 is not included for now, as that "
                        "data is contained in a second, non-standard gazetteer.\n",
                    }

                    resources = dataset.get_resources()
                    assert resources == [
                        {
                            "name": "global_pcodes.csv",
                            "description": "Table contains the 3-digit ISO code, admin "
                            "level, p-code, administrative name, parent p-code, and "
                            "date.",
                            "p_coded": True,
                            "format": "csv",
                        },
                        {
                            "name": "global_pcodes_adm_1_2.csv",
                            "description": "Data for admin levels 1 and 2. Table "
                            "contains the 3-digit ISO code, admin level, p-code, "
                            "administrative name, parent p-code, and date.",
                            "p_coded": True,
                            "format": "csv",
                        },
                        {
                            "name": "global_pcode_lengths.csv",
                            "description": "P-code lengths for all countries at all "
                            "levels. Table contains the 2 or 3 digit ISO code present in "
                            "the p-codes, and p-code lengths.",
                            "format": "csv",
                        },
                    ]

                    for file_name in [
                        "global_pcodes.csv",
                        "global_pcodes_adm_1_2.csv",
                        "global_pcode_lengths.csv",
                    ]:
                        assert_files_same(
                            join("tests", "fixtures", file_name),
                            join(tempdir, file_name),
                        )
