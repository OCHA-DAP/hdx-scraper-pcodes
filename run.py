import logging
import argparse
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.dictandlist import write_list_to_csv

from pcodes import get_global_pcodes, get_pcodes

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-pcodes"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-co", "--countries", default=None, help="Which countries to check")
    parser.add_argument("-sv", "--save", default=False, action="store_true", help="Save downloaded data")
    parser.add_argument("-usv", "--use_saved", default=False, action="store_true", help="Use saved data")
    args = parser.parse_args()
    return args


def main(
    countries,
    save,
    use_saved,
    **ignore,
):
    configuration = Configuration.read()

    logger.info("##### Updating global p-codes #####")

    if not countries:
        countries = [key for key in Country.countriesdata()["countries"]]

    with temp_dir() as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_folder, "saved_data", temp_folder, save, use_saved
            )

            global_dataset = Dataset.read_from_hdx(configuration["dataset_name"])
            global_pcodes = get_global_pcodes(
                global_dataset,
                configuration["resource_name"]["all"],
                retriever,
            )

            for country in countries:
                pcodes = get_pcodes(retriever, country, configuration)

                if len(pcodes) == 0:
                    continue

                global_pcodes = [g for g in global_pcodes if g[configuration["headers"]["country"]] != country]
                for pcode in pcodes:
                    global_pcodes.append(pcode)

            global_pcodes = [global_pcodes[0]] + sorted(
                global_pcodes[1:],
                key=lambda k: (
                    k["Location"],
                    k["Admin Level"],
                    k["P-Code"],
                ),
            )

            adm12_pcodes = [global_pcodes[0]] + [
                g for g in global_pcodes if g[configuration["headers"]["level"]] in ["1", "2"]
            ]

            temp_file_all = join(temp_folder, configuration["resource_name"]["all"])
            temp_file_12 = join(temp_folder, configuration["resource_name"]["adm_12"])
            write_list_to_csv(temp_file_all, rows=global_pcodes)
            write_list_to_csv(temp_file_12, rows=adm12_pcodes)

            for resource in global_dataset.get_resources():
                if resource["name"] == configuration["resource_name"]["all"]:
                    resource.set_file_to_upload(temp_file_all)
                if resource["name"] == configuration["resource_name"]["adm_12"]:
                    resource.set_file_to_upload(temp_file_12)
            global_dataset.update_in_hdx(
                hxl_update=False,
                updated_by_script="HDX Scraper: Global P-codes",
            )

        logger.info("Finished processing")


if __name__ == "__main__":
    args = parse_args()
    countries = args.countries
    if countries:
        countries = countries.split(",")
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        countries=countries,
        save=args.save,
        use_saved=args.use_saved,
    )
