import logging
import argparse
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.dictandlist import read_list_from_csv, write_list_to_csv

from get_pcodes import get_pcodes

logger = logging.getLogger(__name__)

lookup = "cods-summary"


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

    if not countries:
        countries = configuration["countries"]

    global_pcodes = read_list_from_csv("global_pcodes.csv", headers=1)

    with temp_dir() as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_folder, "saved_data", temp_folder, save, use_saved
            )

            logger.info("Updating global p-codes!")

            for country in countries:
                dataset = Dataset.read_from_hdx(f"cod-ab-{country.lower()}")
                if not dataset:
                    logger.warning(f"Could not find boundary dataset for {country}")
                    continue
                pcodes = get_pcodes(dataset, retriever)
                if not pcodes:
                    continue

                global_pcodes = [g for g in global_pcodes if g[0] != country]
                for pcode in pcodes:
                    global_pcodes.append([country, pcode])

            write_list_to_csv("global_pcodes.csv", rows=global_pcodes)

        logger.info("Finished processing")


if __name__ == "__main__":
    args = parse_args()
    countries = args.countries
    if countries:
        countries = countries.split(",")
    facade(
        main,
        hdx_site="prod",
        hdx_read_only=True,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        countries=countries,
        save=args.save,
        use_saved=args.use_saved,
    )
