import logging
import argparse
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from pcodes import check_parents, get_global_pcodes, get_pcodes, get_pcode_lengths

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

    with ErrorsOnExit() as errors_on_exit:
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
                    pcodes = get_pcodes(retriever, country, configuration, errors_on_exit)

                    if len(pcodes) == 0:
                        continue

                    missing_parents = check_parents(pcodes)
                    if len(missing_parents) > 0:
                        errors_on_exit.add(f"{country}: parent units {', '.join(missing_parents)} missing")

                    global_pcodes = [g for g in global_pcodes if g["Location"] != country]
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

                pcode_lengths = get_pcode_lengths(global_pcodes)

                adm12_pcodes = [global_pcodes[0]] + [
                    g for g in global_pcodes if g["Admin Level"] in ["1", "2"]
                ]

                temp_file_all = join(temp_folder, configuration["resource_name"]["all"])
                temp_file_12 = join(temp_folder, configuration["resource_name"]["adm_12"])
                temp_file_lengths = join(temp_folder, configuration["resource_name"]["lengths"])
                write_list_to_csv(temp_file_all, rows=global_pcodes)
                write_list_to_csv(temp_file_12, rows=adm12_pcodes)
                write_list_to_csv(temp_file_lengths, rows=pcode_lengths)

                for resource in global_dataset.get_resources():
                    if resource["name"] == configuration["resource_name"]["all"]:
                        resource.set_file_to_upload(temp_file_all)
                    if resource["name"] == configuration["resource_name"]["adm_12"]:
                        resource.set_file_to_upload(temp_file_12)
                    if resource["name"] == configuration["resource_name"]["lengths"]:
                        resource.set_file_to_upload(temp_file_lengths)

                min_date = min([entry["Valid from date"] for entry in global_pcodes[1:]])
                global_dataset.set_time_period(startdate=min_date, ongoing=True)
                global_dataset.update_in_hdx(
                    hxl_update=False,
                    updated_by_script="HDX Scraper: Global P-codes",
                )

            if len(errors_on_exit.errors) > 0:
                errors = list(set(errors_on_exit.errors))
                errors.sort()
                with open("errors.txt", "w") as fp:
                    fp.writelines(_ + "  \n" for _ in errors)
            logger.info("Finished processing")


if __name__ == "__main__":
    args = parse_args()
    countries = args.countries
    if countries:
        countries = countries.split(",")
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
        countries=countries,
        save=args.save,
        use_saved=args.use_saved,
    )
