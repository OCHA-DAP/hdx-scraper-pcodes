import logging
import re
from pandas import read_excel
from unicodedata import normalize

logger = logging.getLogger(__name__)


def get_global_pcodes(dataset, dataset_info, retriever):
    resource = [r for r in dataset.get_resources() if r["name"] == dataset_info["name"]]

    _, iterator = retriever.get_tabular_rows(resource[0]["url"], dict_form=True)

    pcodes = list()
    for row in iterator:
        pcodes.append(row)
    return pcodes


def find_gazetteer(dataset, country):
    resources = [r for r in dataset.get_resources() if r.get_file_type() in ["xlsx", "xls"]]
    if len(resources) == 0:
        logger.error(f"Could not find gazetteer in {dataset['name']}")
        return None

    if len(resources) > 1:
        resources = [r for r in resources if "gazetteer" in r["description"].lower() or
                     "taxonomy" in r["description"].lower() or
                     bool(re.match(".*adm.*tabular.?data.*", r["name"], re.IGNORECASE))]

    if len(resources) == 0:
        logger.error(f"{country}: Could not find gazetteer in {dataset['name']}")
        return None

    return resources[0]


def get_data(resource, retriever, country):
    filepath = retriever.download_file(resource["url"])
    try:
        data = read_excel(filepath, sheet_name=None)
    except:
        logger.error(f"Could not read {resource['name']}")
        return dict()
    sheetnames = [s for s in data if bool(re.match(".*adm(in)?.?[1-7].*", s, re.IGNORECASE))]

    if len(sheetnames) == 0:
        logger.error(f"{country}: Could not find correct tab in {resource['name']}")
        return dict()

    data_subset = {key: data[key] for key in data if key in sheetnames}
    return data_subset


def get_pcodes(data, pcode_headers, country):
    pcodes = list()

    for sheetname in data:
        level = re.findall("\d", sheetname)[0]
        if level == "":
            logger.warning(f"{country}: Could not determine admin level")
        df = data[sheetname]
        codeheaders = [h for h in df.columns if bool(re.match(f".*{level}.*code?", h, re.IGNORECASE))]
        nameheaders = [h for h in df.columns if (bool(re.match("adm(in)?" + level + "(name)?_?([a-z]{2}$|name$)", h, re.IGNORECASE)) or
                                                 bool(re.match(f"name_?{level}", h, re.IGNORECASE))) and not
                       bool(re.search("alt", h, re.IGNORECASE))]

        if len(codeheaders) > 1:
            pcodeheaders = [c for c in codeheaders if "pcode" in c.lower()]
            if len(pcodeheaders) == 1:
                codeheaders = pcodeheaders

        if len(nameheaders) == 0:
            logger.error(f"{country}: Can't find name header at adm{level}")
            continue

        if len(nameheaders) > 1:
            ennameheaders = [n for n in nameheaders if n[-3:].lower() == "_en"]
            if len(ennameheaders) == 1:
                nameheaders = ennameheaders

        if len(nameheaders) > 1:
            logger.warning(f"{country}: Found multiple name columns at adm{level}, using first")
            nameheaders = [nameheaders[0]]

        if len(codeheaders) != 1:
            logger.error(f"{country}: Can't find code header at adm{level}")
            continue

        for _, row in df[codeheaders + nameheaders].iterrows():
            name = normalize("NFKD", str(row[1])).encode("ascii", "ignore").decode("ascii")
            pcode = {
                pcode_headers["country"]: country,
                pcode_headers["level"]: level,
                pcode_headers["p-code"]: row[0],
                pcode_headers["name"]: name,
            }
            if pcode not in pcodes:
                pcodes.append(pcode)

    return pcodes


def update_resource(dataset, file):
    for resource in dataset.get_resources():
        if resource.get_file_type() == "csv":
            resource.set_file_to_upload(file)
    dataset.update_in_hdx(
        hxl_update=False,
        updated_by_script="HDX Scraper: Global P-codes",
    )
    return