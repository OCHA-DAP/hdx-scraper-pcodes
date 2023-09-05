import logging
import re

from pandas import read_excel, Timestamp
from unicodedata import normalize
from xlrd import xldate_as_datetime

from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)


def get_global_pcodes(dataset, dataset_info, retriever):
    resource = [r for r in dataset.get_resources() if r["name"] == dataset_info["resource_name"]["all"]]

    data_headers = [val for val in dataset_info["headers"].values()]
    hxl_headers = [val for val in dataset_info["headers_hxl"].values()]
    headers, iterator = retriever.get_tabular_rows(resource[0]["url"], dict_form=True)

    pcodes = list()
    hxl_row = {data_headers[i]: hxl_headers[i] for i in range(len(data_headers))}
    pcodes.append(hxl_row)
    for row in iterator:
        if row[headers[0]][0] == "#":
            continue
        pcode = {data_header: row.get(data_header) for data_header in data_headers}
        pcodes.append(pcode)
    return pcodes


def find_gazetteer(dataset, country, exceptions):
    if country in exceptions:
        resources = [r for r in dataset.get_resources() if r["name"] == exceptions[country]]
    else:
        resources = [r for r in dataset.get_resources() if r.get_file_type() in ["xlsx", "xls"]]

    if len(resources) == 0:
        logger.error(f"{country}: Could not find gazetteer in {dataset['name']}")
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
        logger.error(f"{country}: Could not read {resource['name']}")
        return dict()
    sheetnames = [s for s in data if bool(re.match(".*adm(in)?.?[1-7].*", s, re.IGNORECASE))]

    if len(sheetnames) == 0:
        logger.error(f"{country}: Could not find correct tab in {resource['name']}")
        return dict()

    data_subset = {key: data[key] for key in data if key in sheetnames}
    return data_subset


def get_pcodes_from_gazetteer(data, pcode_headers, country, dataset):
    pcodes = list()
    dataset_date = dataset.get_reference_period(date_format="%Y-%m-%d")["startdate_str"]

    for sheetname in data:
        level = re.search("([^\d]\d[^\d])|([^\d]\d$)|(^\d[^\d])", sheetname)
        if not level:
            logger.warning(f"{country}: Could not determine admin level for {sheetname}")
            continue
        level = re.search("\d", level.group()).group()

        df = data[sheetname]
        codeheaders = [h for h in df.columns if bool(re.match(f".*{level}.*code?", h, re.IGNORECASE))]
        nameheaders = [
            h for h in df.columns if
            (bool(re.match("adm(in)?" + level + "(name)?_?([a-z]{2}$|name$)", h, re.IGNORECASE)) or
             bool(re.match(f"name_?{level}", h, re.IGNORECASE))) and not
            bool(re.search("alt", h, re.IGNORECASE))
        ]
        parentlevel = int(level) - 1
        if country == "ARM" and level == "3":
            parentlevel = 1
        parentheaders = []
        if int(level) > 1:
            parentheaders = [h for h in df.columns if bool(re.match(f".*{parentlevel}.*code?", h, re.IGNORECASE))]
        dateheaders = [h for h in df.columns if h.lower() == "validon"]

        if len(codeheaders) == 0:
            codeheaders = [h for h in df.columns if bool(re.match(".*pcode?", h, re.IGNORECASE))]
            if len(codeheaders) != 1:
                logger.error(f"{country}: Can't find code header at adm{level}")
                continue

        if len(codeheaders) == 0:
            logger.error(f"{country}: Can't find code header at adm{level}")
            continue

        if len(nameheaders) == 0:
            logger.error(f"{country}: Can't find name header at adm{level}")
            continue

        if len(parentheaders) == 0 and int(level) > 1:
            logger.warning(f"{country}: Can't find parent code header at adm{level}")

        if len(dateheaders) == 0:
            logger.warning(f"{country}: Can't find date header at adm{level}, using dataset reference date")

        if len(codeheaders) > 1:
            pcodeheaders = [c for c in codeheaders if "pcode" in c.lower()]
            if len(pcodeheaders) >= 1:
                codeheaders = pcodeheaders

        if len(nameheaders) > 1:
            ennameheaders = [n for n in nameheaders if n[-3:].lower() == "_en"]
            if len(ennameheaders) == 1:
                nameheaders = ennameheaders

        if len(nameheaders) > 1:
            logger.warning(f"{country}: Found multiple name columns at adm{level}, using first")
            nameheaders = [nameheaders[0]]

        if len(codeheaders) > 1:
            logger.warning(f"{country}: Found multiple code columns at adm{level}, using first")
            codeheaders = [codeheaders[0]]

        if len(parentheaders) > 1 and int(level) > 1:
            logger.warning(f"{country}: Found multiple parent code columns at adm{level}, using first")
            parentheaders = [parentheaders[0]]

        for _, row in df[codeheaders + nameheaders + parentheaders + dateheaders].iterrows():
            if "#" in str(row[codeheaders[0]]):
                continue
            code = str(row[codeheaders[0]])
            if code == "None" or code == "" or code.lower() == "not reported":
                continue
            name = normalize("NFKD", str(row[nameheaders[0]])).encode("ascii", "ignore").decode("ascii")
            if name.islower() or name.isupper():
                name = name.title()
            row_date = ""
            if len(dateheaders) == 1:
                row_date = row[dateheaders[0]]
                if type(row_date) is Timestamp:
                    row_date = row_date.strftime("%Y-%m-%d")
                elif type(row_date) is int:
                    row_date = xldate_as_datetime(row_date, 0)
                    row_date = row_date.strftime("%Y-%m-%d")
            if len(dateheaders) == 0:
                row_date = dataset_date

            row_parent = country
            if len(parentheaders) == 1:
                row_parent = row[parentheaders[0]]
            pcode = {
                pcode_headers["country"]: country,
                pcode_headers["level"]: level,
                pcode_headers["p-code"]: code,
                pcode_headers["name"]: name,
                pcode_headers["parent"]: row_parent,
                pcode_headers["date"]: row_date,
            }
            if pcode not in pcodes:
                pcodes.append(pcode)

    return pcodes


def get_pcodes(retriever, country, configuration):
    pcodes = list()
    dataset = Dataset.read_from_hdx(f"cod-ab-{country.lower()}")
    if not dataset:
        logger.warning(f"{country}: Could not find boundary dataset")
        return pcodes

    gazetteer = find_gazetteer(dataset, country, configuration["resource_exceptions"])
    if not gazetteer:
        return pcodes

    open_gazetteer = get_data(gazetteer, retriever, country)
    if len(open_gazetteer) == 0:
        return pcodes

    pcodes = get_pcodes_from_gazetteer(open_gazetteer, configuration["headers"], country, dataset)
    return pcodes
