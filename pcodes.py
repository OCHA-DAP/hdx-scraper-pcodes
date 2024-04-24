import logging
import re

from pandas import isna, read_excel, Timestamp
from unicodedata import normalize
from xlrd import xldate_as_datetime

from hdx.location.country import Country
from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)


def get_global_pcodes(dataset, resource_name, retriever):
    resource = [r for r in dataset.get_resources() if r["name"] == resource_name]
    data_headers = ["Location", "Admin Level", "P-Code", "Name", "Parent P-Code", "Valid from date"]
    hxl_headers = ["#country+code", "#geo+admin_level", "#adm+code", "#adm+name", "#adm+code+parent", "#date+start"]

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


def get_pcodes_from_gazetteer(data, non_latin_langs, country, dataset):
    pcodes = list()
    dataset_date = dataset.get_reference_period(date_format="%Y-%m-%d")["startdate_str"]

    for sheetname in data:
        level = re.search("([^\d]\d[^\d])|([^\d]\d$)|(^\d[^\d])", sheetname)
        if not level:
            logger.warning(f"{country}: Could not determine admin level for {sheetname}")
            continue
        level = re.search("\d", level.group()).group()

        df = data[sheetname]
        codeheaders = [h for h in df.columns if bool(re.match(f".*{level}.*code?", h, re.IGNORECASE)) and
                       "unhcr" not in h.lower()]
        nameheaders = [
            h for h in df.columns if
            (bool(re.match("adm(in)?" + level + "(name)?_?([a-z]{2}$|name$)", h, re.IGNORECASE)) or
             bool(re.match(f"name_?{level}", h, re.IGNORECASE))) and not
            bool(re.search("alt", h, re.IGNORECASE))
        ]
        if country == "CMR":
            nameheaders = [f"ADM{level}_FR"]
        parentlevel = int(level) - 1
        if country == "ARM" and level == "3":
            parentlevel = 1
        parentheaders = []
        if int(level) > 1:
            parentheaders = [h for h in df.columns if bool(re.match(f".*{parentlevel}.*code?", h, re.IGNORECASE))
                             and "unhcr" not in h.lower()]
        dateheaders = [h for h in df.columns if h.lower() == "validon"]

        if len(codeheaders) == 0:
            codeheaders = [h for h in df.columns if bool(re.match(".*pcode?", h, re.IGNORECASE))]
            if len(codeheaders) != 1:
                logger.error(f"{country}: Can't find code header at adm{level}")
                continue

        if len(codeheaders) > 1:
            pcodeheaders = [c for c in codeheaders if "pcode" in c.lower()]
            if len(pcodeheaders) >= 1:
                codeheaders = [pcodeheaders[0]]
            else:
                logger.warning(f"{country}: Found multiple code columns at adm{level}, using first")
                codeheaders = [codeheaders[0]]

        if len(nameheaders) == 0:
            logger.error(f"{country}: Can't find name header at adm{level}")
            continue

        if len(nameheaders) > 1:
            ennameheaders = [n for n in nameheaders if n[-3:].lower() == "_en"]
            if len(ennameheaders) == 1:
                nameheaders = ennameheaders
            else:
                latin_nameheaders = [n for n in nameheaders if n[-3] == "_" and n[-2:].lower() not in non_latin_langs]
                if len(latin_nameheaders) > 0:
                    nameheaders = [latin_nameheaders[0]]
                else:
                    logger.warning(f"{country}: Found only non-latin alphabet name columns at adm{level}")
                    nameheaders = [nameheaders[0]]

        if len(parentheaders) == 0 and int(level) > 1:
            logger.warning(f"{country}: Can't find parent code header at adm{level}")

        if len(parentheaders) > 1 and int(level) > 1:
            logger.warning(f"{country}: Found multiple parent code columns at adm{level}, using first")
            parentheaders = [parentheaders[0]]

        if len(dateheaders) == 0:
            logger.warning(f"{country}: Can't find date header at adm{level}, using dataset reference date")

        for _, row in df[codeheaders + nameheaders + parentheaders + dateheaders].iterrows():
            if "#" in str(row[codeheaders[0]]):
                continue
            code = str(row[codeheaders[0]])
            if code in ["None", "", " ", "-"] or code.lower() == "not reported":
                continue
            name = row[nameheaders[0]]
            if isna(name):
                continue
            name = normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
            name = name.strip()
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
                "Location": country,
                "Admin Level": level,
                "P-Code": code,
                "Name": name,
                "Parent P-Code": row_parent,
                "Valid from date": row_date,
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

    if not dataset.get("cod_level"):
        return pcodes

    gazetteer = find_gazetteer(dataset, country, configuration["resource_exceptions"])
    if not gazetteer:
        return pcodes

    open_gazetteer = get_data(gazetteer, retriever, country)

    pcodes = get_pcodes_from_gazetteer(
        open_gazetteer,
        configuration["non_latin_alphabets"],
        country,
        dataset,
    )

    missing_units = configuration["missing_units"].get(country)
    if missing_units:
        for unit in missing_units:
            pcodes.append(dict(missing_units[unit]))
    return pcodes


def check_parents(pcodes):
    missing_units = []
    all_pcodes = [pcode["P-Code"] for pcode in pcodes]
    parent_pcodes = set([pcode["Parent P-Code"] for pcode in pcodes if int(pcode["Admin Level"]) > 1])
    for pcode in parent_pcodes:
        if pcode not in all_pcodes:
            missing_units.append(pcode)
    return missing_units


def get_pcode_lengths(global_pcodes):
    pcode_lengths = {}
    for row in global_pcodes:
        country = row["Location"]
        if "#" in country:
            continue
        if country not in pcode_lengths:
            country_code = row["P-Code"][:3]
            country_info = Country.get_country_info_from_iso3(country_code)
            if not country_info:
                country_code = row["P-Code"][:2]
                country_info = Country.get_country_info_from_iso2(country_code)
            if not country_info:
                country_code = ""
            country_length = len(country_code)
            pcode_lengths[country] = {
                "Location": row["Location"],
                "Country Length": str(country_length),
                "Admin 1 Length": None,
                "Admin 2 Length": None,
                "Admin 3 Length": None,
                "Admin 4 Length": None,
                "Admin 5 Length": None,
            }
        else:
            country_length = int(pcode_lengths[country]["Country Length"])

        field = f"Admin {row['Admin Level']} Length"
        if row["Admin Level"] == "1":
            field_length = str(len(str(row["P-Code"])) - country_length)
        else:
            field_length = str(len(str(row["P-Code"])) - len(str(row["Parent P-Code"])))
        stored_lengths = pcode_lengths[country][field]

        if not stored_lengths:
            pcode_lengths[country][field] = str(field_length)
            continue
        stored_lengths = stored_lengths.split("|")
        if field_length in stored_lengths:
            continue
        stored_lengths.append(field_length)
        all_lengths = "|".join(stored_lengths)
        pcode_lengths[country][field] = all_lengths

    pcode_length_list = [
        {
            "Location": "#country+code",
            "Country Length": "#country+len",
            "Admin 1 Length": "#adm1+len",
            "Admin 2 Length": "#adm2+len",
            "Admin 3 Length": "#adm3+len",
            "Admin 4 Length": "#adm4+len",
            "Admin 5 Length": "#adm5+len",
        }
    ]
    for country in pcode_lengths:
        pcode_length_list.append(pcode_lengths[country])

    return pcode_length_list
