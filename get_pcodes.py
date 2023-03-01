import logging
import re
from pandas import read_excel
from unicodedata import normalize

logger = logging.getLogger(__name__)


def get_pcodes(dataset, retriever):
    pcodes = list()

    resources = [r for r in dataset.get_resources() if r.get_file_type() in ["xlsx", "xls"]]
    if len(resources) == 0:
        logger.error(f"Could not find gazetteer in {dataset['name']}")
        return None

    if len(resources) > 1:
        resources = [r for r in resources if "gazetteer" in r["description"].lower() or
                     "taxonomy" in r["description"].lower() or
                     bool(re.match(".*adm.*tabular.?data.*", r["name"], re.IGNORECASE))]

    if len(resources) == 0:
        logger.error(f"Could not find gazetteer in {dataset['name']}")
        return None

    filepath = retriever.download_file(resources[0]["url"])
    try:
        data = read_excel(filepath, sheet_name=None)
    except:
        logger.error(f"Could not read gazetteer for {dataset['name']}")
        return None
    sheetnames = [s for s in data if bool(re.match(".*adm(in)?.?[1-7].*", s, re.IGNORECASE))]

    if len(sheetnames) == 0:
        logger.error(f"Could not find correct tab in gazetteer for {dataset['name']}")
        return None

    for sheetname in sheetnames:
        level = re.findall("\d", sheetname)[0]
        if level == "":
            logger.warning(f"Could not determine admin level for {dataset['name']}")
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
            logger.error(f"Can't find name header for {dataset['name']} at adm{level}")
            continue

        if len(nameheaders) > 1:
            ennameheaders = [n for n in nameheaders if n[-3:].lower() == "_en"]
            if len(ennameheaders) == 1:
                nameheaders = ennameheaders

        if len(nameheaders) > 1:
            logger.warning(f"Found multiple name columns for {dataset['name']} at adm{level}, using first")
            nameheaders = [nameheaders[0]]

        if len(codeheaders) != 1:
            logger.error(f"Can't find code header for {dataset['name']} at adm{level}")
            continue

        for _, row in df[codeheaders + nameheaders].iterrows():
            name = normalize("NFKD", str(row[1])).encode("ascii", "ignore").decode("ascii")
            if [level, row[0], name] not in pcodes:
                pcodes.append([level, row[0], name])

    return pcodes
