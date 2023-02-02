import logging
import re
from pandas import read_excel

logger = logging.getLogger(__name__)


def get_pcodes(dataset, retriever):
    pcodes = set()

    resources = [r for r in dataset.get_resources() if r.get_file_type() in ["xlsx", "xls"]]
    if len(resources) == 0:
        logger.error(f"Could not find gazetteer in {dataset['name']}")
        return None

    if len(resources) > 1:
        resources = [r for r in resources if "gazetteer" in r["description"].lower()]

    if len(resources) == 0:
        logger.error(f"Could not find gazetteer in {dataset['name']}")
        return None

    filepath = retriever.download_file(resources[0]["url"])
    data = read_excel(filepath, sheet_name=None)
    sheetnames = [s for s in data if bool(re.match(".*adm.*[1-7].*", s, re.IGNORECASE))]

    if len(sheetnames) == 0:
        logger.error(f"Could not find correct tab in gazetteer for {dataset['name']}")
        return None

    sheetnames.sort()
    sheetname = sheetnames[-1]

    df = data[sheetname]
    headers = [h for h in df.columns if bool(re.match(".*[1-7].*code?", h, re.IGNORECASE))]
    for header in headers:
        for pc in df[header]:
            pcodes.add(pc)

    pcodes = list(pcodes)

    return pcodes
