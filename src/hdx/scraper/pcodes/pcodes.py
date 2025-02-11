import logging
import re
from typing import Dict
from unicodedata import normalize

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset, HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add
from hdx.utilities.retriever import Retrieve
from pandas import Timestamp, isna, read_excel
from xlrd import xldate_as_datetime

logger = logging.getLogger(__name__)


class Pcodes:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_folder: str,
        error_handler: HDXErrorHandler,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_folder = temp_folder
        self._error_handler = error_handler
        self.pcodes = {}
        self.pcode_lengths = []

    def get_pcodes(self, iso: str) -> None:
        try:
            dataset = Dataset.read_from_hdx(f"cod-ab-{iso.lower()}")
        except HDXError:
            dataset = None

        if not dataset or not dataset.get("cod_level"):
            self._error_handler.add_message(
                "PCodes",
                iso,
                "Could not find dataset",
                message_type="warning",
            )
            return

        gazetteer = self.find_gazetteer(dataset, iso)
        if not gazetteer:
            return

        open_gazetteer = self.open_gazetteer(gazetteer, iso)

        self.get_pcodes_from_gazetteer(open_gazetteer, iso, dataset)

        missing_units = self._configuration["missing_units"].get(iso)
        if missing_units:
            for _, unit in missing_units.items():
                self.pcodes[iso].append(dict(unit))
        return

    def find_gazetteer(self, dataset, iso):
        exceptions = self._configuration["resource_exceptions"]
        if iso in exceptions:
            resources = [
                r for r in dataset.get_resources() if r["name"] == exceptions[iso]
            ]
        else:
            resources = [
                r for r in dataset.get_resources() if r.get_file_type() in ["xlsx", "xls"]
            ]

        if len(resources) == 0:
            self._error_handler.add_message(
                "PCodes",
                dataset["name"],
                "Could not find gazetteer",
            )
            return None

        if len(resources) > 1:
            resources = [
                r
                for r in resources
                if "gazetteer" in r["description"].lower()
                or "taxonomy" in r["description"].lower()
                or bool(re.match(".*adm.*tabular.?data.*", r["name"], re.IGNORECASE))
            ]

        if len(resources) == 0:
            self._error_handler.add_message(
                "PCodes",
                dataset["name"],
                "Could not find gazetteer",
            )
            return None

        return resources[0]

    def open_gazetteer(self, resource: Resource, iso: str) -> Dict:
        filepath = self._retriever.download_file(resource["url"])
        try:
            data = read_excel(filepath, sheet_name=None)
        except:
            self._error_handler.add(
                "Pcodes",
                f"cod-ab-{iso.lower()}",
                f"Could not open {resource['name']}",
            )
            return {}
        sheetnames = [
            s for s in data if bool(re.match(".*adm(in)?.?[1-7].*", s, re.IGNORECASE))
        ]

        if len(sheetnames) == 0:
            self._error_handler.add_message(
                "PCodes",
                f"cod-ab-{iso.lower()}",
                f"Could not find admin tabs in {resource['name']}",
            )
            return {}

        data_subset = {key: data[key] for key in data if key in sheetnames}
        return data_subset

    def get_pcodes_from_gazetteer(self, data, iso, dataset):
        dataset_date = dataset.get_reference_period(date_format="%Y-%m-%d")[
            "startdate_str"
        ]

        for sheetname in data:
            adm_pcodes = list()
            adm_duplicate_check = list()
            skip_level = False
            if iso == "BMU" and sheetname == "_Admin 2":
                continue
            if iso == "MSR" and "_pop" in sheetname:
                continue
            level = re.search("([^\d]\d[^\d])|([^\d]\d$)|(^\d[^\d])", sheetname)
            if not level:
                self._error_handler.add_message(
                    "PCodes",
                    dataset["name"],
                    f"Could not determine admin level for {sheetname}",
                )
                continue
            level = re.search("\d", level.group()).group()

            df = data[sheetname]
            codeheaders = [
                h
                for h in df.columns
                if bool(re.match(f".*{level}.*code?", h, re.IGNORECASE))
                and "unhcr" not in h.lower()
            ]
            nameheaders = [
                h
                for h in df.columns
                if (
                    bool(
                        re.match(
                            "adm(in)?" + level + "(name)?_?([a-z]{2}$|name$)",
                            h.strip(),
                            re.IGNORECASE,
                        )
                    )
                    or bool(re.match(f"name_?{level}", h, re.IGNORECASE))
                )
                and not bool(re.search("alt", h, re.IGNORECASE))
            ]
            if iso == "CMR":
                nameheaders = [f"ADM{level}_FR"]
            if iso == "EGY" and level == "3":
                nameheaders = ["ADM3_AR"]
            parentlevel = int(level) - 1
            if iso == "ARM" and level == "3":
                parentlevel = 1
            parentheaders = []
            if int(level) > 1:
                parentheaders = [
                    h
                    for h in df.columns
                    if bool(re.match(f".*{parentlevel}.*code?", h, re.IGNORECASE))
                    and "unhcr" not in h.lower()
                ]
            dateheaders = [h for h in df.columns if h.lower() == "validon"]

            if len(codeheaders) == 0:
                codeheaders = [
                    h for h in df.columns if bool(re.match(".*pcode?", h, re.IGNORECASE))
                ]
                if len(codeheaders) != 1:
                    self._error_handler.add_message(
                        "PCodes", dataset["name"], f"Can't find code header at adm{level}"
                    )
                    continue

            if len(codeheaders) > 1:
                pcodeheaders = [c for c in codeheaders if "pcode" in c.lower()]
                if len(pcodeheaders) >= 1:
                    codeheaders = [pcodeheaders[0]]
                else:
                    self._error_handler.add_message(
                        "PCodes",
                        dataset["name"],
                        f"Found multiple code columns at adm{level}, using first",
                        message_type="warning",
                    )
                    codeheaders = [codeheaders[0]]

            if len(nameheaders) == 0:
                self._error_handler.add_message(
                    "PCodes", dataset["name"], f"Can't find name header at adm{level}"
                )
                continue

            if len(nameheaders) > 1:
                ennameheaders = [n for n in nameheaders if n[-3:].lower() == "_en"]
                if len(ennameheaders) == 1:
                    nameheaders = ennameheaders
                else:
                    latin_nameheaders = [
                        n
                        for n in nameheaders
                        if n[-3] == "_"
                        and n[-2:].lower()
                        not in self._configuration["non_latin_alphabets"]
                    ]
                    if len(latin_nameheaders) > 0:
                        nameheaders = [latin_nameheaders[0]]
                    else:
                        self._error_handler.add_message(
                            "PCodes",
                            dataset["name"],
                            f"Found only non-latin alphabet name columns at adm{level}",
                            message_type="warning",
                        )
                        nameheaders = [nameheaders[0]]

            if len(parentheaders) == 0 and int(level) > 1:
                self._error_handler.add_message(
                    "PCodes",
                    dataset["name"],
                    f"Can't find parent code header at adm{level}",
                )

            if len(parentheaders) > 1 and int(level) > 1:
                self._error_handler.add_message(
                    "PCodes",
                    dataset["name"],
                    f"Found multiple parent code columns at adm{level}, using first",
                    message_type="warning",
                )
                parentheaders = [parentheaders[0]]

            if len(dateheaders) == 0:
                self._error_handler.add_message(
                    "PCodes",
                    dataset["name"],
                    f"Can't find date header at adm{level}, using dataset reference date",
                    message_type="warning",
                )

            for _, row in df[
                codeheaders + nameheaders + parentheaders + dateheaders
            ].iterrows():
                if "#" in str(row[codeheaders[0]]):
                    continue
                code = str(row[codeheaders[0]])
                if code in ["None", "", " ", "-"] or code.lower() == "not reported":
                    continue
                if iso == "ECU" and code in ["ECISLA", "ECNO APLICA"]:
                    continue
                if code in adm_duplicate_check:
                    skip_level = True
                else:
                    adm_duplicate_check.append(code)
                name = row[nameheaders[0]]
                if isna(name) or name == " ":
                    self._error_handler.add_missing_value_message(
                        "PCodes",
                        dataset["name"],
                        f"admin {level} name",
                        code,
                    )
                    name = None
                if name and not (iso == "EGY" and level == "3"):
                    name = (
                        normalize("NFKD", str(name))
                        .encode("ascii", "ignore")
                        .decode("ascii")
                    )
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

                row_parent = iso
                if len(parentheaders) == 1:
                    row_parent = str(row[parentheaders[0]])
                pcode = {
                    "Location": iso,
                    "Admin Level": level,
                    "P-Code": code,
                    "Name": name,
                    "Parent P-Code": row_parent,
                    "Valid from date": row_date,
                }
                if pcode not in adm_pcodes:
                    adm_pcodes.append(pcode)

            if skip_level:
                self._error_handler.add_message(
                    "PCodes", dataset["name"], f"Duplicate p-codes found at adm{level}"
                )
            else:
                for adm_pcode in adm_pcodes:
                    dict_of_lists_add(self.pcodes, iso, adm_pcode)
        return

    def check_parents(self, iso: str) -> None:
        if iso not in self.pcodes:
            return None
        all_pcodes = [pcode["P-Code"] for pcode in self.pcodes[iso]]
        parent_pcodes = set(
            [
                pcode["Parent P-Code"]
                for pcode in self.pcodes[iso]
                if int(pcode["Admin Level"]) > 1
            ]
        )
        for pcode in parent_pcodes:
            if pcode not in all_pcodes:
                self._error_handler.add_missing_value_message(
                    "PCodes",
                    f"cod-ab-{iso.lower()}",
                    "parent pcode",
                    pcode,
                )
        return

    def get_pcode_lengths(self, iso) -> None:
        if iso not in self.pcodes:
            return None
        pcode_lengths = {
            "Location": iso,
            "Country Length": None,
            "Admin 1 Length": set(),
            "Admin 2 Length": set(),
            "Admin 3 Length": set(),
            "Admin 4 Length": set(),
            "Admin 5 Length": set(),
        }
        country_code = None
        for row in self.pcodes[iso]:
            if not country_code:
                country_code = row["P-Code"][:3]
                country_info = Country.get_country_info_from_iso3(country_code)
                if not country_info:
                    country_code = row["P-Code"][:2]
                    country_info = Country.get_country_info_from_iso2(country_code)
                if not country_info:
                    country_code = ""
            if not pcode_lengths["Country Length"]:
                pcode_lengths["Country Length"] = len(country_code)

            field = f"Admin {row['Admin Level']} Length"
            pcode = row["P-Code"]
            parent_pcode = row["Parent P-Code"]
            if row["Admin Level"] == "1":
                parent_pcode = country_code

            field_length = len(str(pcode)) - len(str(parent_pcode))
            dict_of_sets_add(pcode_lengths, field, str(field_length))

        for admin_level in range(1, 6):
            field_lengths = pcode_lengths[f"Admin {admin_level} Length"]
            if len(field_lengths) == 0:
                field_lengths = None
            else:
                field_lengths = "|".join(field_lengths)
            pcode_lengths[f"Admin {admin_level} Length"] = field_lengths

        self.pcode_lengths.append(pcode_lengths)
        return

    def generate_dataset(self) -> Dataset:
        global_pcodes = []
        for _, rows in self.pcodes.items():
            for row in rows:
                global_pcodes.append(row)

        global_pcodes = sorted(
            global_pcodes,
            key=lambda k: (
                k["Location"],
                k["Admin Level"],
                k["P-Code"],
            ),
        )

        adm12_pcodes = [g for g in global_pcodes if g["Admin Level"] in ["1", "2"]]

        dataset = Dataset(
            {
                "name": self._configuration["dataset_name"],
                "title": self._configuration["dataset_title"],
            }
        )
        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])

        min_date = min([entry["Valid from date"] for entry in global_pcodes])
        dataset.set_time_period(startdate=min_date, ongoing=True)

        hxl_tags = self._configuration["hxl_tags"]
        dataset.generate_resource_from_iterable(
            headers=list(hxl_tags.keys()),
            iterable=global_pcodes,
            hxltags=hxl_tags,
            folder=self._temp_folder,
            filename=self._configuration["resource_info_all"]["name"],
            resourcedata=self._configuration["resource_info_all"],
            encoding="utf-8-sig",
        )
        dataset.generate_resource_from_iterable(
            headers=list(hxl_tags.keys()),
            iterable=adm12_pcodes,
            hxltags=hxl_tags,
            folder=self._temp_folder,
            filename=self._configuration["resource_info_1_2"]["name"],
            resourcedata=self._configuration["resource_info_1_2"],
            encoding="utf-8-sig",
        )
        hxl_tags = self._configuration["hxl_tags_lengths"]
        dataset.generate_resource_from_iterable(
            headers=list(hxl_tags.keys()),
            iterable=self.pcode_lengths,
            hxltags=hxl_tags,
            folder=self._temp_folder,
            filename=self._configuration["resource_info_lengths"]["name"],
            resourcedata=self._configuration["resource_info_lengths"],
            encoding="utf-8-sig",
        )

        return dataset
