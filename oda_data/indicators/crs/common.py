import json

import numpy as np
import pandas as pd

from oda_data.config import OdaPATHS


def generate_crs_type_of_flow_mapping(crs: pd.DataFrame) -> None:
    """Create json mapping categories to category names (which represent types of flow)"""

    with open(OdaPATHS.settings / "flow_types.json", "r") as f:
        flow_types = json.load(f)

    crs = crs.filter(["category"]).drop_duplicates()
    crs["category_name"] = crs.category.map({int(k): v for k, v in flow_types.items()})

    crs = crs.set_index("category")["category_name"].to_dict()

    with open(OdaPATHS.settings / "crs_flow_types.json", "w") as f:
        json.dump(crs, f, indent=2)


def read_crs_type_of_flow() -> dict:
    with open(OdaPATHS.settings / "crs_flow_types.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v for k, v in mapping.items()}


def generate_crs_type_of_finance_mapping(crs: pd.DataFrame) -> None:
    crs = crs.filter(["type_of_finance"]).drop_duplicates()

    broad_mapping = {
        0: "Non flows",
        100: "Grants",
        420: "Debt instruments",
        430: "Mezzanine finance instruments",
        500: "Equity and shares in collective investment vehicles",
        600: "Debt relief",
        1000: "Guarantees and other unfunded contingent liabilities",
    }

    crs["grouped"] = np.where(
        crs.type_of_finance.between(100, 399),
        100,
        np.where(
            crs.type_of_finance.between(420, 429),
            420,
            np.where(
                crs.type_of_finance.between(430, 439),
                430,
                np.where(
                    crs.type_of_finance.between(500, 599),
                    500,
                    np.where(
                        crs.type_of_finance.between(600, 699),
                        600,
                        np.where(crs.type_of_finance > 1000, 1000, crs.type_of_finance),
                    ),
                ),
            ),
        ),
    )

    crs = crs.set_index("type_of_finance")["grouped"]

    mapping = {}

    for type_, group in crs.items():
        mapping[type_] = {"code": group, "name": broad_mapping[group]}

    with open(OdaPATHS.settings / "crs_type_of_finance.json", "w") as f:
        json.dump(mapping, f, indent=2)


def read_crs_type_of_finance_mapping() -> dict:
    with open(OdaPATHS.settings / "crs_type_of_finance.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v for k, v in mapping.items()}


def generate_crs_modality_mapping(crs: pd.DataFrame) -> None:
    crs = crs.filter(["modality"]).fillna("X").drop_duplicates()
    crs["simple_modality"] = crs["modality"].str[0]

    modalities = crs.set_index("modality")["simple_modality"].to_dict()

    names = {
        "A": "Budget support",
        "B": "Core contributions and pooled programmes and funds",
        "C": "Project-type interventions",
        "D": "Experts and other technical assistance",
        "E": "Scholarships and student costs in donor countries",
        "F": "Debt relief",
        "G": "Administrative costs not included elsewhere",
        "H": "Other in-donor expenditures",
        "X": "Modality not specified",
    }

    mapping = {}

    for modality, group in modalities.items():
        mapping[modality] = {"code": group, "name": names[group]}

    with open(OdaPATHS.settings / "crs_modalities.json", "w") as f:
        json.dump(mapping, f, indent=2)


def read_crs_modality_mapping() -> dict:
    with open(OdaPATHS.settings / "crs_modalities.json", "r") as file:
        mapping = json.load(file)

    return mapping


def generate_crs_purpose_mapping(crs: pd.DataFrame) -> None:
    sectors = {
        100: "Social infrastructure",
        110: "Education",
        120: "Health",
        130: "Population Policies/Programmes & Reproductive Health",
        140: "Water Supply & Sanitation",
        150: "Government & Civil Society",
        160: "Other Social Infrastructure & Services",
        200: "Economic infrastructure",
        210: "Transport & Storage",
        220: "Communications",
        230: "Energy",
        240: "Banking & Financial Services",
        250: "Business & Other Services",
        310: "Agriculture, Forestry, Fishing",
        320: "Industry, Mining, Construction",
        330: "Trade Policies & Regulations",
        410: "General Environment Protection",
        430: "Other Multisector",
        510: "General Budget Support",
        520: "Development Food Assistance",
        530: "Other Commodity Assistance",
        600: "Action Relating to Debt",
        720: "Emergency Response",
        730: "Reconstruction Relief & Rehabilitation",
        740: "Disaster Prevention & Preparedness",
        910: "Administrative Costs of Donors",
        930: "Refugees in Donor Countries",
        998: "Unallocated / Unspecified",
    }

    crs = crs.filter(["sector_code"]).fillna(998).drop_duplicates()
    detailed = crs.sector_code.to_list()

    groups = {}

    for sector in sectors.keys():
        groups[sector] = [s for s in detailed if (s >= sector) and (s < sector + 10)]

    mapping = {}
    for group, codes in groups.items():
        for code in codes:
            mapping[code] = {"code": group, "name": sectors[group]}

    with open(OdaPATHS.settings / "crs_purpose.json", "w") as f:
        json.dump(mapping, f, indent=2)


def read_crs_purpose_mapping() -> dict:
    with open(OdaPATHS.settings / "crs_purpose.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v for k, v in mapping.items()}


def generate_crs_policy_markers() -> None:
    markers = {
        "gender": "GEN",
        "environment": "ENV",
        "dig_code": "DIG",
        "trade": "TRD",
        "rmnch_code": "RMNCH",
        "drr_code": "DRR",
        "nutrition": "NUT",
        "disability": "DIS",
        "biodiversity": "BIO",
        "climate_mitigation": "CM",
        "climate_adaptation": "CA",
        "desertification": "DES",
    }

    with open(OdaPATHS.settings / "crs_policy_markers.json", "w") as f:
        json.dump(markers, f, indent=2)


def read_crs_policy_markers() -> dict:
    with open(OdaPATHS.settings / "crs_policy_markers.json", "r") as file:
        mapping = json.load(file)

    return mapping
