from oda_data.config import ODAPaths
from oda_data.indicators.common import update_mapping_file
from oda_data.indicators.indicator import Indicator


def multilateral_spending_by_purpose_shares() -> Indicator:
    return Indicator(
        code="ONE.P.40.T.T.S_M",
        name=(
            "Purpose shares (3-year rolling total) out of total Official Development Assistance (ODA)"
            " and Other Official Flows (OOF), for multilateral organisations."
        ),
        description=(
            "Shares for each purpose code out of total Official Development Assistance (ODA)"
            " and Other Official Flows (OOF). Data for multilateral organisations."
        ),
        sources=["CRS"],
        type="ONE",
        filters={
            "CRS": {
                "donor_code": (
                    "in",
                    [
                        "104",
                        "807",
                        "811",
                        "812",
                        "901",
                        "902",
                        "903",
                        "905",
                        "906",
                        "907",
                        "909",
                        "910",
                        "913",
                        "914",
                        "915",
                        "918",
                        "921",
                        "923",
                        "926",
                        "928",
                        "932",
                        "940",
                        "944",
                        "948",
                        "951",
                        "952",
                        "953",
                        "954",
                        "956",
                        "958",
                        "959",
                        "960",
                        "962",
                        "963",
                        "964",
                        "966",
                        "967",
                        "971",
                        "974",
                        "976",
                        "978",
                        "979",
                        "980",
                        "981",
                        "982",
                        "983",
                        "988",
                        "990",
                        "992",
                        "997",
                        "1011",
                        "1012",
                        "1013",
                        "1014",
                        "1015",
                        "1016",
                        "1017",
                        "1018",
                        "1019",
                        "1020",
                        "1023",
                        "1024",
                        "1025",
                        "1037",
                        "1038",
                        "1039",
                        "1045",
                        "1046",
                        "1047",
                        "1048",
                        "1049",
                        "1050",
                        "1052",
                        "1053",
                        "1054",
                        "1055",
                        "1057",
                        "1058",
                        "1311",
                        "1312",
                        "1313",
                        "1401",
                        "1404",
                        "1406",
                    ],
                )
            }
        },
        custom_function="multilateral_purpose_spending_shares",
    )


def crs_one_indicators():
    """
    Generate a dictionary defining the CRS indicator codes and their filtering logic.

    Returns:
        dict[str, dict]: Dictionary of indicator codes and their serialized definitions.
    """

    indicators = [multilateral_spending_by_purpose_shares()]

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    one_crs_indicators = crs_one_indicators()
    update_mapping_file(
        {"ONE": one_crs_indicators},
        file_path=ODAPaths.indicators / "crs" / "crs_indicators.json",
    )
