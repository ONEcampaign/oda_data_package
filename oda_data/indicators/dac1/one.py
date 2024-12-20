from oda_data.indicators.indicator import Indicator


def total_oda_official() -> Indicator:
    return Indicator(
        code="ONE.10.1010_11010",
        name="Total ODA (Official Definition)",
        description=(
            "Official Development Assistance (ODA) according "
            "to the DAC definition for each year."
        ),
        sources=["DAC1"],
        type="ONE",
        filters={"aidtype_code": ("in", [1010, 11010])},
        operations=[
            {
                "query": (
                    "(aidtype_code == 1010 & year <2018) | "
                    "(aidtype_code == 11010 & year >= 2018)"
                )
            }
        ],
    )


def official_oda_gni() -> Indicator:
    return Indicator(
        code="ONE.40.1010_11010_1",
        name="Total ODA (official definition) as a percentage of GNI",
        description=(
            "Official Development Assistance (ODA) as a percentage of "
            "Gross National Income (GNI) for each year. Based on the official DAC "
            "definition for each year."
        ),
        sources=["DAC1"],
        filters={"aidtype_code": ("in", [1010, 11010, 1])},
        operations=[
            (
                "query",
                (
                    "(aidtype_code == 1010 & year <2018) | "
                    "(aidtype_code == 11010 & year >= 2018) | "
                    "(aidtype_code == 1)"
                ),
            ),
            ("pivot", {"columns": "aidtype_code", "values": "value"}),
            ("divide", {"divisor_column": 1, "columns": [1010, 11010]}),
            ("melt", {"value_vars": [1010, 11010], "exclude_columns": 1}),
        ],
    )


def oda_gni_flow() -> Indicator:
    return Indicator(
        code="ONE.40.1010_1",
        name="Total ODA (net flows) as a percentage of GNI",
        description=(
            "Official Development Assistance (ODA) net flows as a percentage of "
            "Gross National Income (GNI) for each year."
        ),
        sources=["DAC1"],
        filters={"aidtype_code": ("in", [1010, 1])},
        operations=[
            ("pivot", {"columns": "aidtype_code", "values": "value"}),
            ("divide", {"divisor_column": 1, "columns": [1010]}),
            ("melt", {"value_vars": 1010, "exclude_columns": 1}),
        ],
    )

