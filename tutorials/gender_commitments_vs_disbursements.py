from oda_data import ODAData, set_data_path
import pandas as pd

from oda_data.tools.groupings import donor_groupings

# If you haven't set the data path, you can do it now.
set_data_path(path="../tutorials/data")


def total_gender_oda(oda: ODAData) -> pd.DataFrame:

    indicators = [
        "crs_gender_total_flow_commitment_gross",
        "crs_gender_total_flow_disbursement_gross",
    ]

    for indicator in indicators:
        oda.load_indicator(indicator)

    return oda.get_data()


def annual_commitments_vs_disbursements_by_donor(df: pd.DataFrame) -> pd.DataFrame:

    groupby = ["donor_name", "year", "currency", "prices", "indicator"]

    return (
        df.groupby(groupby, observed=True, dropna=False)["value"]
        .sum(numeric_only=True)
        .reset_index(drop=False)
    )


# def calculate_2013_2022_commitments_vs_disbursements_by_donor(df: pd.DataFrame) -> pd.DataFrame:
#
#     groupby = ["donor_name", "currency", "prices", "indicator"]
#
#     df = df.groupby(groupby, observed=True, dropna=False)["value"].sum(numeric_only=True).reset_index(drop=False)
#
#     df_total = groupby()
#
#     return


def calculate_total_commitments_disbursements(
    df: pd.DataFrame,
) -> pd.DataFrame:

    groupby = ["donor_name", "currency", "prices", "indicator"]

    return df.groupby(groupby, observed=True, dropna=False)["value"].sum().reset_index()


def shape_for_observable(df: pd.DataFrame) -> pd.DataFrame:

    index = ["donor_name", "currency", "prices"]

    return df.pivot(index=index, columns="indicator", values="value").reset_index()


def add_disbursements_as_share_of_commitments_column(df: pd.DataFrame) -> pd.DataFrame:

    df["disursement_as_share_commitment"] = (
        df["crs_gender_total_flow_disbursement_gross"]
        / df["crs_gender_total_flow_commitment_gross"]
    )

    return df


def add_dac_total_row(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    sums = df_copy[
        [
            "crs_gender_total_flow_commitment_gross",
            "crs_gender_total_flow_disbursement_gross",
        ]
    ].sum()

    df_total = pd.DataFrame(
        [
            {
                "donor_name": "dac_members",
                "currency": df_copy["currency"].iloc[0],
                "prices": df_copy["prices"].iloc[0],
                "crs_gender_total_flow_commitment_gross": sums[
                    "crs_gender_total_flow_commitment_gross"
                ],
                "crs_gender_total_flow_disbursement_gross": sums[
                    "crs_gender_total_flow_disbursement_gross"
                ],
            }
        ]
    )

    # Concatenate the original DataFrame with the new total row
    return pd.concat([df_copy, df_total], ignore_index=True)


if __name__ == "__main__":

    oda = ODAData(
        years=range(2013, 2023),
        donors=list(donor_groupings()["dac_members"]),
        recipients=None,
        currency="USD",
        prices="current",
        base_year=None,
        include_names=True,
    )

    total_by_country = (
        total_gender_oda(oda=oda)
        .pipe(annual_commitments_vs_disbursements_by_donor)
        .pipe(calculate_total_commitments_disbursements)
        .pipe(shape_for_observable)
    )

    total_dac_difference_2013_2022 = add_dac_total_row(total_by_country)

    total_by_country.to_csv(r"../tutorials/output/total_by_country.csv", index=False)
    total_dac_difference_2013_2022.to_csv(
        r"../tutorials/output/total_dac_diffeence_2013_2022.csv", index=False
    )
