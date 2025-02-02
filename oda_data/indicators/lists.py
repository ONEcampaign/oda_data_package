from oda_data.api.classes import load_indicators


def crs_totals_by_purpose(perspective: str = "P") -> list:
    indicators = load_indicators()

    indicators = {
        i for i in indicators if "CRS" in i and perspective in i and ".T.T" in i
    }
