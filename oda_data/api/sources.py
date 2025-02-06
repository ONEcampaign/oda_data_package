from abc import ABC


class DacSource(ABC):
    """Class to model access to a DAC dataset. It handles validation of the parameters
    and manages returning a dataframe with the requested data."""
