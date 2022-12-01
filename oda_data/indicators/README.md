# Indicators

This module contains configuration files and scripts to produce a series of frequently-used indicators.

## [indicators.json](../settings/indicators.json)
This json file contains settings used to filter the different DAC databases and provide a dataframe with a 
specific indicator.

Regular users should not need to modify this file. The following explanation documents how indicators are built and
provides an idea of how new indicators could be constructed.

Generally, there are two types of indicators:
- Indicators which are just a filtered version of DAC data
- Indicators which involve additional aggregation/filtering/combining, based on ONE's methodologies.

The first type of indicators are defined by a json dictionary of the following type:
```json5
{
  "indicator_name": {
    "source": "name of the source database",
    "type": "dac or one",
    "filters": {
      "column_name": "value",
      "column_name2": 1000
    }
  }
}
```
For example, for total ODA expressed as net flows in current USD.
```json5
{
  "total_oda_flow_net": {
    "source": "dac1",
    "type": "dac",
    "filters": {
      "aidtype_code": 1010,
      "flows_code": 1140,
      "amounttype_code": "A"
    }
  }
}
```
For an indicator which requires additional aggregation, following ONE's methodologies, the following example
combines two indicators from table `dac2a` and produces totals for donors/recipients.

```json5
{
  "recipient_total_flow_net": {
    "source": "dac2a",
    "type": "one",
    "indicators": [
      "recipient_imputed_multi_flow_net",
      "recipient_bilateral_flow_net"
    ],
    "group_by": [
      "donor_code",
      "donor",
      "recipient_code",
      "recipient",
      "year",
      "indicator"
    ]
  },
}
```