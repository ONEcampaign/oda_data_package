# Settings

## [indicators.json](../settings/indicators.json)

This json file contains settings used to filter the different DAC databases and provide a dataframe with a
specific indicator.

Regular users should not need to modify this file. The following explanation documents how indicators are built and
provides an idea of how new indicators could be constructed.

There are currently four types of indicators:

1. Indicators which are just a filtered version of DAC data
2. Indicators which are a combination of DAC indicators, put together to fill reporting gaps
3. Indicators which involve additional aggregation/filtering/combining, based on ONE's methodologies.
4. Indicators which involve additional steps to produce, based on ONE's methodologies (including imputations,etc.)

### 1. The first type of indicators are defined by a json dictionary of the following type:

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

### 2. Indicators which are a combination of DAC indicators, put together to fill reporting gaps

In certain cases, data is only reported as Grant Equivalents or as Flows, even though the two numbers are equivalent.
Flows
and grant equivalents are equal in all cases but for loans.

To avoid having gaps, we put together "linked" indicators. The use either the "ge" or "flow" version as the primary
indicator,
and fill any gaps with a secondary indicator (the equivalent "flow" or "ge" as may be required). For example:

```json5
{
  "total_in_donor_students_ge_linked": {
    "source": "dac1",
    "type": "one_linked",
    "components": {
      "main": "total_in_donor_students_ge",
      "secondary": "total_in_donor_students_flow",
      "new": "total_in_donor_students_ge_linked"
    }
  }
}
```

### 3. For an indicator which requires additional aggregation, following ONE's methodologies.

The following example combines two indicators from table `dac2a` and produces totals for donors/recipients:

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
      "aidtype_code"
    ]
  },
}
```

### 4. For an indicator which requires additional steps to produce, following ONE's methodologies (including imputations,etc.)

```json5
{
  "imputed_multi_flow_disbursement_gross": {
    "source": "multisystem",
    "type": "one_research",
    "function": "multilateral_imputed_flows"
  }
}
```

## [crs_config.json](../settings/crs_config.json)

This file stores the basic settings used to clean and save the raw CRS files. It contains column names, with their type
and
whether they should be kept when a 'small' version of the file is saved

```json5
{
  "year": {
    "type": "Int16",
    "keep": true
  },
  "donor_code": {
    "type": "Int16",
    "keep": true
  }
}
```

## [dac1_config.json](../settings/dac1_config.json)

This file stores the basic settings used to clean and save the raw DAC1 file. It contains column names, with their type
and
whether they should be kept when a 'small' version of the file is saved

```json5
{
  "donor_code": {
    "type": "Int32",
    "keep": true
  },
  "donor": {
    "type": "category",
    "keep": true
  }
}
```

## [dac2a_config.json](../settings/dac2a_config.json)

This file stores the basic settings used to clean and save the raw dac2a file. It contains column names, with their type
and
whether they should be kept when a 'small' version of the file is saved

```json5
{
  "recipient_code": {
    "type": "Int32",
    "keep": true
  },
  "recipient": {
    "type": "category",
    "keep": true
  }
}
```

## [multisystem_config.json](../settings/multisystem.json)

This file stores the basic settings used to clean and save the raw multisystem file. It contains column names, with
their type and
whether they should be kept when a 'small' version of the file is saved

```json5
{
  "year": {
    "type": "Int16",
    "keep": true
  },
  "donor_code": {
    "type": "Int32",
    "keep": true
  }
}
```

## [donor_groupings.json](../settings/donor_groupings.json)

This file stores lists of donors that belong to specific groupings. The keys are the donor codes, the values the names.
For example:

```json5
{
  "g7": {
    "4": "France",
    "5": "Germany",
    "6": "Italy",
    "12": "United Kingdom",
    "301": "Canada",
    "302": "United States",
    "701": "Japan"
  }
}
```

## [recipient_groupings.json](../settings/recipient_groupings.json)

This file stores lists of recipients that belong to specific groupings. The keys are the recipient codes, the values the
names. For example:

```json5
{
  "france_priority": {
    "228": "Burundi",
    "231": "Central African Republic",
    "232": "Chad",
    "233": "Comoros",
    "235": "Democratic Republic of the Congo",
    "236": "Benin",
    "238": "Ethiopia",
    "240": "Gambia",
    "243": "Guinea",
    "251": "Liberia",
    "252": "Madagascar",
    "255": "Mali",
    "256": "Mauritania",
    "260": "Niger",
    "269": "Senegal",
    "274": "Djibouti",
    "283": "Togo",
    "287": "Burkina Faso",
    "349": "Haiti"
  }
}
```