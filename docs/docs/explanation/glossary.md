# Glossary

Each term as oda-data uses it.

- aggregate: A row the OECD computes from other rows, such as the DAC countries total (provider 20001) or developing countries (recipient 10100). Its `is_aggregate` is `True`, and oda-data never sums it with member rows.
- basis: How money is counted: `headline`, `net_disbursement`, `gross_disbursement`, `commitment`, or `grant_equivalent`. 2.x called it `measure`.
- bi_multi: The CRS column that says whether a flow is bilateral (1, 3, 7, 8), a core contribution to a multilateral (2), or a multilateral organisation's own outflow (4).
- channel: The organisation through which money flows, such as IDA (44002) or the World Food Programme (41140).
- core contribution: Money a provider gives a multilateral organisation for its general budget. DAC1 counts it as multilateral ODA, and `one.crs.imputed_multilateral_oda` spreads it across sectors.
- dimension: A column a result can be broken down by, named in `by=`, such as `provider`, `sector`, or `score`.
- flow: A DAC1 flow code, such as 1140 (net flows), 1160 (grant equivalents), or 1121 (grants), or a CRS flow, such as 11 (ODA grants).
- grant equivalent: The ODA value of a flow, counting a loan by how generous its terms are. The OECD's headline measure from 2018.
- group: A named set of providers, recipients, or sectors, such as `oda.groups.g7` or `oda.sectors.health`.
- headline: The basis that follows the OECD's own series: net flows to 2017 and grant equivalents from 2018 on DAC1, net disbursements on DAC2A.
- indicator: A named rule for what to count from one table, such as `oecd.dac1.total_oda`. A 2.x indicator was a catalogue code, such as `DAC1.10.1010`.
- line: A published DAC1 or DAC2A aid type code, such as 1010 (total ODA). DAC1 and DAC2A number their lines separately.
- modality: The CRS aid type, such as `B01` (core support to NGOs) or `C01` (project-type interventions).
- provider: Who gives the money. The OECD tables call this the donor.
- recipient: Who receives the money: a country, a region, or an unallocated row.
- release: One published version of an OECD table, identified by its token.
- scale: `millions` for money and counts, `percent` for ratios.
- total: A group's sum as one row beside its members, built with `oda.total(group)`.
- unit: A currency and prices, with a base year for constant prices, built with `oda.current()` or `oda.constant()`.
