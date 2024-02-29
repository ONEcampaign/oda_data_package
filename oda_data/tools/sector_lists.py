# Education
import pandas as pd

from oda_data.clean_data.schema import OdaSchema

edu_unspecified = list(range(111 * 100, 112 * 100)) + [111]
edu_basic = list(range(112 * 100, 113 * 100)) + [112]
edu_secondary = list(range(113 * 100, 114 * 100)) + [113]
edu_postsec = list(range(114 * 100, 115 * 100)) + [114]

# Health
health_general = list(range(121 * 100, 122 * 100)) + [121]
health_basic = list(range(122 * 100, 123 * 100)) + [122]
health_NCDs = list(range(123 * 100, 124 * 100)) + [123]
pop_RH = list(range(130 * 100, 131 * 100)) + [130]

# Social protection
social_pro = list(range(16010, 16020))
social_services = [16050]
social_other = list(range(16020, 16050)) + list(range(16060, 16100)) + [160]

# other social infrastructure
water_sanitation = list(range(140 * 100, 141 * 100)) + [140]
gov_ps = list(range(152 * 100, 153 * 100)) + [152]

# Government and Civil Society
public_sector = [
    15110,
    15121,
    15122,
    15123,
    15124,
    15126,
    15127,
    15143,
    15144,
    15154,
    15196,
    151,
]
public_finance_m = [15117, 15118, 15119, 15111]
decentral_subnational = [15128, 15129, 15185, 15112]
anticurruption = [15113]
drm = [15116, 15155, 15156, 15114]
public_procurement = [15125]
legal_judicial = [15130, 15131, 15132, 15133, 15134, 15135, 15136, 15137]
macroeconomic_policy = [15142]

democratic_participation = [15150]
elections = [15151]
legislature_political_parties = [15152]
media_free_flow_info = [15153]
human_rights = [15160]
womens_rights = [15170]
ending_violence_women_girls = [15180]
migration = [15190]


# Agriculture, forestry, fishing
agriculture = list(range(311 * 100, 312 * 100)) + [311]
forestry_fishing = list(range(312 * 100, 314 * 100)) + [312, 313]

# Other Economic infrastructure
transport_storage = list(range(210 * 100, 211 * 100)) + [210]
communications = list(range(220 * 100, 221 * 100)) + [220]
banking_financial = list(range(240 * 100, 241 * 100)) + [240]
business = list(range(250 * 100, 251 * 100)) + [250]
industry_mining_const = list(range(321 * 100, 324 * 100)) + [320, 321, 322, 323]
trade_p_r = list(range(331 * 100, 333 * 100)) + [331, 332]
trade_other = [330]

# Energy
energy_policy = [231] + list(range(231 * 100, 232 * 100))
energy_generation_renewable = [232] + list(range(232 * 100, 233 * 100))
energy_generation_nonrenewable = [233] + list(range(233 * 100, 234 * 100))
hybrid_energy_plants = [234] + list(range(234 * 100, 235 * 100))
nuclear_energy_plants = [235] + list(range(235 * 100, 236 * 100))
energy_distribution = [236] + list(range(236 * 100, 237 * 100))

# Environmental Protection
env_policy = [41010]
biosphere_protection = [41020]
bio_diversity = [41030]
site_preservation = [41040]
environment_edu = [41081]
environment_research = [41082]

# Humanitarian
emergency_response = list(range(720 * 100, 721 * 100)) + [720]
reconstruction = list(range(730 * 100, 731 * 100)) + [730]
disaster_prevention = list(range(740 * 100, 741 * 100)) + [740]

# Multi_sector
multi_sector = [43010, 430]
urban_dev = list(range(43030, 43040))
rural_dev = list(range(43040, 43050))
drr = [43060]
other_multi = [43050] + list(range(43070, 431 * 100))

# other
general_budget = list(range(510 * 100, 511 * 100)) + [510]
food_aid = list(range(520 * 100, 521 * 100)) + [520]
commodity_other = list(range(530 * 100, 531 * 100)) + [530]
debt_action_total = list(range(600 * 100, 610 * 100)) + [600]
admin_total = list(range(910 * 100, 911 * 100)) + [910]

refugees = list(range(930 * 100, 931 * 100)) + [930]
unspecified = list(range(998 * 100, 999 * 100)) + [998]


def get_sector_groups():
    return {
        "Education, Level Unspecified": edu_unspecified,
        "Basic Education": edu_basic,
        "Secondary Education": edu_secondary,
        "Post-Secondary Education": edu_postsec,
        "Health, General": health_general,
        "Basic Health": health_basic,
        "Non-communicable diseases (NCDs)": health_NCDs,
        "Population Policies/Programmes & Reproductive Health": pop_RH,
        "Social Protection": social_pro,
        "Multi-Sector Aid for Basic Social Services": social_services,
        "Water Supply & Sanitation": water_sanitation,
        "Public sector policy & management": public_sector,
        "Public finance management": public_finance_m,
        "Decentralization & Subnational government": decentral_subnational,
        "Anti-corruption organisations and institutions": anticurruption,
        "Domestic resource mobilisation": drm,
        "Public procurement": public_procurement,
        "Legal & Judicial Development": legal_judicial,
        "Macroeconomic policy": macroeconomic_policy,
        "Democratic participation and civil society": democratic_participation,
        "Legislature & Political Parties": legislature_political_parties,
        "Media & Free Flow of Information": media_free_flow_info,
        "Elections": elections,
        "Human Rights": human_rights,
        "Women's rights organisations, movements, and institutions": womens_rights,
        "Ending violence against women and girls": ending_violence_women_girls,
        "Migration": migration,
        "Conflict Peace and Security": gov_ps,
        "Other Social Infrastructure & Services": social_other,
        "Agriculture": agriculture,
        "Forestry & Fishing": forestry_fishing,
        "Transport & Storage": transport_storage,
        "Communications": communications,
        "Energy Policy": energy_policy,
        "Energy Generation, Renewable": energy_generation_renewable,
        "Energy Generation, Non-renewable": energy_generation_nonrenewable,
        "Hybrid Energy Plants": hybrid_energy_plants,
        "Nuclear Energy Plants": nuclear_energy_plants,
        "Energy Distribution": energy_distribution,
        "Banking & Financial Services": banking_financial,
        "Business & Other Services": business,
        "Industry, Mining, Construction": industry_mining_const,
        "Trade Policies & Regulations": trade_p_r,
        "Trade other": trade_other,
        "Environmental Policy and Admin Management": env_policy,
        "Biosphere Protection": biosphere_protection,
        "Bio-diversity": bio_diversity,
        "Site- Preservation": site_preservation,
        "Environment Education/Training": environment_edu,
        "Environmental Research": environment_research,
        "Emergency Response": emergency_response,
        "Reconstruction, Relief & Rehabilitation": reconstruction,
        "Disaster Prevention & Preparedness": disaster_prevention,
        "Multi-Sector": multi_sector,
        "Urban Development": urban_dev,
        "Rural Development": rural_dev,
        "Disaster Risk Reduction": drr,
        "Other multi-sector Aid": other_multi,
        "General Budget Support": general_budget,
        "Developmental Food Aid/Food Security Assistance": food_aid,
        "Other Commodity Assistance": commodity_other,
        "Action Relating to Debt": debt_action_total,
        "Administrative Costs of Donors": admin_total,
        "Refugees in Donor Countries": refugees,
        "Unallocated/ Unspecificed": unspecified,
    }


def get_broad_sector_groups():
    return {
        "Education, Level Unspecified": "Education",
        "Basic Education": "Education",
        "Secondary Education": "Education",
        "Post-Secondary Education": "Education",
        "Health, General": "Health",
        "Basic Health": "Health",
        "Non-communicable diseases (NCDs)": "Health",
        "Population Policies/Programmes & Reproductive Health": "Health",
        "Social Protection": "Social infrastructure, protection and services",
        "Multi-Sector Aid for Basic Social Services": "Social infrastructure, protection and services",
        "Water Supply & Sanitation": "Water Supply & Sanitation",
        "Public sector policy & management": "Government",
        "Public finance management": "Government",
        "Decentralization & Subnational government": "Government",
        "Anti-corruption organisations and institutions": "Civil society",
        "Domestic resource mobilisation": "Government",
        "Public procurement": "Government",
        "Legal & Judicial Development": "Government",
        "Macroeconomic policy": "Government",
        "Democratic participation and civil society": "Civil society",
        "Elections": "Civil society",
        "Legislature & Political Parties": "Government",
        "Media & Free Flow of Information": "Civil society",
        "Human Rights": "Civil society",
        "Women's rights organisations, movements, and institutions": "Civil society",
        "Ending violence against women and girls": "Civil society",
        "Migration": "Government",
        "Conflict Peace and Security": "Conflict Peace and Security",
        "Other Social Infrastructure & Services": "Social infrastructure, protection and services",
        "Agriculture": "Agriculture and Forestry & Fishing",
        "Forestry & Fishing": "Agriculture and Forestry & Fishing",
        "Transport & Storage": "Transport & Storage and communications",
        "Communications": "Transport & Storage and communications",
        "Energy Policy": "Energy",
        "Energy Generation, Renewable": "Energy",
        "Energy Generation, Non-renewable": "Energy",
        "Hybrid Energy Plants": "Energy",
        "Nuclear Energy Plants": "Energy",
        "Energy Distribution": "Energy",
        "Banking & Financial Services": "Banking & Financial Services and Business",
        "Business & Other Services": "Banking & Financial Services and Business",
        "Industry, Mining, Construction": "Industry, Mining, Construction",
        "Trade Policies & Regulations": "Trade Policies & Regulations",
        "Trade other": "Trade Policies & Regulations",
        "Environmental Policy and Admin Management": "Environment Protection",
        "Biosphere Protection": "Environment Protection",
        "Bio-diversity": "Environment Protection",
        "Site- Preservation": "Environment Protection",
        "Environment Education/Training": "Environment Protection",
        "Environmental Research": "Environment Protection",
        "Emergency Response": "Humanitarian",
        "Reconstruction, Relief & Rehabilitation": "Humanitarian",
        "Disaster Prevention & Preparedness": "Humanitarian",
        "Multi-Sector": "Multi-sector",
        "Urban Development": "Multi-sector",
        "Rural Development": "Multi-sector",
        "Disaster Risk Reduction": "Multi-sector",
        "Other multi-sector Aid": "Multi-sector",
        "General Budget Support": "General Budget Support",
        "Developmental Food Aid/Food Security Assistance": "Other",
        "Other Commodity Assistance": "Other",
        "Null": "Other",
        "Action Relating to Debt": "Action Relating to Debt",
        "Administrative Costs of Donors": "Administrative Costs of Donors",
        "Refugees in Donor Countries": "Refugees in Donor Countries",
        "Unallocated/ Unspecificed": "Unallocated/ Unspecificed",
        "government & Civil Society": "Government & Civil Society",
    }


def _groupby_sector(data: pd.DataFrame) -> pd.DataFrame:
    data = data.drop("purpose_code", axis=1)
    return (
        data.groupby(
            [c for c in data.columns if c not in [OdaSchema.VALUE]],
            observed=True,
            dropna=False,
        )
        .sum(numeric_only=True)
        .loc[lambda d: d[OdaSchema.VALUE] != 0]
        .reset_index(drop=False)
        .rename(columns={"broad_sector": OdaSchema.PURPOSE_NAME})
    )


def add_sectors(data: pd.DataFrame) -> pd.DataFrame:
    # Load the sectors group
    sectors = get_sector_groups()

    for name, codes in sectors.items():
        data.loc[data.purpose_code.isin(codes), "broad_sector"] = name

    data = _groupby_sector(data)

    return data


def add_broad_sectors(data: pd.DataFrame) -> pd.DataFrame:
    # copy data
    data = data.copy(deep=True)
    # Load the sectors group
    sectors = get_sector_groups()
    broad = get_broad_sector_groups()

    for name, codes in sectors.items():
        data.loc[data.purpose_code.isin(codes), "broad_sector"] = broad[name]

    data = _groupby_sector(data)

    return data
