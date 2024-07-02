from opensearchpy import OpenSearch, helpers
import pandas as pd
import os
import getpass
#                               ------------ CONSTANTS -----------------
FOLDER = "../data/oecd-eurostat/"
HOST = 'localhost'
PORT = 9200
INDEX = 'oecd-eurostat'
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': HOST, 'port': PORT}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

metadata = pd.read_csv("./Statistical_metadata.csv")
legend = {
    "COUNTRY": "Country",
    "TIME_PERIOD": "Year",
    "POPU": "Population",
    "PROVIDER": "Healthcare Provider",
    "OBS_VALUE": "Value",
    "FINANCING_SCHEME": "Financing Scheme",
    "FUNCTION": "Function",
    "GDEATHRT_THSP": "Crude death rate - per thousand people",
    "INFMORRT": "Infant mortality rate - Per 1000 live births",
    "GBIRTHRT_THSP": "Crude birth rate - Per 1000 people",
    "LIFE_EXP_M": "Life expectancy male - Years",
    "LIFE_EXP_F": "Life expectancy female - Years",
    "HLY_M": "Healthy life years male - Years",
    "HLY_F": "Healthy life years female - Years",
    "PT_POP_Y_GE65": "Population 65 years and over - Percentage",
    "POPU_GROWTH": "Population growth rate - Percentage",
    "PT_B1GQ": "Percentage of GDP - Percentage",
    "PT_GOV_EXP_HEA": "Percentage of expenditure on health - Percentage",
    "TOT_HC": "Total Health expenditure - Million euro",
    "TOT_HC_EUR_HAB": "Total Health expenditure per inhabitant - Euro",
    "GDP_MIL_EUR": "GDP - Million euro",
    "HEALTH_GEN_GOV_EXP_PERC": "Health general government expenditure - Percentage",
    "HC1": "Curative care",
    "HC2": "Rehabilitative care",
    "HC3": "Long-term care (health)",
    "HC4": "Ancillary services (non-specified by function)",
    "HC5": "Medical goods (non-specified by function)",
    "HC6": "Preventive care",
    "HC7": "Governance and health system and financing administration",
    "HP1": "Hospitals",
    "HP2": "Residential long-term care facilities",
    "HP3": "Providers of ambulatory healthcare",
    "HP4": "Providers of ancillary services",
    "HP5": "Retailers and other providers of medical goods",
    "HP6": "Providers of preventive care",
    "HP7": "Providers of healthcare system administration and financing",
    "HF11": "Government schemes",
    "HF12HF13": "Compulsory contributory health insurance schemes",
    "HF2": "Voluntary health care payment schemes",
    "HF3": "Household out-of-pocket payments"
}

#                                   ---------CODE---------
dfs = []
stat_files = [stat_file for stat_file in os.listdir(FOLDER)]
for csv_file in stat_files:
    file_path = os.path.join(FOLDER, csv_file)
    df = pd.read_csv(file_path, sep=';')
    if csv_file == "Health exp by scheme.csv":
        df['ID'] = csv_file[:-4].replace(" ", "") + "_" + df['FINANCING_SCHEME'] + "_" + df['ID']
    elif csv_file == "Health exp Government  Compulsory financing schemes.csv":
        df['ID'] = csv_file[:-4].replace(" ", "") + "_" + df['UNIT_MEASURE'] + "_" + df['ID']
    elif csv_file == "Health exp by services.csv":
        df['ID'] = csv_file[:-4].replace(" ", "") + "_" + df['FUNCTION'] + "_" + df['ID']
    elif csv_file == "Health exp by providers.csv":
        df['ID'] = csv_file[:-4].replace(" ", "") + "_" + df['PROVIDER'] + "_" + df['ID']
    else:
        df['ID'] = csv_file[:-4].replace(" ", "") + "_" + df['ID']

    df.rename(columns=legend, inplace=True)
    df.replace(legend, inplace=True)
    df['File'] = csv_file[:-4]
    df = pd.merge(df, metadata, on='File', how='left')
    df = df.where(pd.notna(df), None)

    columns_to_drop = ['DATAFLOW', 'Health care provider', 'Financing scheme', 'UNIT_MEASURE']
    columns_existing = [col for col in columns_to_drop if col in df.columns]
    if columns_existing:
        df.drop(columns=columns_existing, inplace=True)
    actions = [
        {
            "_op_type": "index",
            "_index": INDEX,
            "_id": doc['ID'],
            **{f"{col_name}": doc[col_name] for col_name in df.columns if col_name != 'ID'}
        }
        for _, doc in df.iterrows()
    ]
    try:
        success, failed = helpers.bulk(client, actions, index=INDEX, raise_on_error=True, refresh=True)
        print("Indexed "+csv_file)
    except Exception as e:
        print(f"Error during bulk indexing: {e}")

