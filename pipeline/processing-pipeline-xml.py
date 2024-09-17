import json

import concurrent.futures
from opensearchpy import OpenSearch, helpers
import pandas as pd
#from deep_translator import GoogleTranslator
import getpass

HEALTHCARE_CPV = [33600000,
               33110000,
               33120000, 33130000, 33150000, 33160000, 33170000, 33180000, 33190000,
               33141000, 33141420,
               85100000,
               35113400, 18143000]

CRITICAL_CPV = [18143000, #Protective Gear
                33141420, #Surgical Gloves
                33157400, #Medical breathing devices
                35113400] #Protective and safety clothing



def cpv_match(categories, cpv_list):
    category_prefixes = [str(i).zfill(8).rstrip('0') for i in categories]
    if isinstance(cpv_list, int):# If cpv_list is a single integer, convert it to a list
        cpv_list = [cpv_list]
    return any(any(str(cpv).zfill(8).startswith(prefix) for prefix in category_prefixes) for cpv in cpv_list)

def processing_scroll(df):
    # VALUE FILTERING
    df['Value'] = df['Value'].where((df['Value'] > 100) & (df['Value'] < 10 ** 10), -1) #Filter high and low values
    df["Healthcare CPV"] = df["CPV"].apply(lambda x: cpv_match(HEALTHCARE_CPV, x)) #Checks a contract CPV codes against a set of healthcare CPV
    df["Critical Services CPV"] = df["CPV"].apply(
        lambda x: cpv_match(CRITICAL_CPV, x) if cpv_match(HEALTHCARE_CPV, x) else False)
    #For those contracts categorized above checks again against a set of critical CPV
    return df

def parse_weight(weight):
    """Convert weight to a float, whether it's an integer, a string percentage, or a float."""
    if isinstance(weight, str):
        weight = weight.replace(",", ".")
        if "%" in weight:
            return float(weight.replace("%", "").strip())
    try:
        return float(weight)
    except ValueError:
        return -1

def extract_lots(can):
    lots = can.get("OBJECT_CONTRACT", {}).get("OBJECT_DESCR", [])
    extracted_lots = []
    if isinstance(lots, dict):
        lots=[lots]
    for lot in lots:
        # Extract the title, short description, and lot number
        lot_title = lot.get("TITLE", "-")
        lot_short_descr = lot.get("SHORT_DESCR", "-")
        lot_no = lot.get("LOT_NO", "-")
        cpv = lot.get("CPV_MAIN", {}).get("CPV_CODE", {}).get("@CODE", "-")

        # Extract the criteria and their weightings
        ac_list = lot.get("AC", {})
        criteria_list = []
        if isinstance(ac_list, dict): #Handle if contractors is a list or dictionary. I do not know for sure if it can be a list
            ac_list = [ac_list]

        for ac in ac_list:
            if ac:
                try:
                    #PRICE CRITERIA
                    ac_price = ac.get("AC_PRICE", {})
                    if isinstance(ac_price, dict):
                        criteria = {"Price": {"Criterion": "Price", "Weight": parse_weight(ac_price.get("AC_WEIGHTING", 0))}}
                    else:
                        criteria = {"Price": {"Criterion": "Price", "Weight": 0}}


                    # PRICE CRITERIA(S)
                    ac_quality = ac.get("AC_QUALITY", None)
                    if isinstance(ac_quality, dict):
                        criteria["Quality"] = [{"Criterion": ac_quality.get("AC_CRITERION", "-"), "Weight": parse_weight(ac_quality.get("AC_WEIGHTING", 0))}]
                    elif isinstance(ac_quality, list):
                        criteria["Quality"] = [{"Criterion": q.get("AC_CRITERION", "-"), "Weight": parse_weight(q.get("AC_WEIGHTING", 0))} for q in ac_quality]


                    # COST CRITERIA(S)
                    ac_cost = ac.get("AC_COST", None)
                    if isinstance(ac_cost, dict):
                        criteria["Cost"] = [{"Criterion": ac_cost.get("AC_CRITERION", "-"), "Weight": parse_weight(ac_cost.get("AC_WEIGHTING", 0))}]
                    elif isinstance(ac_cost, list):
                        criteria["Cost"] = [{"Criterion": q.get("AC_CRITERION", "-"), "Weight": parse_weight(q.get("AC_WEIGHTING", 0))} for q in ac_cost]

                    criteria_list.append(criteria)
                except Exception as e:
                    print(f"Error extracting criteria: {e}")
                    criteria_list.append({})
            else:
                print("warning: no criteria")
                criteria_list = []
        extracted_lots.append({
            "Lot Number": lot_no,
            "Title": lot_title,
            "Short Description": lot_short_descr,
            "Criteria": criteria_list,
            "CPV Codes": cpv
        })

    return extracted_lots

def extract_awarded_contracts(can):
    aw_contracts = can.get("AWARD_CONTRACT", {})
    if isinstance(aw_contracts, dict):  # Handle if contractors is a list or dictionary. I do not know for sure if it can be a list
        aw_contracts = [aw_contracts]
    awards = []
    for aw_contract in aw_contracts:
        aw_title = aw_contract.get("TITLE", "-")

        awarded_lot = aw_contract.get("AWARDED_CONTRACT", {})
        n_tenders = awarded_lot.get("TENDERS", {}).get("NB_TENDERS_RECEIVED", "0")
        contractors = awarded_lot.get("CONTRACTORS", {}).get("CONTRACTOR", [])
        if isinstance(contractors, dict): #Handle if contractors is a list or dictionary. I do not know for sure if it can be a list
            contractors = [contractors]

        contractors_info = []
        for contractor in contractors:
            c_address = contractor.get("ADDRESS_CONTRACTOR", {})
            c_name = c_address.get("OFFICIALNAME", "-")
            c_country = c_address.get("COUNTRY", {}).get("@VALUE", "-")
            c_town = c_address.get("TOWN", "-")
            c_postal_code = c_address.get("POSTAL_CODE", "-")
            c_address_line = c_address.get("ADDRESS", "-")
            c_email = contractor.get("E_MAIL", "-")
            c_phone = c_address.get("PHONE", "-")
            c_url = contractor.get("URL", "-")  # Note: not all contractor objects have URL
            c_national_id = c_address.get("NATIONALID", "-")

            contractor_info = {
                "Name": c_name,
                "National ID": c_national_id,
                "Address": {
                    "Country": c_country,
                    "Town": c_town,
                    "Postal Code": c_postal_code,
                    "Address": c_address_line, },
                "Contact": {
                    "URL": c_url,
                    "Email": c_email,
                    "Phone": c_phone
                }
            }

            contractors_info.append(contractor_info)

        aw_info = {
            "Awarded Contract Title": aw_title,
            "Number of Tenders": n_tenders,
            "Contractors": contractors_info
        }
        awards.append(aw_info)

    return awards


def extract_contracting_authority(can):
    contracting_body = can.get("CONTRACTING_BODY", {})
    address = contracting_body.get("ADDRESS_CONTRACTING_BODY", {})

    ca_activity = contracting_body.get("CA_ACTIVITY", {}).get("@VALUE", "-") or contracting_body.get(
        "CA_ACTIVITY_OTHER", "-")
    ca_name = address.get("OFFICIALNAME", "-")
    ca_url_general = address.get("URL_GENERAL", "-")
    ca_country = address.get("COUNTRY", {}).get("@VALUE", "-")
    ca_town = address.get("TOWN", "-")
    ca_postal_code = address.get("POSTAL_CODE", "-")
    ca_address = address.get("ADDRESS", "-")
    ca_phone = address.get("PHONE", "-")
    ca_email = address.get("E_MAIL", "-")
    ca_ca_type = contracting_body.get("CA_TYPE", {}).get("@VALUE", "-") or contracting_body.get("CA_TYPE_OTHER", "-")
    ca_national_id = address.get("NATIONALID", "-")

    return [{
        "Name": ca_name,
        "National ID": ca_national_id,
        "Activity": ca_activity,
        "CA Type": ca_ca_type,
        "Address": {
            "Country": ca_country,
            "Town": ca_town,
            "Postal Code": ca_postal_code,
            "Address": ca_address
        },
        "Contact": {
            "URL": ca_url_general,
            "Email": ca_email,
            "Phone": ca_phone
        }
    }]


# Initialize the OpenSearch client
host = 'localhost'
port = 9200
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

# Define your index and field
index_name = "ted-xml"
# Define the query to retrieve all documents
query = {
    "query": {
        "match_all": {}  # Retrieve all documents
    }
}
scroll_size = 1000
# Execute the initial search query to get the first batch of results
response = client.search(
    index=index_name,
    body=query,
    size=scroll_size,  # Number of documents to retrieve per batch
    scroll="10m"  # Keep the scroll window open for 1 minute
)

scroll_id = response["_scroll_id"]

scr = 1
while True:
    # Continue scrolling
    response = client.scroll(scroll_id=scroll_id, scroll="1m")
    id_field_pairs = []

    # Extract document IDs and corresponding field values from the current batch of results
    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
        doc_id = hit["_id"]
        skip = False  # Temp variable to skip certain formats in processing phase

        if "CONTRACT_AWARD_NOTICE" in hit["_source"].keys(): ######## Processing for legacy XML ####################################
            can = hit["_source"]["CONTRACT_AWARD_NOTICE"]
            if isinstance(can, list):
                can = can[0]
            try:
                title = can.get("OBJECT_CONTRACT", {}).get("TITLE", "-")
                description = can.get("OBJECT_CONTRACT", {}).get("SHORT_DESCR", "-")

                cpv_data = hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"] # may be a list or a dictionary
                if isinstance(cpv_data, list):
                    cpv = [int(item["@CODE"]) for item in cpv_data]
                    cpv_desc = [str(item["#text"]) for item in cpv_data]
                else:
                    cpv = int(cpv_data["@CODE"])
                    cpv_desc = str(cpv_data["#text"])

                health_cpv = False
                critical_cpv = False

                country = hit["_source"]["CODED_DATA_SECTION"].get("NOTICE_DATA",{}).get("ISO_COUNTRY",{}).get("@VALUE","-")

                ca_data = extract_contracting_authority(can)
                awards_data = extract_awarded_contracts(can)
                lot_data = extract_lots(can)
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                print(hit)
        else:          ############################################## Processing for eforms ##########################################
            skip = True #Eforms has been separated into different index and processing pipeline

        try: ######################################################### Query for CSV data ################################################
            inner_hit = client.get(index="ted-csv", id=doc_id)
            value = inner_hit["_source"]["VALUE_EURO_FIN_2"]

            multiple_country = inner_hit["_source"]["B_MULTIPLE_COUNTRY"]
            central_body = inner_hit["_source"]["B_AWARDED_BY_CENTRAL_BODY"]
            joint_procurement = inner_hit["_source"]["B_INVOLVES_JOINT_PROCUREMENT"]
            cae_type = inner_hit["_source"]["CAE_TYPE"]
            if multiple_country:
                proc_route = "Cross Country Procurement"
            elif joint_procurement:
                proc_route = "Joint Procurement"
            elif not central_body:
                proc_route = "Direct Procurement"
            elif cae_type == "1" or cae_type == "N":
                proc_route = "Centralized Procurement at National Level"
            elif cae_type == "3" or cae_type == "R":
                proc_route = "Centralized Procurement at Regional Level"
            elif cae_type == "4" or cae_type == "6" or cae_type == "8" or cae_type == "Z":
                proc_route = "Centralized Procurement at Unspecified Level"
            else:
                proc_route = "Not applicable"
        except Exception as e: ########################################## If CSV not found handler ###########################################
            print(f"An error occurred: {e}")
            value = -1 #To obtain value from xml, currency transform is needed.
            proc_route = "Unknown"

        title_translated = "-" # No translation for now (too slow)
        description_translated = "-"

        if skip == False:
            id_field_pairs.append((doc_id, title, title_translated, description, description_translated, cpv, cpv_desc,
                                   health_cpv, critical_cpv, country, value, proc_route, ca_data, lot_data, awards_data))
    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description",
                                               "Description (Translation)", "CPV", "CPV Description", "Healthcare CPV","Critical Services CPV",
                                               "Country", "Value", "Procurement Route", "Contracting Authority","Lots", "Awarded Contracts"])

    processing_scroll(df)

    print("Scroll " + str(scr))
    actions = [
        {
            "_op_type": "index",
            "_index": "procure_v3",
            "_id": doc['Document ID'],
            **{f"{col_name}": doc[col_name] for col_name in df.columns if col_name != "Document ID"}

        }
        for _, doc in df.iterrows()
    ]
    try:
        success, failed = helpers.bulk(client, actions, index="procure_v3", raise_on_error=True, refresh=True)
        print(f"Successfully indexed {success} documents.")
        print(f"Failed to index {failed} documents.")
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
    # Check if there are more results to fetch
    scr = scr + 1
    if len(response["hits"]["hits"]) < scroll_size:
        break
# Create a DataFrame to store the document IDs and field values
