import json
import re
import concurrent.futures
from opensearchpy import OpenSearch, helpers
import pandas as pd
import traceback
#from deep_translator import GoogleTranslator
import getpass
from datetime import datetime

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
        weight = weight.replace(",", ".").strip()

        match = re.fullmatch(r"(\d+(?:\.\d+)?)(\D+)", weight.strip()) # We are exactly matching a number, including optional decimal part followed by a non numeric string
        if match:
            weight = match.group(1)  # Extract the numeric part
        else:
            return -1.0  # Fail if the structure doesn't match exactly

    try: #Float conversion
        weight = float(weight)
    except ValueError:
        return -1.0  # Invalid float string

    if isinstance(weight, (int, float)):
        if 1 < weight <= 100:
            return weight/100 #Percentage based weight. We assume percentages below 1% are not reasonable.
        elif 0 <= weight <= 1:
            return weight #Proportion based weight
        else:
            return -1.0  # Out of range
    return -1.0  # Invalid type


def get_main_criterion(criteria_list):
    """
    Function to determine the main criterion based on the highest weight in the criteria list.
    """
    highest_weight = -1
    main_criterion = None

    for ac in criteria_list:

        for criterion_type, criterion_dict in ac.items():
            if isinstance(criterion_dict, list):
                weight = sum(subcriteria_dict.get("Weight", 0) for subcriteria_dict in criterion_dict)
                if weight > highest_weight:
                    highest_weight = weight
                    main_criterion = criterion_type
            else:
                weight = criterion_dict.get("Weight", 0)
                if weight > highest_weight:
                    highest_weight = weight
                    main_criterion = criterion_type

    return main_criterion if main_criterion else "-"

def extract_lots(can):
    lots = can.get("OBJECT_CONTRACT", {}).get("OBJECT_DESCR", [])
    extracted_lots = []
    if isinstance(lots, dict):
        lots=[lots]
    number_of_lots = len(lots)
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
                        criteria["Quality"] = [{"Criterion": ac_quality.get("AC_CRITERION", "-"),
                                                "Criterion (Translation)": "-",
                                                "Weight": parse_weight(ac_quality.get("AC_WEIGHTING", 0))}]
                    elif isinstance(ac_quality, list):
                        criteria["Quality"] = [{"Criterion": q.get("AC_CRITERION", "-"),
                                                "Criterion (Translation)": "-",
                                                "Weight": parse_weight(q.get("AC_WEIGHTING", 0))} for q in ac_quality]


                    # COST CRITERIA(S)
                    ac_cost = ac.get("AC_COST", None)
                    if isinstance(ac_cost, dict):
                        criteria["Cost"] = [{"Criterion": ac_cost.get("AC_CRITERION", "-"),
                                            "Criterion (Translation)": "-",
                                            "Weight": parse_weight(ac_cost.get("AC_WEIGHTING", 0))}]
                    elif isinstance(ac_cost, list):
                        criteria["Cost"] = [{"Criterion": q.get("AC_CRITERION", "-"),
                                            "Criterion (Translation)": "-",
                                             "Weight": parse_weight(q.get("AC_WEIGHTING", 0))} for q in ac_cost]

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
            "Title (Translated)": "-",
            "Short Description (Translated)": "-",
            "Criteria": criteria_list,
            "Main Criterion": get_main_criterion(criteria_list),
            "CPV Codes": cpv
        })

    return number_of_lots, extracted_lots

def extract_awarded_contracts(can):
    aw_contracts = can.get("AWARD_CONTRACT", {})
    if isinstance(aw_contracts, dict):  # Handle if contractors is a list or dictionary. I do not know for sure if it can be a list
        aw_contracts = [aw_contracts]
    awards = []
    for aw_contract in aw_contracts:
        aw_title = aw_contract.get("TITLE", "-")

        awarded_lot = aw_contract.get("AWARDED_CONTRACT", {})
        n_tenders = awarded_lot.get("TENDERS", {}).get("NB_TENDERS_RECEIVED", "0")
        date_conclusion = awarded_lot.get("DATE_CONCLUSION_CONTRACT", None)
        try:
            if date_conclusion is not None:
                date_conclusion = datetime.strptime(date_conclusion, '%Y-%m-%d')
        except ValueError:
            date_conclusion = None  # Handle parsing errors gracefully

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
            c_nuts = c_address.get("n2016:NUTS", {}).get("@CODE", "-")

            contractor_info = {
                "Name": c_name,
                "National ID": c_national_id,
                "Address": {
                    "Country": c_country,
                    "Town": c_town,
                    "Postal Code": c_postal_code,
                    "Address": c_address_line,
                    "Territorial Unit (NUTS3)": c_nuts
                },
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
            "Contractors": contractors_info,
            "Conclusion Date": date_conclusion
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
    ca_type = contracting_body.get("CA_TYPE", {}).get("@VALUE", "-") or contracting_body.get("CA_TYPE_OTHER", "-")
    ca_national_id = address.get("NATIONALID", "-")
    ca_nuts = address.get("n2016:NUTS", {}).get("@CODE", "-")


    return [{
        "Name": ca_name,
        "National ID": ca_national_id,
        "Activity": ca_activity,
        "CA Type": ca_type,
        "Address": {
            "Country": ca_country,
            "Town": ca_town,
            "Postal Code": ca_postal_code,
            "Address": ca_address,
            "Territorial Unit (NUTS3)": ca_nuts
        },
        "Contact": {
            "URL": ca_url_general,
            "Email": ca_email,
            "Phone": ca_phone
        }
    }]


def calculate_p_route(multiple_country, joint_procurement, central_body, ca_type):
    if multiple_country:
        return "Cross Country Procurement"
    elif joint_procurement:
        return "Joint Procurement"
    elif not central_body:
        return "Direct Procurement"
    elif ca_type == "1" or ca_type == "N":
        return "Centralized Procurement at National Level"
    elif ca_type == "3" or ca_type == "R":
        return "Centralized Procurement at Regional Level"
    elif ca_type == "4" or ca_type == "6" or ca_type == "8" or ca_type == "Z":
        return "Centralized Procurement at Unspecified Level"
    else:
        return "Not applicable"

def calculate_p_technique(dynamic_purch, eauction, on_behalf, central_body, fram_agreement, multiple_country):
    return {
        "Framework Agreement": fram_agreement,
        "Dynamic Purchasing Systems" : dynamic_purch,
        "Electronic Auction": eauction,
        "Electronic Catalogue": False,
        "Centralized Purchasing Activities and Central Purchasing Bodies" : on_behalf and central_body,
        "Occasional Joint Procurement": on_behalf and not central_body,
        "Procurement Involving Contracting Authorities from Different Member States": multiple_country
    }




def extract_notice_data(codeddata):
    country = codeddata.get("NOTICE_DATA", {}).get("ISO_COUNTRY", {}).get("@VALUE","-")
    codifdata = codeddata.get("CODIF_DATA", {})
    ca_type = codifdata.get("AA_AUTHORITY_TYPE", {}).get("@CODE", "-")
    c_nature = codifdata.get("NC_CONTRACT_NATURE", {}).get("#text", "-")
    p_type = codifdata.get("PR_PROC", {}).get("#text", "-")
    date_dispatch = codifdata.get("DS_DATE_DISPATCH", None)
    try:
        if date_dispatch is not None:
            date_dispatch = datetime.strptime(date_dispatch, '%Y%m%d')
    except ValueError:
        date_dispatch = None  # Handle parsing errors gracefully

    return country, ca_type, c_nature, p_type, date_dispatch


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
index = "procure_v4"
scroll_size = 1000
# Execute the initial search query to get the first batch of results
response = client.search(
    index = "ted-xml",
    body =   {"query":   {
                        "match_all": {}  # Retrieve all documents
                        }
            },
    size = scroll_size,  # Number of documents to retrieve per batch
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

        if "CONTRACT_AWARD_NOTICE" in hit["_source"].keys(): ######## Processing for legacy XML ####################################

            country, ca_type, c_nature, proc_type, date_dispatch = extract_notice_data(hit["_source"]["CODED_DATA_SECTION"])

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


                ca_data = extract_contracting_authority(can)
                awards_data = extract_awarded_contracts(can)
                number_of_lots, lot_data = extract_lots(can)

                try:  ######################################################### Query for CSV data ################################################
                    inner_hit = client.get(index="ted-csv", id=doc_id)
                    csv_found = True

                    value = inner_hit["_source"]["VALUE_EURO_FIN_2"]

                    multiple_country = inner_hit["_source"]["B_MULTIPLE_COUNTRY"]
                    central_body = inner_hit["_source"]["B_AWARDED_BY_CENTRAL_BODY"]
                    joint_procurement = inner_hit["_source"]["B_INVOLVES_JOINT_PROCUREMENT"]
                    dynamic_purch = inner_hit["_source"]["B_DYN_PURCH_SYST"]
                    eauction = inner_hit["_source"]["B_ELECTRONIC_AUCTION"]
                    on_behalf = inner_hit["_source"]["B_ON_BEHALF"]

                    fram_agreement = inner_hit["_source"].get("B_FRA_AGREEMENT", False)
                    fram_estimated = inner_hit["_source"].get("FRA_ESTIMATED")
                    if fram_estimated and isinstance(fram_estimated, str):
                        if 'K' in fram_estimated or 'C' in fram_estimated:
                            fram_agreement = True # K for when the keyword framework was detected in the description, C for consistency, previous notices were indicated as framework agreements. A third option has not been considered, A for multiple awards per lot, which may correspond with fram. agreements, dynamic purch. systems or innovation partnerships

                    proc_route = calculate_p_route(multiple_country, joint_procurement, central_body, ca_type)
                    proc_technique = calculate_p_technique(dynamic_purch, eauction, on_behalf, central_body, fram_agreement, multiple_country)
                    if central_body:
                        if ca_type == "1":
                            health_ca_class = 'Government Public Procurers'
                        elif ca_type == "3" or ca_type == "R":
                            health_ca_class = 'Regional or Local Public Purchasing Bodies'
                        else:
                            health_ca_class = 'Central Public Purchasing Bodies'
                    else:
                        if health_cpv:
                            health_ca_class = 'Healthcare Direct Procurer'
                        else:
                            health_ca_class = 'Non-Healthcare Direct Procurer'

                except Exception as e:  ########################################## If CSV not found handler ###########################################
                    print(f"An error occurred: {e}")
                    csv_found = False
                    value = -1  # To obtain value from xml, currency transform is needed.
                    proc_route = "Unknown"
                    proc_technique = "Unknown"
                    health_ca_class = "Unknown"

                title_translated = "-"  # No translation for now (too slow)
                description_translated = "-"

                sources = {"TED-XML": True}
                if csv_found:
                    sources["TED-CSV"] = True
                tags = {"Source": sources,
                        "Process Date": datetime.now()
                        }

                id_field_pairs.append(
                    (doc_id, title, title_translated, description, description_translated, date_dispatch,
                     cpv, cpv_desc, health_cpv, critical_cpv,
                     country, value, c_nature, proc_route, proc_type, health_ca_class,
                     ca_data, number_of_lots, lot_data, awards_data, tags))

            except Exception as e: ########################################## Error extracting some field from XML ####################################
                print(f"An unexpected error occurred: {e}")
                traceback.print_exc()
                print(hit)
        else:          ############################################## Processing for eforms ##########################################
            continue #Eforms has been separated into different index and processing pipeline

    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description", "Description (Translation)", "Dispatch Date",
                                               "CPV", "CPV Description", "Healthcare CPV", "Critical Services CPV",
                                               "Country", "Value", "Contract Nature", "Procurement Route", "Procurement Type", "Healthcare Authority Class",
                                               "Contracting Authority", "Number of Lots", "Lots", "Awarded Contracts", "Tags"])

    processing_scroll(df)

    print("Scroll " + str(scr))
    actions = [
        {
            "_op_type": "index",
            "_index": index,
            "_id": doc['Document ID'],
            **{f"{col_name}": doc[col_name] for col_name in df.columns if col_name != "Document ID"}

        }
        for _, doc in df.iterrows()
    ]
    try:
        success, failed = helpers.bulk(client, actions, index = index, raise_on_error=True, refresh=True)
        print(f"Successfully indexed {success} documents.")
        print(f"Failed to index {failed} documents.")
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
    # Check if there are more results to fetch
    scr = scr + 1
    if len(response["hits"]["hits"]) < scroll_size:
        break
# Create a DataFrame to store the document IDs and field values
