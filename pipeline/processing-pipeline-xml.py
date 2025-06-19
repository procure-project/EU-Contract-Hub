import json

from pipeline.pipelinepackage.processingmodule import process_value, process_health_cpv
from pipelinepackage import processingmodule as proc
import concurrent.futures
from opensearchpy import OpenSearch, helpers
import pandas as pd
import traceback
import getpass
from datetime import datetime
from pipelinepackage.auth import get_opensearch_auth


def log_pipeline_status(client, doc_id):
    composite_id = f"processing-{doc_id}"
    doc = {
        "pipeline":  "processing",
        "doc_id": doc_id,
        "timestamp": datetime.datetime.now()
    }
    client.index(index="pipeline_status", id=composite_id, body=doc)

def is_doc_processed(client, doc_id):
    composite_id = f"processing-{doc_id}"
    return client.exists(index="pipeline_status", id=composite_id)

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


def extract_lots(can):
    lots = can.get("OBJECT_CONTRACT", {}).get("OBJECT_DESCR", [])
    extracted_lots = []
    if isinstance(lots, dict):
        lots=[lots]
    number_of_lots = len(lots)
    for lot in lots:
        # Extract the criteria and their weightings
        ac_list = lot.get("AC", {})
        if isinstance(ac_list, dict): #Handle if contractors is a list or dictionary. I do not know for sure if it can be a list
            ac_list = [ac_list]
        criteria = []
        for ac in ac_list:
            if ac:
                try:
                    #PRICE CRITERIA
                    ac_price = ac.get("AC_PRICE", {})
                    criteria = criteria + [{"Type": "Price", "Weight": proc.parse_weight(ac_price.get("AC_WEIGHTING", 0))}]

                    # QUALITY CRITERIA(S)
                    ac_quality = ac.get("AC_QUALITY", None)
                    if isinstance(ac_quality, dict):
                        criteria = criteria + [{
                                                "Type": "Quality",
                                                "Criterion": ac_quality.get("AC_CRITERION", "-"),
                                                "Criterion (Translation)": "-",
                                                "Weight": proc.parse_weight(ac_quality.get("AC_WEIGHTING", 0))
                        }]
                    elif isinstance(ac_quality, list):
                        criteria = criteria + [{
                                                "Type": "Quality",
                                                "Criterion": q.get("AC_CRITERION", "-"),
                                                "Criterion (Translation)": "-",
                                                "Weight": proc.parse_weight(q.get("AC_WEIGHTING", 0))
                        } for q in ac_quality]


                    # COST CRITERIA(S)
                    ac_cost = ac.get("AC_COST", None)
                    if isinstance(ac_cost, dict):
                        criteria = criteria + [{
                                            "Type": "Cost",
                                            "Criterion": ac_cost.get("AC_CRITERION", "-"),
                                            "Criterion (Translation)": "-",
                                            "Weight": proc.parse_weight(ac_cost.get("AC_WEIGHTING", 0))
                        }]
                    elif isinstance(ac_cost, list):
                        criteria = criteria + [{
                                            "Type": "Cost",
                                            "Criterion": q.get("AC_CRITERION", "-"),
                                            "Criterion (Translation)": "-",
                                            "Weight": proc.parse_weight(q.get("AC_WEIGHTING", 0))} for q in ac_cost]

                except Exception as e:
                    print(f"Error extracting criteria: {e}")
            else:
                print("warning: no criteria")
                criteria = []
        extracted_lots.append({
            "Lot Number": lot.get("LOT_NO", "-"),
            "Title": lot.get("TITLE", "-"),
            "Short Description": lot.get("SHORT_DESCR", "-"),
            "Title (Translated)": "-", #Empty for now, translator later
            "Short Description (Translated)": "-",
            "Criteria": criteria,
            "Main Criterion": proc.get_main_criterion(criteria),
            "CPV Codes": lot.get("CPV_MAIN", {}).get("CPV_CODE", {}).get("@CODE", "-")
        })

    return number_of_lots, extracted_lots

def extract_awarded_contracts(can):
    aw_contracts = can.get("AWARD_CONTRACT", {})
    if isinstance(aw_contracts, dict):  # Handle if contractors is a list or dictionary. I do not know for sure if it can be a list
        aw_contracts = [aw_contracts]
    awards = []
    for aw_contract in aw_contracts:
        awarded_lot = aw_contract.get("AWARDED_CONTRACT", {})
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
            contractor_info = {
                "Name": c_address.get("OFFICIALNAME", "-"),
                "National ID": c_address.get("NATIONALID", "-"),
                "Address": {
                    "Country": c_address.get("COUNTRY", {}).get("@VALUE", "-"),
                    "Town": c_address.get("TOWN", "-"),
                    "Postal Code": c_address.get("POSTAL_CODE", "-"),
                    "Address": c_address.get("ADDRESS", "-"),
                    "Territorial Unit (NUTS3)": c_address.get("n2016:NUTS", {}).get("@CODE", "-")
                },
                "Contact": {
                    "URL": contractor.get("URL", "-"),  # Note: not all contractor objects have URL
                    "Email": contractor.get("E_MAIL", "-"),
                    "Phone": c_address.get("PHONE", "-")
                }
            }

            contractors_info.append(contractor_info)

        aw_info = {
            "Awarded Contract Title": aw_contract.get("TITLE", "-"),
            "Corresponding Lot": aw_contract.get("LOT_NO", "-"),
            "Number of Tenders": awarded_lot.get("TENDERS", {}).get("NB_TENDERS_RECEIVED", "0"),
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

    return [{
        "Name": address.get("OFFICIALNAME", "-"),
        "National ID": address.get("NATIONALID", "-"),
        "Activity": ca_activity,
        "CA Type": contracting_body.get("CA_TYPE", {}).get("@VALUE", "-") or contracting_body.get("CA_TYPE_OTHER", "-"),
        "Address": {
            "Country": address.get("COUNTRY", {}).get("@VALUE", "-"),
            "Town": address.get("TOWN", "-"),
            "Postal Code": address.get("POSTAL_CODE", "-"),
            "Address": address.get("ADDRESS", "-"),
            "Territorial Unit (NUTS3)": address.get("n2016:NUTS", {}).get("@CODE", "-")
        },
        "Contact": {
            "URL": address.get("URL_GENERAL", "-"),
            "Email": address.get("E_MAIL", "-"),
            "Phone": address.get("PHONE", "-")
        }
    }]


# Initialize the OpenSearch client
host = 'localhost'
port = 9200
auth = get_opensearch_auth()

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
index = "procure_v5"
scroll_size = 1000
# Execute the initial search query to get the first batch of results
response = client.search(
    index = "ted-xml",
    body = {"query":   {
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
        if is_doc_processed(client, doc_id):
            continue  # Skip already processed by this pipeline
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

            health_cpv = proc.process_health_cpv(cpv)
            critical_cpv = proc.process_crit_cpv(cpv)


            ca_data = extract_contracting_authority(can)
            if ca_data and isinstance(ca_data, list):
                ca_name = ca_data[0].get("Name", "-")
                ca_country = ca_data[0].get("Address", {}).get("Country", "-")
            else:
                ca_name = "-"
                ca_country = "-"

            number_of_lots, lot_data = extract_lots(can)
            awards_data = extract_awarded_contracts(can)

            try:  ######################################################### Query for CSV data ################################################
                inner_hit = client.get(index="ted-csv", id=doc_id)
                csv_found = True

                value = inner_hit["_source"]["VALUE_EURO_FIN_2"]
                value = process_value(value)

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

                proc_route = proc.calculate_p_route(multiple_country, joint_procurement, central_body, ca_type)
                proc_technique = proc.calculate_p_technique(dynamic_purch, eauction, on_behalf, central_body, fram_agreement, multiple_country)
                health_ca_class = proc.calculate_ca_class(ca_name, ca_country, central_body, ca_type, health_cpv)


            except Exception as e:  ########################################## If CSV not found handler ###########################################
                print(f"An error occurred: {e}")
                csv_found = False
                value = -1  # To obtain value from xml, currency transform is needed.
                proc_route = "Unknown"
                proc_technique = {"Unknown":True}
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
                 country, value, c_nature, proc_route, proc_type, proc_technique, health_ca_class,
                 ca_data, number_of_lots, lot_data, awards_data, tags))

        except Exception as e: ########################################## Error extracting some field from XML ####################################
            print(f"An unexpected error occurred: {e}")
            traceback.print_exc()
            print(hit)

    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description", "Description (Translation)", "Dispatch Date",
                                               "CPV", "CPV Description", "Healthcare CPV", "Critical Services CPV",
                                               "Country", "Value", "Contract Nature", "Procurement Route", "Procurement Type", "Procurement Techniques", "Healthcare Authority Class",
                                               "Contracting Authority", "Number of Lots", "Lots", "Awarded Contracts", "Tags"])


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
        for doc_id in df["Document ID"]:
            log_pipeline_status(client, doc_id)
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
    # Check if there are more results to fetch
    scr = scr + 1
    if len(response["hits"]["hits"]) < scroll_size:
        break


