from pipelinepackage import processingmodule as proc
from opensearchpy import OpenSearch, helpers
import pandas as pd
import traceback
import getpass
from datetime import datetime

def get_organization_data(id, extensions):
    try:
        organization = {}
        all_organizations = extensions["ext:UBLExtension"]["ext:ExtensionContent"]["efext:EformsExtension"]["efac:Organizations"]["efac:Organization"]
        for org in all_organizations:
            if org["efac:Company"]["cac:PartyIdentification"]["cbc:ID"] == id:
                organization = org["efac:Company"]

        if organization == {}:
            return None
    except KeyError:
        return None
    return {
        "Name": organization.get("cac:PartyName",{}).get("cbc:Name","-"),
        "National ID": organization.get("cac:PartyLegalEntity",{}).get("cbc:CompanyID",-1),
        "Address": {
            "Country": organization.get("cac:PostalAddress",{}).get("cac:Country",{}).get("cbc:IdentificationCode","-"),
            "Town": organization.get("cac:PostalAddress",{}).get("cbc:CityName","-"),
            "Postal Code": organization.get("cac:PostalAddress",{}).get("cbc:PostalZone","-"),
            "Address": organization.get("cac:PostalAddress",{}).get("cbc:StreetName","-"),
            "Territorial Unit (NUTS3)": organization.get("cac:PostalAddress",{}).get("cbc:CountrySubentityCode","-")
        },
        "Contact": {
            "URL": organization.get("cbc:WebsiteURI","-"),
            "Email": organization.get("cac:Contact",{}).get("cbc:Telephone","-"),
            "Phone": organization.get("cac:Contact",{}).get("cbc:ElectronicMail","-")
        }
    }

def extract_contracting_authority(source_data):
    cparty_id = source_data["cac:ProcurementProject"].get("cac:ContractingParty", {}).get("cac:Party", {}).get("cac:PartyIdentification", {}).get(
        "cbc:ID", "-")
    cparty_data = get_organization_data(cparty_id, source_data["ext:UBLExtensions"])
    cparty_data["Activity"] = TO-DO
    cparty_data["CA Type"] = TO-DO
    return cparty_data


def extract_lots(lots):
    extracted_lots = []
    if isinstance(lots, dict):
        lots=[lots]
    number_of_lots = len(lots)
    for lot in lots:
        # Extract the criteria and their weightings
        lot_project = lot.get("cac:ProcurementProject", {})
        ac_list = lot_project.get("cac:TenderingTerms", {}).get("cac:AwardingTerms", {}).get("cac:AwardingCriterion", {}).get("cac:SubordinateAwardingCriterion", [])
        for ac in ac_list:
            if ac:
                criteria = []
                try:
                    #PRICE CRITERIA
                    criteria_type= ac.get("cbc:AwardingCriterionTypeCode", "-")
                    criteria[criteria_name] =

                    ac_price = ac.get("ext:UBLExtensions", {}).get("ext:UBLExtension").get("ext:ExtensionContent").get("efext:EformsExtension")
                    if isinstance(ac_price, dict):
                        criteria = {"Price": {"Criterion": "Price", "Weight": proc.parse_weight(ac_price.get("AC_WEIGHTING", 0))}}
                    else:
                        criteria = {"Price": {"Criterion": "Price", "Weight": 0}}

                    criteria_list.append(criteria)
                except Exception as e:
                    print(f"Error extracting criteria: {e}")
                    criteria_list.append({})
            else:
                print("warning: no criteria")
                criteria_list = []
        extracted_lots.append({
            "Lot Number": lot.get("cbc:ID", "-"),
            "Title": lot_project.get("cbc:Name", "-"),
            "Short Description": lot_project.get("cbc:Description", "-"),
            "Title (Translated)": "-", #Empty for now, translator later
            "Short Description (Translated)": "-",
            "Criteria": criteria_list,
            "Main Criterion": proc.get_main_criterion(criteria_list),
            "CPV Codes": lot_project.get("cac:MainCommodityClassification", {}).get("cbc:ItemClassificationCode", -1)
        })

    return number_of_lots, extracted_lots

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
    index = "ted-eforms",
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

        try:
            project = hit["_source"]["cac:ProcurementProject"]
            lots = hit["_source"].get("cac:ProcurementProjectLots",{})
            title = project.get("cbc:Name", "-")
            description = project.get("cbc:Description", "-")
            country = project.get("cac:RealizedLocation", {}).get("cac:Address", {}).get("cac:Country", {}).get("cbc:IdentificationCode", "-")
            cpv = project.get("cac:MainCommodityClassification", {}).get("cbc:ItemClassificationCode", -1)
            add_cpv = project.get("cac:AdditionalCommodityClassification", {}).get("cbc:ItemClassificationCode", -1)
            if add_cpv != -1:
                cpv = [cpv].append(add_cpv)
            cpv_desc = TO-DO - Correspondence of code - description
            ca_type = project.get("cac:ContractingPartyType", {}).get("cbc:PartyTypeCode", "Unknown") TO-DO - WE MUST TRANSFORM THE TYPES!
            c_nature = project.get("cbc:ProcurementTypeCode", "Unknown")
            proc_type = project.get("cac:TenderingProcess", {}).get("cbc:ProcedureCode", "Unknown")
            date_dispatch = project.get("cbc:IssueDate", None)
            try:
                if date_dispatch is not None:
                    date_dispatch = datetime.strptime(date_dispatch, "%Y-%m-%d%z")
            except ValueError:
                date_dispatch = None  # Handle parsing errors gracefully

            health_cpv = False
            critical_cpv = False



            ca_data = extract_contracting_authority(project)
            number_of_lots, lot_data = extract_lots(lots)
            awards_data = extract_awarded_contracts(can)

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

                proc_route = proc.calculate_p_route(multiple_country, joint_procurement, central_body, ca_type)
                proc_technique = proc.calculate_p_technique(dynamic_purch, eauction, on_behalf, central_body, fram_agreement, multiple_country)
                health_ca_class = proc.calculate_ca_class(central_body,ca_type,health_cpv)


            except Exception as e:  ########################################## If CSV not found handler ###########################################
                print(f"An error occurred: {e}")
                csv_found = False
                value = -1  # To obtain value from xml, currency transform is needed.
                proc_route = "Unknown"
                proc_technique = {"Unknown":True}
                health_ca_class = "Unknown"

            title_translated = "-"  # No translation for now (too slow)
            description_translated = "-"

            sources = {"TED-EFORMS": True}
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

    proc.processing_scroll(df)

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