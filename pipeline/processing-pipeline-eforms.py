from pipelinepackage import processingmodule as proc
from opensearchpy import OpenSearch, helpers
import pandas as pd
import traceback
import getpass
from datetime import datetime
import json

def get_organization_data(id, all_organizations):
    try:
        organization = {}
        if isinstance(all_organizations, dict):
            all_organizations = [all_organizations]
        for org in all_organizations:
            if org["efac:Company"]["cac:PartyIdentification"]["cbc:ID"] == id:
                organization = org["efac:Company"]

        if organization == {}:
            return {}

        else:
            name = organization.get("cac:PartyName", {}) #Apparently there can be multiple names
            natid = organization.get("cac:PartyLegalEntity", {}) #And IDs
            if isinstance(name, list):
                name = name[0]
            if isinstance(natid, list):
                natid = natid[0]
            return {
                "Name": name.get("cbc:Name", "-"),
                "National ID": natid.get("cbc:CompanyID", -1),
                "Address": {
                    "Country": organization.get("cac:PostalAddress", {}).get("cac:Country", {}).get(
                        "cbc:IdentificationCode", "-"),
                    "Town": organization.get("cac:PostalAddress", {}).get("cbc:CityName", "-"),
                    "Postal Code": organization.get("cac:PostalAddress", {}).get("cbc:PostalZone", "-"),
                    "Address": organization.get("cac:PostalAddress", {}).get("cbc:StreetName", "-"),
                    "Territorial Unit (NUTS3)": organization.get("cac:PostalAddress", {}).get(
                        "cbc:CountrySubentityCode", "-")
                },
                "Contact": {
                    "URL": organization.get("cbc:WebsiteURI", "-"),
                    "Email": organization.get("cac:Contact", {}).get("cbc:Telephone", "-"),
                    "Phone": organization.get("cac:Contact", {}).get("cbc:ElectronicMail", "-")
                }
            }
    except KeyError:
        return {}


def extract_contracting_authority(cparty, organizations):
    cparty_id = cparty.get("cac:PartyIdentification", {}).get("cbc:ID", "-")
    cparty_data = get_organization_data(cparty_id, organizations)
    cparty_data["Activity"] = cparty.get("cac:ContractingActivity",{}).get("cbc:ActivityTypeCode","-")
    cparty_types = cparty.get("cac:ContractingActivity",[])
    if isinstance(cparty_types, dict):
        cparty_types = [cparty_types]
    cparty_data["CA Type"] = [type.get("cbc:PartyTypeCode","-") for type in cparty_types]
    return cparty_data


def extract_lots(lots):
    extracted_lots = []
    if isinstance(lots, dict):
        lots = [lots]
    number_of_lots = len(lots)
    for lot in lots:
        # Extract the criteria and their weights
        lot_project = lot.get("cac:ProcurementProject", {})
        ac_list = lot.get("cac:TenderingTerms", {}).get("cac:AwardingTerms", {}).get("cac:AwardingCriterion", [])
        if isinstance(ac_list, dict):
            ac_list = [ac_list]
        sac = []
        for ac in ac_list:
            subcriteria = ac.get("cac:SubordinateAwardingCriterion", [])
            if isinstance(subcriteria, dict):
                subcriteria = [subcriteria]
            sac = sac + subcriteria
        criteria_list = []
        for ac in sac:
            if ac:
                try:
                    #PRICE CRITERIA
                    criteria_type = ac.get("cbc:AwardingCriterionTypeCode", "-").capitalize()
                    acparam = (ac.get("ext:UBLExtensions", {}).get("ext:UBLExtension",{}).get("ext:ExtensionContent",{}).get("efext:EformsExtension",{}).get("efac:AwardCriterionParameter",[]))
                    if isinstance(acparam, list):
                        acparam = acparam[0]
                    criteria_weight = acparam.get("efbc:ParameterNumeric",-1)
                    try:
                        criteria_weight = float(criteria_weight)/100
                    except ValueError:
                        criteria_weight = -1.0
                    criteria = {"Type": criteria_type,
                                "Weight": criteria_weight}

                    criteria_list.append(criteria)
                except Exception as e:
                    print(f"Error extracting criteria: {e}")
                    print(ac)
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





def extract_awarded_contracts(result):
    all_lot_results = result.get("efac:LotResult",[])
    if isinstance(all_lot_results, dict):
        all_lot_results = [all_lot_results]

    all_settled_contracts = result.get("efac:SettledContract",[])
    if isinstance(all_settled_contracts, dict):
        all_settled_contracts = [all_settled_contracts]

    all_lot_tenders = result.get("efac:LotTender", [])
    if isinstance(all_lot_tenders, dict):
        all_lot_tenders = [all_lot_tenders]

    all_tendering_parties = result.get("efac:TenderingParty", [])
    if isinstance(all_tendering_parties, dict):
        all_tendering_parties = [all_tendering_parties]

    all_organizations = result.get("efac:Organizations", {}).get("efac:Organization",[])
    if isinstance(all_organizations, dict):
        all_organizations = [all_organizations]

    awards = []
    date_conclusion = None
    aw_title = "-"
    for lot_result in all_lot_results:
        contractors_info = []
        contractid =  lot_result.get("efac:SettledContract",{})
        if isinstance(contractid, list): # apparently a lot result can have multiple associated contracts, what do they mean I do not know. Documentation is completely lacking so I will ignore these results.
            contractid = contractid[0]
        sett_contract = [contract for contract in all_settled_contracts if contract["cbc:ID"] ==contractid.get("cbc:ID")]
        if sett_contract:
            sett_contract = sett_contract[0]
            lot_tenders = sett_contract.get("efac:LotTender", {})
            aw_title = sett_contract.get("cbc:Title", "-")
            date_conclusion = sett_contract.get("cbc:IssueDate", None)
            try:
                if date_conclusion is not None:
                    date_conclusion = datetime.strptime(date_conclusion, "%Y-%m-%d%z")
            except ValueError:
                date_conclusion = None  # Handle parsing errors
        else: #Alternative route, there may not be settled contracts in the extensions but the link is made through LotTender directly
            lot_tenders = lot_result.get("efac:LotTender",{})

        if lot_tenders: #This in reality should take the tenderresultcode instead!!
            if isinstance(lot_tenders, dict):
                lot_tenders = [lot_tenders]
            for lot_tender in lot_tenders:
                lot_tender = [lt for lt in all_lot_tenders if lt["cbc:ID"] == lot_tender["cbc:ID"]]
                lot_tender = lot_tender[0] if lot_tender else {}
                tendering_party = [tpa for tpa in all_tendering_parties if tpa["cbc:ID"] == lot_tender["efac:TenderingParty"]["cbc:ID"]]
                tendering_party = tendering_party[0] if tendering_party else {}

                org_list = tendering_party.get("efac:Tenderer",[])
                if isinstance(org_list, dict):
                    org_list = [org_list]
                for org in org_list:
                    contractors_info.append(get_organization_data(org.get("cbc:ID",-1), all_organizations))
        statistics = lot_result.get("efac:ReceivedSubmissionsStatistics", [])
        if statistics:
            if isinstance(statistics, dict):
                number_of_tenders = statistics.get("efbc:StatisticsNumeric",-1) #Apparently there can be a dict instead of a list and then there is no code, just default tender number
            else:
                if all("efbc:StatisticsCode" in stat.keys() for stat in statistics):
                    stat_tenders = [stat for stat in statistics if stat["efbc:StatisticsCode"] == "tenders"]
                    if stat_tenders:
                        stat_tenders = stat_tenders[-1]
                        number_of_tenders = stat_tenders.get("efbc:StatisticsNumeric",-1)
                    else:
                        number_of_tenders = -1
                else: #Yes, there may be a list with multiple and contradicting entries, AND unlabelled. I will get the latest entry.
                    number_of_tenders = statistics[-1].get("efbc:StatisticsNumeric",-1)
        else:
            number_of_tenders = -1


        aw_info = {
            "Awarded Contract Title": aw_title,
            "Corresponding Lot": lot_result.get("efac:TenderLot", {}).get("cbc:ID", "-"),
            "Number of Tenders": number_of_tenders,
            "Contractors": contractors_info,
            "Conclusion Date": date_conclusion
        }
        awards.append(aw_info)
    return awards








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
CPV_dict = proc.import_CPVDict()
while True:
    # Continue scrolling
    response = client.scroll(scroll_id=scroll_id, scroll="1m")
    id_field_pairs = []

    # Extract document IDs and corresponding field values from the current batch of results
    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
        doc_id = hit["_id"]

        try:
            project = hit["_source"]["cac:ProcurementProject"]
            lots = hit["_source"].get("cac:ProcurementProjectLot",{})
            result = hit["_source"].get("ext:UBLExtensions", {}).get("ext:UBLExtension").get("ext:ExtensionContent").get("efext:EformsExtension").get("efac:NoticeResult",{})
            cparties = hit["_source"].get("cac:ContractingParty", {})


            organizations = hit["_source"]["ext:UBLExtensions"]["ext:UBLExtension"]["ext:ExtensionContent"]["efext:EformsExtension"]["efac:Organizations"]["efac:Organization"]

            value_eforms = result.get("cbc:TotalAmount",-1)
            title = project.get("cbc:Name", "-")
            description = project.get("cbc:Description", "-")
            locations = project.get("cac:RealizedLocation", {})
            if isinstance(locations, list):
                country = []
                for loc in locations:
                    country.append(loc.get("cac:Address", {}).get("cac:Country", {}).get("cbc:IdentificationCode", "-"))
            else:
                country = locations.get("cac:Address", {}).get("cac:Country", {}).get("cbc:IdentificationCode", "-")
            cpv = project.get("cac:MainCommodityClassification", {}).get("cbc:ItemClassificationCode", -1)
            cpv_desc = CPV_dict.get(cpv, "-")
            add_cpv = project.get("cac:AdditionalCommodityClassification", None)
            if add_cpv:
                if isinstance(add_cpv, dict):
                    add_cpv = [add_cpv]
                for cpv_i in add_cpv:
                    new_cpv = cpv_i["cbc:ItemClassificationCode"]
                    cpv = [cpv].append(new_cpv)
                    cpv_desc = [cpv_desc].append(CPV_dict.get(new_cpv, "-"))
            c_nature = project.get("cbc:ProcurementTypeCode", "Unknown")
            proc_type = project.get("cac:TenderingProcess", {}).get("cbc:ProcedureCode", "Unknown")
            date_dispatch = project.get("cbc:IssueDate", None)
            try:
                if date_dispatch is not None:
                    date_dispatch = datetime.strptime(date_dispatch, "%Y-%m-%d%z")
            except ValueError:
                date_dispatch = None  # Handle parsing errors

            health_cpv = proc.process_health_cpv(cpv)
            critical_cpv = proc.process_health_cpv(cpv)

            if isinstance(cparties, list):
                ca_data = []
                for ca in cparties:
                    ca_data.append(extract_contracting_authority(ca.get("cac:Party", {}), organizations))
            else:
                ca_data = extract_contracting_authority(cparties.get("cac:Party", {}), organizations)
            number_of_lots, lot_data = extract_lots(lots)
            awards_data = extract_awarded_contracts(result)

            try:  ######################################################### Query for CSV data ################################################
                inner_hit = client.get(index="ted-csv", id=doc_id)
                csv_found = True

                value = inner_hit["_source"]["VALUE_EURO_FIN_2"]
                value = proc.process_value(value)

                multiple_country = inner_hit["_source"]["B_MULTIPLE_COUNTRY"]
                central_body = inner_hit["_source"]["B_AWARDED_BY_CENTRAL_BODY"]
                joint_procurement = inner_hit["_source"]["B_INVOLVES_JOINT_PROCUREMENT"]
                dynamic_purch = inner_hit["_source"]["B_DYN_PURCH_SYST"]
                eauction = inner_hit["_source"]["B_ELECTRONIC_AUCTION"]
                on_behalf = inner_hit["_source"]["B_ON_BEHALF"]
                ca_type = inner_hit["_source"]["CAE_TYPE"]

                fram_agreement = inner_hit["_source"].get("B_FRA_AGREEMENT", False)
                fram_estimated = inner_hit["_source"].get("FRA_ESTIMATED")
                if fram_estimated and isinstance(fram_estimated, str):
                    if 'K' in fram_estimated or 'C' in fram_estimated:
                        fram_agreement = True # K for when the keyword framework was detected in the description, C for consistency, previous notices were indicated as framework agreements. A third option has not been considered, A for multiple awards per lot, which may correspond with fram. agreements, dynamic purch. systems or innovation partnerships

                proc_route = proc.calculate_p_route(multiple_country, joint_procurement, central_body, ca_type)
                proc_technique = proc.calculate_p_technique(dynamic_purch, eauction, on_behalf, central_body, fram_agreement, multiple_country)
                health_ca_class = proc.calculate_ca_class(central_body,ca_type,health_cpv)


            except Exception as e:  ########################################## If CSV not found handler ###########################################
                #print(f"An error occurred: {e}")
                csv_found = False
                try:
                    value = float(value_eforms)
                    value = proc.process_value(value)
                except ValueError:
                    value = -1
                proc_route = "Unknown"
                proc_technique = {"Unknown":True}
                health_ca_class = "Unknown"

            title_translated = "-"  # No translation for now (too slow)
            description_translated = "-"

            sources = {"TED-EForms": True}
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
            print(json.dumps(hit, indent=4))

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
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
    # Check if there are more results to fetch
    scr = scr + 1
    if len(response["hits"]["hits"]) < scroll_size:
        break