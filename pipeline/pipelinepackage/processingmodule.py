import re


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

def calculate_ca_class(central_body,ca_type,health_cpv):
    if central_body:
        if ca_type == "1":
            return 'Government Public Procurers'
        elif ca_type == "3" or ca_type == "R":
            return 'Regional or Local Public Purchasing Bodies'
        else:
            return 'Central Public Purchasing Bodies'
    else:
        if health_cpv:
            return 'Healthcare Direct Procurer'
        else:
            return 'Non-Healthcare Direct Procurer'