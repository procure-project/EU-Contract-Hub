import re
import csv
import os
from collections import defaultdict
import json

with open('data/ca_keywords.json', 'r', encoding='utf-8') as f:
    ca_keywords = json.load(f)

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


def import_CPVDict():
    result_dict = {}
    module_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(module_dir, 'CPV_Dictionary.csv')
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader) #Header
        for row in reader:
            first_col = row[0]
            second_col = row[1]
            cropped_value = first_col[:8] # Verify if it's 8 digits
            if cropped_value.isdigit() and len(cropped_value) == 8:
                result_dict[cropped_value] = second_col
            else:
                print(f"Skipping invalid entry: {first_col}")
    return result_dict

def cpv_match(categories, cpv_list):
    category_prefixes = [str(i).zfill(8).rstrip('0') for i in categories]
    if isinstance(cpv_list, int):# If cpv_list is a single integer, convert it to a list
        cpv_list = [cpv_list]
    return any(any(str(cpv).zfill(8).startswith(prefix) for prefix in category_prefixes) for cpv in cpv_list)

def process_value(value):
    """Process the 'Value' field to filter high and low values."""
    return value if (value > 100) and (value < 10 ** 10) else -1

def process_health_cpv(cpv):
    """Check if the CPV code matches any healthcare CPV."""
    return cpv_match(HEALTHCARE_CPV, cpv) if cpv is not None else False

def process_crit_cpv(cpv):
    """Check if the CPV code matches any critical services CPV."""
    return cpv_match(CRITICAL_CPV, cpv) if cpv is not None and cpv_match(HEALTHCARE_CPV, cpv) else False


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


def get_main_criterion(criteria):
    type_weights = defaultdict(float)
    if not type_weights:
        return "-"
    for crit in criteria:
        type_weights[crit["Type"]] += crit["Weight"]
    max_type = max(type_weights, key=type_weights.get)
    return max_type if max_type else "-"




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

def rule_classify(name, keywords):
    """
    Receives a list of regex patterns (keywords) and matches a single name against it.
    Returns 1 if any keyword matches, else 0.
    """
    if keywords:
        patterns = r"(" + r"|".join(keywords) + r")"
        combined_pattern = re.compile(patterns, re.IGNORECASE)
        # Convert to string and handle NaN/None values
        if name is None or (hasattr(name, 'isna') and name.isna()):
            return False
        name_str = str(name)
        return True if combined_pattern.search(name_str) else False
    else:
        return False
    
def calculate_ca_class(name, country, central_body, ca_type, health_cpv):
    if central_body:
        if ca_type == "1":
            return 'Government Public Procurers'
        elif ca_type == "3" or ca_type == "R":
            return 'Regional or Local Public Purchasing Bodies'
        else:
            return 'Central Public Purchasing Bodies'
    else:
        if health_cpv:
            hospital_keywords = ca_keywords['whitelist_hospital'].get(country, [])
            if(name, hospital_keywords):
                university_hospital_keywords = ca_keywords['whitelist_university_hospital'].get(country, [])
                if(name, university_hospital_keywords):
                    return 'University Hospital'
                else:
                    return 'Hospital'
            else:
                return 'Other Healthcare Direct Procurer'
        else:
            return 'Non-Healthcare Direct Procurer'


