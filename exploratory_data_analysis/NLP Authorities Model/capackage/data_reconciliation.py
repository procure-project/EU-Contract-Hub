from SPARQLWrapper import SPARQLWrapper, JSON
from geopy.geocoders import Nominatim
import pandas as pd
import requests
import ast
import time
from difflib import SequenceMatcher
import re
import folium
from folium.plugins import MarkerCluster
from tqdm import tqdm



def load_opensearch_data(file):
    df_opensearch = pd.read_csv(file)
    df_opensearch['Contracting Authorities'] = df_opensearch['Contracting Authorities'].apply(ast.literal_eval)
    df_opensearch = df_opensearch.explode('Contracting Authorities') #This column is a list of participating authorities,  splits each into its own row
    authorities_df = pd.json_normalize(df_opensearch['Contracting Authorities']) # Extract data from the 'Contracting Authorities' dictionary
    authorities_df['Document ID'] = df_opensearch['Document ID'].values
    authorities_df['Document Year'] = authorities_df['Document ID'].apply(lambda x: int(x.split('-')[1]))
    os_df = authorities_df.drop_duplicates(subset=['Address.Country', 'National ID'])
    return os_df

def load_sparql_data():
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

    # Define the SPARQL query to fetch data about cities and municipalities in Spain
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>

    SELECT ?instance ?instanceLabel ?class ?classLabel ?country ?countryLabel ?location WHERE {
      ?instance wdt:P31 wd:Q16917.   # Instance of building (Q16917) or its subclasses
      ?instance wdt:P31 ?class .     # Get the class of the instance

      ?instance wdt:P17 ?country .   # The instance is located in a country

      OPTIONAL { ?instance wdt:P625 ?location. }  # Get location coordinates if available

      VALUES ?country { 
        wd:Q29 wd:Q45 wd:Q142 wd:Q233 wd:Q41 wd:Q38 wd:Q183 wd:Q31 wd:Q55 
        wd:Q34 wd:Q20 wd:Q33 wd:Q211 wd:Q191 wd:Q37 wd:Q36 wd:Q28 wd:Q218 
        wd:Q214 wd:Q213 wd:Q215
      }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }  # Get labels in English
    }

    """

    # Set the query and request JSON results
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    # Execute the query and fetch results
    results = sparql.query().convert()

    # Process the results into a list of dictionaries
    wikidata_data = []
    for result in results["results"]["bindings"]:
        coordinates_str = result["location"]["value"] if "location" in result else None
        if coordinates_str:
            match = re.match(r"Point\(([-\d\.]+) ([\d\.]+)\)", coordinates_str)
            if match:
                latitude = float(match.group(2))
                longitude = float(match.group(1))
            else:
                latitude = None
                longitude = None
        else:
            latitude = None
            longitude = None

        hospital = {
            "name": result["instanceLabel"]["value"],
            "class": result["classLabel"]["value"] if "classLabel" in result else None,
            "country": result["countryLabel"]["value"] if "countryLabel" in result else None,
            "latitude": latitude,
            "longitude": longitude
        }
        wikidata_data.append(hospital)

    # Convert the list of dictionaries to a pandas DataFrame
    df_wikidata = pd.DataFrame(wikidata_data)
    return df_wikidata


def clean_wd_data(wd_df):
    # Define classification tags for university hospitals and types posing disqualification
    university_hospital_tags = ['university', 'research', 'teaching', 'academic', 'educational']
    former_hospital_tags = ['former hospital', 'destroyed building or structure', 'museum', 'ruins']

    # Group by latitude and longitude, ensuring specific classes have priority
    wd_df = wd_df.groupby(['latitude', 'longitude'], as_index=False).agg({
        'class': lambda x: ', '.join(x),
        'name': 'first',  # or use another aggregation method if needed
        'country': 'first',  # or use another aggregation method if needed
    })

    def classify(cls_list):
        if any(tag in cls_list for tag in university_hospital_tags):
            return 'university hospital'
        elif any(tag in cls_list for tag in former_hospital_tags):
            return 'disqualified'  # Mark for potential removal later
        else:
            return 'hospital'

    wd_df['category'] = wd_df['class'].apply(lambda x: classify(x))

    # Return non-disqualified entries
    wd_df = wd_df[wd_df['category'] != 'disqualified']
    wd_df = wd_df[~wd_df['name'].str.match(r'Q\d+')]

    return wd_df


def load_osm_data():
    def query_hospitals_in_country(country):
        # Overpass API query to find hospitals in the specified country
        overpass_url = "http://overpass-api.de/api/interpreter"

        overpass_query = f"""
        [out:json];
        area["ISO3166-1"="{country}"];
        (
          node["amenity"="hospital"](area);
          way["amenity"="hospital"](area);
          relation["amenity"="hospital"](area);
        );
        out center;
        """

        # Sending the request to the Overpass API
        response = requests.post(overpass_url, data={'data': overpass_query})

        if response.status_code == 200:
            # Load the response as JSON
            hospitals_data = response.json()

            # Create a list to store hospital information
            hospitals = []
            for element in hospitals_data['elements']:
                hospital_name = element.get('tags', {}).get('name', 'Unnamed Hospital')

                if 'lat' in element and 'lon' in element:
                    hospitals.append({
                        'name': hospital_name,
                        'latitude': element['lat'],
                        'longitude': element['lon'],
                        'country': country
                    })
            return pd.DataFrame(hospitals)
        else:
            print(f"Error for country {country}: {response.status_code}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    all_hospitals_df = pd.DataFrame()
    countries = ['ES', 'PT', 'FR', 'MT', 'GR', 'IT', 'DE', 'BE', 'NL', 'SE',
                 'NO', 'FI', 'LV', 'EE', 'LT', 'PL', 'HU', 'RO', 'SK', 'CZ', 'SI']
    for country in countries:
        country_hospitals_df = query_hospitals_in_country(country)
        all_hospitals_df = pd.concat([all_hospitals_df, country_hospitals_df], ignore_index=True)
    return all_hospitals_df


def get_coords_request(address_list):
    time.sleep(1)
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'postalcode': address_list['Postal Code'],
        'countrycodes': address_list['Country'],
        'format': 'json',
        'limit': 1
    }
    response = requests.get(url, params=params)

    if response.status_code == 200 and response.json():
        data = response.json()[0]  # Get the first result
        latitude = data['lat']
        longitude = data['lon']
        return latitude, longitude
    else:
        return None, None


def get_coords_api(address_list):
    geolocator = Nominatim(user_agent="myGeocoder")
    time.sleep(1)
    # Concatenate components into a full address string
    full_address = f"{address_list['Country']}, {address_list['Town']},{address_list['Postal Code']}, {address_list['Address']}"
    print(full_address)
    # Geocode the address
    try:
        location = geolocator.geocode(full_address)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except Exception as e:
        return None, None

def compute_edit_distance(row, df_wikidata):
    best_match = None
    best_ratio = 0
    for wikidata_row in df_wikidata.iterrows():
        ratio = SequenceMatcher(None, row['Name'], wikidata_row['name']).ratio()
        if ratio > best_ratio:
            best_match = wikidata_row['name']
            best_ratio = ratio
    return best_match, best_ratio


def coordinates_to_map(names, latitudes, longitudes, color = 'blue', map_obj = None):
    if map_obj is None:
        map_obj = folium.Map(location=[50, 10], zoom_start=4)  # Centered around Central Europe
    marker_cluster = MarkerCluster(options={
        'spiderfyOnMaxZoom': False,  # Do not spiderfy markers on max zoom
        'disableClusteringAtZoom': 15,  # Disable clustering at zoom level 18 (adjust as needed)
        'maxClusterRadius': 50,  # Radius for clustering (adjust as needed)
        'chunkedLoading': True
    }).add_to(map_obj)
    filtered_data = [(name, lat, lon) for name, lat, lon in zip(names, latitudes, longitudes)
                     if not (pd.isna(name) or pd.isna(lat) or pd.isna(lon))]
    for name, lat, lon in filtered_data:
        folium.Marker(
            location=[lat, lon],
            popup=name,
            icon=folium.Icon(color=color)
        ).add_to(marker_cluster)

    return map_obj

