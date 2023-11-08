import os
import requests
import csv
import re
from datetime import datetime
from bs4 import BeautifulSoup
import shutil

# Replace with your Google Cloud API Key
api_key = 'AIzaSyDL9WZl7UQwCvryIlA1T_M-i1vUnNm3tww'
# Global variable to determine behavior
breakintotimezone = True  # If True, use timezone-based files; if False, use city-timezone-based files.
radius = 50000
place_type = 'plumber'
max_reviews = 30
max_results = 100
endpoint = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
page_token = None  # Initialize a page token
data_folder = 'data'

def ensure_data_folder_exists():
    # Check if the "data" folder exists, and create it if not

    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

def make_request(url, params=None, max_retries=3):
    for _ in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response
            else:
                print(f"Error: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
    return None     

def find_email(website_url):
    # Check if the website_url is empty, None, or 'N/A'
    if not website_url or website_url == 'N/A':
        return 'N/A'

    response = make_request(website_url, timeout=10)
    
    if response and response.ok:
        # Use regular expressions to look for email addresses
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', response.text)

        # Return the first email found, if any
        if emails:
            return emails[0]

    return 'N/A'  # Default if no email found or in case of an error

# Function to fetch and write places to a CSV file
def fetch_and_write_places(writer, city, state, latitude, longitude, timezone, radius, place_type, max_reviews, api_key, processed_count):
    endpoint = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
    page_token = None

    while True:
        params = {
            'location': f'{latitude},{longitude}',
            'radius': radius,
            'type': place_type,
            'key': api_key
        }
        if page_token:
            params['pagetoken'] = page_token

        response = make_request(endpoint, params=params)
        if response and response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            page_token = data.get('next_page_token', None)

            for result in results:
                if result.get('business_status') == 'OPERATIONAL' and result.get('user_ratings_total', 0) < max_reviews:
                    place_id = result.get('place_id', 'N/A')

                    details_url = f'https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,formatted_address,international_phone_number,website&key={api_key}'
                    details_response = make_request(details_url)

                    if details_response and details_response.status_code == 200:
                        details_data = details_response.json()
                        details = details_data.get('result', {})

                        business_name = details.get('name', 'N/A')
                        address = details.get('formatted_address', 'N/A')
                        phone_number = details.get('international_phone_number', 'N/A')
                        website = details.get('website', 'N/A')
                        email = 'N/A'  # Default value, extract email if website exists
                        
                        if website:
                            try:
                                site_response = requests.get(website)
                                soup = BeautifulSoup(site_response.text, 'html.parser')
                                mailtos = soup.select('a[href^=mailto]')
                                for i in mailtos:
                                    href = i['href']
                                    try:
                                        str1, str2 = href.split(':')
                                    except ValueError:
                                        break
                                    email = str2
                            except requests.exceptions.RequestException as e:
                                print(f"Failed to fetch website: {e}")

                        writer.writerow({
                            'Business Name': business_name,
                            'Address': address,
                            'Phone Number': phone_number,
                            'Website': website,
                            'Email': email,
                            'Number of Reviews': result.get('user_ratings_total', 'N/A'),
                            'Timezone': timezone
                        })
                        processed_count += 1

            if not page_token or processed_count >= max_results:
                break  # No more pages to fetch, or we've reached the maximum results
        else:
            break  # Handle any errors or lack of response

    return processed_count

def get_places_in_area(city, state, latitude, longitude, timezone, processed_count):

    # Create the API request URL
    url = f'{endpoint}?location={latitude},{longitude}&radius={radius}&type={place_type}&key={api_key}'

    # Get the current time and format it as "HH_mm_a" (e.g., 7_41_PM)
    current_time = datetime.now().strftime('%I_%M_%p')
    data_folder = 'data'
    output_file_base = f'{timezone}_{place_type}_in_{city}_{state}_{current_time}'

    output_file_name = os.path.join(data_folder, f'{output_file_base}_({processed_count}).csv')

    # Create a CSV file to write the data
    with open(output_file_name, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['Business Name', 'Address', 'Phone Number', 'Website', 'Email', 'Number of Reviews', 'Timezone']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

  
        processed_count = fetch_and_write_places(writer, city, state, latitude, longitude, timezone, radius, place_type, max_reviews, api_key, processed_count)

    # After all data has been written, rename the file if needed
    final_output_file_name = f'data/{timezone}_{place_type}_{processed_count}.csv'
    shutil.move(output_file_name, final_output_file_name)

    # Return the processed count
    return processed_count


    # # Get the current time for filename
    # current_time = datetime.now().strftime('%I_%M_%p')
    # data_folder = 'data'
    # processed_count = 0

    # output_file_name = os.path.join(data_folder, f'{timezone}_{place_type}_{current_time}_{processed_count}.csv')

    # # Check if we need to write headers
    # write_headers = not os.path.exists(output_file_name)

    # with open(output_file_name, mode='a', newline='', encoding='utf-8') as csv_file:
    #     fieldnames = ['Business Name', 'Address', 'Phone Number', 'Website', 'Email', 'Number of Reviews', 'City', 'State', 'Timezone']
    #     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
    #     if write_headers:
    #         writer.writeheader()

    #     for city_info in cities:
    #         processed_count = fetch_and_write_places(writer, city_info['City'], city_info['State'], city_info['Latitude'], city_info['Longitude'], timezone, radius, place_type, max_reviews, api_key, processed_count) 


    # with open(output_file_name, mode='a' , newline='', encoding='utf-8') as csv_file:
    #     csv_reader = csv.DictReader(csv_file)
    #     for row in csv_reader:
    #         # Skip lines that start with #
    #         if not row['City'].strip().startswith('#'):
    #             city = row['City']
    #             state = row['State']
    #             latitude = row['Latitude']
    #             longitude = row['Longitude']
    #             timezone = row['Timezone']

    #             processed_count = 0  # Initialize the processed count for each location
    #             processed_count = get_places_in_area(city, state, latitude, longitude, timezone, processed_count)

def get_places_in_timezone(timezone, locations, writer):
    # Generate the timestamp for the filename
    timestamp = datetime.now().strftime("%I_%M_%p").lower()
    # Count the number of processed locations
    processed_count = len(locations)

    # Create the filename using the timezone, placetype, timestamp, and processed_count
    filename = f"{timezone}_{place_type}_{timestamp}_({processed_count}).csv"

    # Now open the file with the generated filename
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ['Business Name', 'Address', 'Phone Number', 'Website', 'Email', 'Number of Reviews', 'Timezone']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        # Here, you would write the locations to the file
        # Since we don't have the implementation details of how locations are processed
        # and written, I'll leave this part for you to fill in.
        for location in locations:
            # You will have to implement the logic to get the place details here
            # place_details = get_place_details(location)
            # writer.writerow(place_details)
            pass

def get_place_details(place_id):
    # Function to get detailed place information
    details_url = f'https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,formatted_address,international_phone_number,website&key={api_key}'
    response = make_request(details_url)
    if response and response.status_code == 200:
        details_data = response.json()
        return details_data.get('result', {})
    else:
        handle_error(response)
        return None

def handle_error(response):
    if response:
        print(f'Error: {response.status_code} - {response.text}')
    else:
        print('Error: Unable to get a valid response.')


    # Initialize a dictionary to keep track of processed counts by timezone
    processed_counts = {}

    with open(input_file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            # Skip lines that start with #
            if row['City'].strip().startswith('#'):
                continue

            city = row['City']
            state = row['State']
            latitude = row['Latitude']
            longitude = row['Longitude']
            timezone = row['Timezone']

            # Initialize the processed count for the timezone if it hasn't been already
            if timezone not in processed_counts:
                processed_counts[timezone] = 0

            # Get the current processed count for this timezone
            processed_count = processed_counts[timezone]

            # Call the function to get places and update the processed count
            processed_count = get_places_in_area(city, state, latitude, longitude, timezone, processed_count)

            # Update the processed count for the timezone
            processed_counts[timezone] = processed_count

def process_locations_input(input_file):
    with open(input_file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        timezone_locations = {}

        for row in csv_reader:
            if row['City'].strip().startswith('#'):
                continue

            city = row['City']
            state = row['State']
            latitude = row['Latitude']
            longitude = row['Longitude']
            timezone = row['Timezone']
            if timezone not in timezone_locations:
                timezone_locations[timezone] = []
            timezone_locations[timezone].append(row)

            # Depending on breakintotimezone, process by timezone or by area
            if breakintotimezone:
             for timezone, locations in timezone_locations.items():
                timestamp = datetime.now().strftime("%I_%M_%p").lower()
                processed_count = len(locations)
                filename = f"{timezone}_plumbers_{timestamp}_({processed_count}).csv"
                with open(filename, mode='w', newline='', encoding='utf-8') as file:
                   fieldnames = ['Business Name', 'Address', 'Phone Number', 'Website', 'Email', 'Number of Reviews', 'Timezone']
                   writer = csv.DictWriter(file, fieldnames=fieldnames)
                   writer.writeheader()
                   get_places_in_timezone(timezone, locations, writer)
            else:
                # Here you would call get_places_in_area for each row 
                processed_count = 0               
                get_places_in_area(city, state, latitude, longitude, timezone, processed_count)


if __name__ == "__main__":
    ensure_data_folder_exists()  # Ensure the "data" folder exists
    locations_input_file = 'cities_input.csv'
    process_locations_input(locations_input_file)