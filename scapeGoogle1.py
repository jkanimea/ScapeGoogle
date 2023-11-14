import csv
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Replace with your Google Cloud API Key
api_key = os.getenv("GOOGLE_CLOUD_API_KEY")
radius = int(os.getenv("Radius"))
niche = os.getenv("Niche")
max_reviews = int(os.getenv("Max_reviews"))
max_results = int(
    os.getenv("Max_results")
)  # Define the parameters for the Places API request
endpointNearbySearch = os.getenv("GoogleAPIPlaceNearbySearchEndpoint")
endpointDetails = os.getenv("GoogleAPIPlaceDetailsEndpoint")
fileOutputMode = int(os.getenv("FileOutputMode"))
locations_input_file = os.getenv("Locations_input_file")

def ensure_data_folder_exists():
    
     # Get the current date and time formatted as YY-MM-DD_hh-mm-ss
    timestamp = datetime.now().strftime("%y-%m-%d_%I-%M-%p").lower()
    # Check if the "data" folder exists, and create it if not
    data_folder = os.path.join(os.getenv("Data_folder"), f"data_{timestamp}")
    try:
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
    except OSError as e:
        print(f"Error creating data folder: {e}")
        exit(1)  # Exit the script if the data folder can't be created


def get_email_from_website(website_url):
    try:
        response = requests.get(website_url, timeout=10)
        response.raise_for_status()  # Raises HTTPError if the HTTP request returned an unsuccessful status code

        # Parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all <a> tags with a mailto: link
        mailto_links = soup.select('a[href^="mailto:"]')
        for mailto in mailto_links:
            email = mailto.get("href")
            if email:
                # Strip 'mailto:' and return the email address
                return email.replace("mailto:", "", 1)

    except requests.exceptions.RequestException as e:
        # If there is any request-related error, print it and return 'N/A'
        print(f"An error occurred while fetching the website for email: {e}")

    return "N/A"  # Default if no email found or in case of an error


def get_places_in_area(
    city, state, url, timezone, processed_count, max_results, writer=None
):
    page_token = None  # Initialize a page token
    data_folder = os.getenv("Data_folder")

    # Check if a writer is provided. If not, create a new CSV file.
    if writer is None:
        current_time = datetime.now().strftime("%I_%M_%p")
        output_file_base = f"{timezone}_{niche}_in_{city}_{state}_{current_time}"
        output_file_name = os.path.join(
            data_folder, f"{output_file_base}_({processed_count}).csv"
        )

        csv_file = open(output_file_name, mode="w", newline="", encoding="utf-8")
        fieldnames = [
            "Business Name",
            "Address",
            "Phone Number",
            "Website",
            "Email",
            "Number of Reviews",
            "Timezone",
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        own_file = True
    else:
        own_file = False

    while max_results > 0:
            # Send the API request with the page token if available
            if page_token:
                next_url = f"{url}&pagetoken={page_token}"
                response = requests.get(next_url)
            else:
                response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                next_page_token = data.get("next_page_token", None)

                for result in results:
                    # Check if the business status is "OPERATIONAL"
                    business_status = result.get("business_status", "N/A")
                    if business_status != "OPERATIONAL":
                        continue  # Skip non-operational businesses

                    num_reviews = result.get("user_ratings_total", 0)

                    if num_reviews < max_reviews:
                        place_id = result.get("place_id", "N/A")

                        # Use the Place Details API to get additional information
                        details_url = f"{endpointDetails}?place_id={place_id}&fields=name,formatted_address,international_phone_number,website&key={api_key}"
                        details_response = requests.get(details_url)

                        if details_response.status_code == 200:
                            details_data = details_response.json()
                            details = details_data.get("result", {})

                            business_name = details.get("name", "N/A")
                            address = details.get("formatted_address", "N/A")
                            phone_number = details.get(
                                "international_phone_number", "N/A"
                            )
                            website = details.get("website", "N/A")
                            email = "N/A"  # Default value

                            if website != "N/A":
                                # If a website is present, try to fetch the email from it
                                email = get_email_from_website(website)

                            # Write data to the CSV file
                            writer.writerow(
                                {
                                    "Business Name": business_name,
                                    "Address": address,
                                    "Phone Number": phone_number,
                                    "Website": website,
                                    "Email": email,
                                    "Number of Reviews": num_reviews,
                                    "Timezone": timezone,
                                }
                            )
                            processed_count += 1  # Increment the processed count

                            max_results -= 1
                        else:
                            print(
                                f"Error fetching details: {details_response.status_code} - {details_response.text}"
                            )

                # If there's no next page, break the loop
                if not next_page_token:
                    break

                page_token = next_page_token
            else:
                print(f"Error: {response.status_code} - {response.text}")

    # Close the file if this function created its own file
    if own_file:
        csv_file.close()

        # Update the output file name with the final processed count after the loop
        final_output_file_name = os.path.join(
            data_folder, f"{output_file_base}_({processed_count}).csv"
        )
        os.rename(output_file_name, final_output_file_name)

    # Return the processed count
    return processed_count


def process_locations_input(input_file):
    with open(input_file, mode="r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data_folder = os.getenv("Data_folder")
        processed_count = 0

        if fileOutputMode == 1:
            output_file_base = f"combined_{niche}"
            output_file_name = os.path.join(data_folder, f"{output_file_base}_({processed_count}).csv")
            with open(
                output_file_name, mode="w", newline="", encoding="utf-8"
            ) as combined_csv_file:
                fieldnames = [
                    "Business Name",
                    "Address",
                    "Phone Number",
                    "Website",
                    "Email",
                    "Number of Reviews",
                    "Timezone",
                ]
                writer = csv.DictWriter(combined_csv_file, fieldnames=fieldnames)
                writer.writeheader()

                for row in csv_reader:
                    if not row["City"].strip().startswith("#"):
                        processed_count= process_row(row, processed_count, writer)
                        
            final_output_file_name = os.path.join(
            data_folder, f"{output_file_base}_({processed_count}).csv")
            os.rename(output_file_name, final_output_file_name)       
        elif fileOutputMode == 2:
            for row in csv_reader:
                if not row["City"].strip().startswith("#"):
                    process_row(row, processed_count)
        elif fileOutputMode == 3:

            writers = {}  # Dictionary to hold writers for each timezone
            for row in csv_reader:
                if not row["City"].strip().startswith("#"):
                    timezone = row["Timezone"]
                    if timezone not in writers:
                        writers[timezone] = create_timezone_writer(timezone, data_folder, niche)
                    processed_count = process_row(row, processed_count, writers[timezone]['writer'])                    
                    
            # Close all the CSV files
            for writer_info in writers.values():
                writer_info['file'].close()                  

def create_timezone_writer(timezone, data_folder, niche):
    output_file_base = f"{timezone}_{niche}"
    output_file_name = os.path.join(data_folder, f"{output_file_base}.csv")

    csv_file = open(output_file_name, mode="a", newline="", encoding="utf-8")
    fieldnames = [
        "Business Name", "Address", "Phone Number", "Website",
        "Email", "Number of Reviews", "Timezone",
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    return {"writer": writer, "file": csv_file}
      
def process_row(row, processed_count, writer=None):
    city = row["City"]
    state = row["State"]
    latitude = row["Latitude"]
    longitude = row["Longitude"]
    timezone = row["Timezone"]
    url = f"{endpointNearbySearch}?location={latitude},{longitude}&radius={radius}&type={niche}&key={api_key}"
    processed_count = get_places_in_area(city, state, url, timezone, processed_count, max_results, writer)
    return processed_count


if __name__ == "__main__":
   try: 
    ensure_data_folder_exists()  # Ensure the "data" folder exists
    process_locations_input(locations_input_file)
   except Exception as e:
        print(f"An unexpected error occurred: {e}")
