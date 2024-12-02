# shout out chat: https://chatgpt.com/share/674d19eb-bccc-8002-8711-261b11807e4e

import pandas as pd
import os
from datetime import datetime

# Define the date ranges
date_ranges = [
    ("2020-03-10", "2020-09-10"),
    ("2020-09-11", "2021-03-10"),
    ("2021-03-11", "2021-09-10"),
    ("2021-09-11", "2022-03-10"),
    ("2022-03-11", "2022-09-10"),
    ("2022-09-11", "2023-03-10"),
]

# Convert date ranges to datetime objects
# date_ranges = [
#     (datetime.strptime(start, "%Y-%m-%d"), datetime.strptime(end, "%Y-%m-%d"))
#     for start, end in date_ranges
# ]


def get_save_name(county: str):
    return "_".join(county.split(" ")).lower()


# Process data
def process_data(parent_dir, counties):
    for date_range in date_ranges:
        # Prepare results for this date range
        results = []

        # Iterate over main counties and their neighbors
        for county, details in counties.items():
            # Process main county
            county_save_name = get_save_name(county)
            main_file = os.path.join(
                parent_dir, county_save_name, f"{county_save_name}_daily.csv"
            )
            results.extend(process_file(main_file, date_range))

            # Process neighbors
            for neighbor in details["neighboring_counties"]:
                neighbor_save_name = get_save_name(neighbor)
                neighbor_file = os.path.join(
                    parent_dir,
                    county_save_name,
                    "neighbors",
                    neighbor_save_name,
                    f"{neighbor_save_name}_daily.csv",
                )
                results.extend(process_file(neighbor_file, date_range))

        # Save results for the current date range to a CSV
        save_to_csv(results, date_range)


def process_file(file_path, date_range):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path, dtype={"fips": str})

        # Convert date column to datetime
        df["date"] = pd.to_datetime(df["date"])

        # Filter rows based on the date range
        start_date, end_date = date_range
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        df["start_date"] = start_date

        # Calculate the average delta_cases_per_100k for this date range
        avg_df = df.groupby(
            ["fips", "county", "state", "start_date"], as_index=False
        ).agg(avg_delta_cases_per_100k=("delta_cases_per_100k", "mean"))
        return avg_df.to_dict("records")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []


def save_to_csv(data, date_range):
    # Convert date range to string for file naming
    start_date, end_date = date_range
    file_name = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv"
    save_path = f"arcgis_data/{file_name}"
    # Create a DataFrame and save to CSV
    result_df = pd.DataFrame(data)
    result_df.to_csv(save_path, index=False)
    print(f"Saved: {file_name}")


# Example usage
parent_dir = "queer_areas"
queer_areas = {
    "San Francisco": {
        "state": "California",
        "neighboring_counties": [
            "Alameda",
            "Contra Costa",
            "Marin",
            "Napa",
            "San Mateo",
            "Santa Clara",
            "Solano",
            "Sonoma",
        ],
    },
    "Multnomah": {
        "state": "Oregon",
        "neighboring_counties": ["Columbia", "Hood River", "Clackamas", "Washington"],
    },
    "King": {
        "state": "Washington",
        "neighboring_counties": [
            "Snohomish",
            "Kitsap",
            "Kittitas",
            "Yakima",
            "Pierce",
            "Chelan",
        ],
    },
}

merged_data = pd.DataFrame()
for start_date, end_date in date_ranges:
    file_name = (
        f"arcgis_data/{start_date.replace('-', '')}_to_{end_date.replace('-', '')}.csv"
    )
    data = pd.read_csv(file_name, dtype={"fips": str})
    data["end_date"] = end_date
    midpoint = (
        datetime.strptime(start_date, "%Y-%m-%d")
        + (
            datetime.strptime(end_date, "%Y-%m-%d")
            - datetime.strptime(start_date, "%Y-%m-%d")
        )
        / 2
    )
    # print(midpoint.date())
    data["mid_date"] = midpoint.date()
    merged_data = pd.concat([merged_data, data], ignore_index=True)

# Save the merged data to a new CSV
merged_data.to_csv("arcgis_data/merged_covid_data.csv", index=False)

# process_data(parent_dir, queer_areas)
