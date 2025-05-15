
import os
import pandas as pd 
from fuzzywuzzy import process

SGI_Registry_20250129_FilePath = r"C:\Users\HarryBox\Documents\GitHub\dashboard\src\data\sgi_registry_csvs\SGI_RegisterList_20250129.csv"

SGI_Registry_20250129 = []


file_path = os.path.join(SGI_Registry_20250129_FilePath)  # Create the full file path
df = pd.read_csv(file_path)  # Read the CSV into a DataFrame
SGI_Registry_20250129.append(df)  # Append the DataFrame to the list


# Show the first few rows of the combined DataFrame

# print(SGI_Registry_20250129)




excel_folder_path = r"C:\Users\HarryBox\Documents\GitHub\dashboard\src\data\sgi_registry_csvs"

# Variables to track the total change and number of days
total_change_before = 0
num_days_before = 0

# Loop through the range of dates in the file names (from 20250130 to 20250218)
for date in range(20250130, 20250219):  # Looping through the range
    file_name = f"SGI_Significant_Movements_{date}.XLS"  # Generate the file name for each date
    file_path = os.path.join(excel_folder_path, file_name)  # Full file path
    
    if os.path.exists(file_path):  # Ensure the file exists
        #print(f"Reading {file_path}...")  # Debug: Print the file being read
        
        try:
            # Read the first sheet of the Excel file into a DataFrame
            df = pd.read_excel(file_path, sheet_name=0, engine='xlrd')  # Assuming the first sheet
            #print(f"Processing {file_name}...")

            # Check if the 5th column exists and clean the data to ensure it's numeric
            if df.shape[1] > 5:  # Ensure there are at least 5 columns
                # Convert the 5th column to numeric, forcing errors to NaN
                df.iloc[:, 5] = pd.to_numeric(df.iloc[:, 5], errors='coerce')

                # Sum the 5th column (index 4)
                daily_change = df.iloc[:, 5].sum()
            else:
                daily_change = 0  # If no 5th column, set change to 0
            
            # Add this day's change to the total change
            total_change_before += daily_change
            num_days_before += 1  # Count the days
            
        except Exception as e:
            print(f"Error reading {file_path}: {e}")  # Print any error messages

    else:
        print(f"File {file_name} does not exist.")

# Calculate the average daily change
if num_days_before > 0:
    average_daily_change = total_change_before / num_days_before
    print(f"Total change in holdings over {num_days_before} days: {total_change_before}")
    print(f"Average daily change in holdings: {average_daily_change}")
else:
    print("No valid files were processed.")
    

# Variables to track the total change and number of days
total_change_after = 0
num_days_after = 0

# Loop through the range of dates in the file names (from 20250130 to 20250218)
for date in range(20250225, 20250304):  # Looping through the range
    file_name = f"SGI_Significant_Movements_{date}.XLS"  # Generate the file name for each date
    file_path = os.path.join(excel_folder_path, file_name)  # Full file path
    
    if os.path.exists(file_path):  # Ensure the file exists
        #print(f"Reading {file_path}...")  # Debug: Print the file being read
        
        try:
            # Read the first sheet of the Excel file into a DataFrame
            df = pd.read_excel(file_path, sheet_name=0, engine='xlrd')  # Assuming the first sheet
            #print(f"Processing {file_name}...")

            # Check if the 5th column exists and clean the data to ensure it's numeric
            if df.shape[1] > 5:  # Ensure there are at least 5 columns
                # Convert the 5th column to numeric, forcing errors to NaN
                df.iloc[:, 5] = pd.to_numeric(df.iloc[:, 5], errors='coerce')

                # Sum the 5th column (index 4)
                daily_change = df.iloc[:, 5].sum()
            else:
                daily_change = 0  # If no 5th column, set change to 0
            
            # Add this day's change to the total change
            total_change_after += daily_change
            num_days_after += 1  # Count the days
            
        except Exception as e:
            print(f"Error reading {file_path}: {e}")  # Print any error messages

    else:
        print(f"File {file_name} does not exist.")

# Calculate the average daily change
if num_days_after > 0:
    average_daily_change = total_change_after / num_days_after
    print(f"Total change in holdings over {num_days_after} days: {total_change_after}")
    print(f"Average daily change in holdings: {average_daily_change}")
else:
    print("No valid files were processed.")



# Load the reference CSV file and extract Column B (assuming it's the second column, index 1)
reference_df = pd.read_csv(SGI_Registry_20250129_FilePath)  # Load CSV
reference_column_b = reference_df.iloc[:, 1].dropna().unique()  # Get unique, non-null values from Column B

# Lists to track new shareholders added during the two periods
new_shareholders_period_1 = []
new_shareholders_period_2 = []

# Dates for the periods
before_period_dates = range(20250130, 20250219)  # From 20250130 to 20250219
after_period_dates = range(20250226, 20250304)  # From 20250220 to 20250305

# ------------------------------------------
# Process the before period (20250130 to 20250219)
days_before_period = 0
for date in before_period_dates:
    file_name = f"SGI_Significant_Movements_{date}.XLS"  # Generate the file name for each date
    file_path = os.path.join(excel_folder_path, file_name)  # Full file path
    
    if os.path.exists(file_path):  # Ensure the file exists
        print(f"Reading {file_path}...")  # Debug: Print the file being read
        days_before_period += 1  # Increment the day counter for each valid file
        
        try:
            # Read the first sheet of the Excel file into a DataFrame
            df = pd.read_excel(file_path, sheet_name=0, engine='xlrd')  # Assuming the first sheet
            
            # Extract Column B (second column, index 1)
            column_b = df.iloc[:, 1].dropna().unique()  # Get unique, non-null values from Column B
            
            # Check each entry in Column B to see if it's not in the reference list and not already in new_shareholders_period_1
            for entry in column_b:
                if entry not in reference_column_b and entry not in new_shareholders_period_1:
                    new_shareholders_period_1.append(entry)  # Add the shareholder to the list of new ones
        
        except Exception as e:
            print(f"Error reading {file_path}: {e}")  # Print any error messages

    else:
        print(f"File {file_name} does not exist.")

# ------------------------------------------
# Process the after period (20250220 to 20250305)
days_after_period = 0
for date in after_period_dates:
    file_name = f"SGI_Significant_Movements_{date}.XLS"  # Generate the file name for each date
    file_path = os.path.join(excel_folder_path, file_name)  # Full file path
    
    if os.path.exists(file_path):  # Ensure the file exists
        print(f"Reading {file_path}...")  # Debug: Print the file being read
        days_after_period += 1  # Increment the day counter for each valid file
        
        try:
            # Read the first sheet of the Excel file into a DataFrame
            df = pd.read_excel(file_path, sheet_name=0, engine='xlrd')  # Assuming the first sheet
            
            # Extract Column B (second column, index 1)
            column_b = df.iloc[:, 1].dropna().unique()  # Get unique, non-null values from Column B
            
            # Check each entry in Column B to see if it's not in the reference list and not already in new_shareholders_period_2
            for entry in column_b:
                if entry not in reference_column_b and entry not in new_shareholders_period_1 and entry not in new_shareholders_period_2:
                    new_shareholders_period_2.append(entry)  # Add the shareholder to the list of new ones

        except Exception as e:
            print(f"Error reading {file_path}: {e}")  # Print any error messages

    else:
        print(f"File {file_name} does not exist.")

# ------------------------------------------
# Calculate Average Daily Rate
# Before Period: 20250130 to 20250219
new_shareholders_before = len(new_shareholders_period_1)
average_daily_rate_before = new_shareholders_before / days_before_period if days_before_period > 0 else 0

# After Period: 20250220 to 20250305
new_shareholders_after = len(new_shareholders_period_2)
average_daily_rate_after = new_shareholders_after / days_after_period if days_after_period > 0 else 0

# ------------------------------------------
# Output the results
print(f"\nTotal new shareholders before 20250219: {new_shareholders_before}")
print(f"Total new shareholders after 20250219: {new_shareholders_after}")
print(f"Average daily rate of new shareholders before 20250219: {average_daily_rate_before:.2f}")
print(f"Average daily rate of new shareholders after 20250219: {average_daily_rate_after:.2f}")


print(new_shareholders_period_2)




# Dictionary to track broker share volumes for both bought and sold shares
broker_share_volume = {}

# Dates for the period (20250129 to 20250219)
file_dates = list(range(20250226, 20250304))  # From 20250129 to 20250219 as a list

# ------------------------------------------
# Process each file
for date in file_dates:
    file_name = f"SGI_Significant_Movements_{date}.XLS"  # Generate the file name for each date
    file_path = os.path.join(excel_folder_path, file_name)  # Full file path
    
    if os.path.exists(file_path):  # Ensure the file exists
        print(f"Reading {file_path}...")  # Debug: Print the file being read
        
        try:
            # Read the first sheet of the Excel file into a DataFrame
            df = pd.read_excel(file_path, sheet_name=0, engine='xlrd')  # Assuming the first sheet
            
            # Iterate through each row and extract broker, shares bought, and shares sold
            for index, row in df.iterrows():
                broker = row.iloc[3]  # Column D: Broker (4th column)
                shares_bought = row.iloc[5]  # Column F: Shares Bought (6th column)
                shares_sold = row.iloc[6]  # Column G: Shares Sold (7th column)
                
                if pd.notna(broker) and pd.notna(shares_bought) and pd.notna(shares_sold):  # Only consider valid rows
                    # If the broker already exists, accumulate both bought and sold shares
                    if broker in broker_share_volume:
                        broker_share_volume[broker]['bought'] += shares_bought
                        broker_share_volume[broker]['sold'] += shares_sold
                    else:
                        # Initialize the broker's data if it does not exist
                        broker_share_volume[broker] = {'bought': shares_bought, 'sold': shares_sold}
            
        except Exception as e:
            print(f"Error reading {file_path}: {e}")  # Print any error messages

    else:
        print(f"File {file_name} does not exist.")

# ------------------------------------------
# Convert the results to a DataFrame
broker_data = []
for broker, volume in broker_share_volume.items():
    broker_data.append([broker, volume['bought'], volume['sold']])

# Create DataFrame
broker_share_volume_df = pd.DataFrame(broker_data, columns=["Broker", "Total Shares Bought", "Total Shares Sold"])


excel_folder_path_outputPath = r"C:\Users\HarryBox\Documents\GitHub\dashboard\src\data\sgi_registry_csvs\output.csv"
broker_share_volume_df.to_csv(excel_folder_path_outputPath, index=False)


# Optionally, print the first few rows of the broker share volumes
print(broker_share_volume_df)  # Show first few rows

