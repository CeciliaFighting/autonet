import pandas as pd

# Define the input and output file paths
input_file = 'Sep63.10180.xlsx'
output_file = 'Sep63.10180_minimum.xlsx'

# Load the Excel file into a DataFrame
try:
    df = pd.read_excel(input_file)

    # Slice the first 100 rows from the DataFrame
    df_minimum = df.head(100)

    # Save the resulting DataFrame to a new Excel file
    df_minimum.to_excel(output_file, index=False)

    print(f"Successfully saved the first 100 rows to {output_file}")

except FileNotFoundError:
    print(f"Error: The file '{input_file}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")