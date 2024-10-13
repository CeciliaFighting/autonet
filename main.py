import itertools
import pandas as pd
import re
import os
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from multiprocessing import Pool, cpu_count
from decimal import Decimal
from datetime import datetime
import time
import logging


def extract_date(journal_name):
    # contents inside journal_name is either in format:
    # 26-SEP-2024 Bank Statements(1) JPY
    # or
    # USL 25-SEP-24 800019920808 Checkbook JPY
    # it should be converted to '26-SEP-2024' format in this function

    # Pattern 1: Match the '26-SEP-2024' format
    match = re.search(r'(\d{2}-\w{3}-\d{4})', journal_name)

    if match:
        # If the date is already in the 'DD-MMM-YYYY' format, return it
        return match.group(0)

    # Pattern 2: Match the '25-SEP-24' format
    match = re.search(r'(\d{2}-\w{3}-(\d{2}))', journal_name)

    if match:
        # Extract the day, month, and 2-digit year
        date_str = match.group(1)  # '25-SEP-24'
        year_2_digit = match.group(2)  # '24'

        # Convert the 2-digit year to 4-digit year (assuming 2000s)
        year_4_digit = '20' + year_2_digit

        # Replace the 2-digit year with the 4-digit year
        converted_date_str = date_str[:-2] + year_4_digit
        return converted_date_str

    return None  # Return None if no valid date is found


def amount_conversion(amount):
    # Function to round the value and check tolerance

    # return Decimal(amount)

    tolerance = 1e-5
    try:
        rounded_value = round(amount, 0)

        # Check if the difference between the original and rounded value exceeds the tolerance
        if abs(amount - rounded_value) < tolerance:
            return Decimal(rounded_value)

        logging.warning(f"Cannot round amount {amount} to a whole number.")
        return Decimal(amount)

    except Exception as e:
        logging.error(f"Error processing amount {amount}: {e}")
        exit(0)


def find_zero_sum_combinations(args):
    # Function to find matching combinations of numbers that sum to zero and assign a unique ID to each matched group
    numbers, matched_mask, combination_size, tolerance, time_limit, current_match_id = args

    matched_indices = set()  # To store indices of matched rows
    # Consider only unmatched rows
    indices = [i for i in range(len(numbers)) if not matched_mask[i]]

    start_time = time.time()
    matched_groups = {}

    for combo in itertools.combinations(indices, combination_size):
        logging.warning(f"time.time() - start_time: {time.time() - start_time}")
        # Check the time limit
        if time.time() - start_time > time_limit:
            logging.info(f"Time limit exceeded for {
                combination_size}-number matches, stopping further processing.")
            break  # Stop processing if time limit is exceeded

        values = [numbers[i] for i in combo]

        # Skip if all values are positive or all values are negative
        if all(v > 0 for v in values) or all(v < 0 for v in values):
            continue  # Skip combinations that are all positive or all negative

        if abs(sum(values)) <= Decimal(tolerance):  # Check if the sum is within tolerance
            matched_indices.update(combo)  # Mark this combination as matched
            for idx in combo:
                # Assign a unique match ID to each number in the combo
                matched_groups[idx] = current_match_id
            current_match_id += 1  # Increment match ID for next match group

        # Check time again within the loop
        if time.time() - start_time > time_limit:
            logging.info(f"Reached time limit while processing {
                combination_size}-number matches.")
            break

    return matched_indices, matched_groups, current_match_id


def parallel_match_combinations(numbers, matched_mask, combination_size, tolerance, time_limit, current_match_id):
    # Helper function to parallelize combination matching and track match IDs

    num_workers = cpu_count()  # Get the number of CPU cores
    pool = Pool(processes=num_workers)
    # Split work across CPU cores
    args = [(numbers, matched_mask, combination_size, tolerance,
             time_limit, current_match_id) for _ in range(num_workers)]
    result_sets = pool.map(find_zero_sum_combinations, args)
    pool.close()
    pool.join()

    # Combine all matched indices and match groups from parallel processes
    matched_indices = set()
    matched_groups = {}
    for result in result_sets:
        matched_indices.update(result[0])
        matched_groups.update(result[1])
        current_match_id = result[2]  # Update the current match ID

    return matched_indices, matched_groups, current_match_id


def group_by_date(df, matched_mask, combination_size, tolerance, time_limit, current_match_id):
    # Group by same date and perform matching, assigning unique match IDs

    grouped = df.groupby('date')
    total_group_matched_indices = set()
    matched_groups = {}

    for group_name, group in grouped:
        # Start time for each group
        start_time = time.time()

        group_indices = group.index.tolist()
        group_numbers = df.loc[group_indices, 'accounted_amount'].tolist()

        group_matched_indices, group_matched_groups, current_match_id = parallel_match_combinations(
            group_numbers, [False] * len(group_numbers), combination_size, tolerance, time_limit, current_match_id)
        total_group_matched_indices.update(
            [group_indices[idx] for idx in group_matched_indices])
        matched_groups.update(
            {group_indices[idx]: group_matched_groups[idx] for idx in group_matched_groups})

        # Calculate time taken for each group
        process_time = round(time.time() - start_time, 2)
        logging.info(f"Processed date {group_name} for {
            combination_size}-number matches, Matched: {len(group_matched_indices)} numbers, Time: {process_time} seconds")

    return total_group_matched_indices, matched_groups, current_match_id


def _select_file_path(desired_file_path: str) -> str:
    if desired_file_path != '':
        return desired_file_path
    else:
        # Open a file dialog to ask the user to select an Excel file
        path = askopenfilename(
            title="Select an Excel file",
            filetypes=[("Excel files", "*.xlsx"), ("Excel files", "*.xls")]
        )
        if not path:
            logging.error("No file selected, exiting.")
            exit(0)
        return path


def _check_file_path(file_path: str) -> None:
    if not os.path.exists(file_path):
        logging.error(f"File {file_path} does not exist.")
        exit(0)


def _setup_log() -> None:
    # Configure logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # change color of logging
    logging.addLevelName(logging.INFO, "\033[32m%s\033[0m" %
                         logging.getLevelName(logging.INFO))

    logging.addLevelName(logging.WARNING, "\033[33m%s\033[0m" %
                         logging.getLevelName(logging.WARNING))

    logging.addLevelName(logging.ERROR, "\033[31m%s\033[0m" %
                         logging.getLevelName(logging.ERROR))


def perform_number_matches(numbers, matched_mask, combination_size, tolerance, time_limit, current_match_id, start_time):
    # Function to perform number matches for a given combination size
    matched_indices, matched_groups, current_match_id = parallel_match_combinations(
        numbers, matched_mask, combination_size, tolerance, time_limit, current_match_id)

    for idx in matched_indices:
        matched_mask[idx] = True  # Mark these numbers as matched

    process_time = round(time.time() - start_time, 2)
    logging.info(
        f"{combination_size}-number matches found: {len(matched_indices)}, Time: {process_time} seconds")

    return matched_indices, matched_groups, current_match_id


class CustomConfig:
    # desired_file_path: str = '' # leave empty to select file
    desired_file_path: str = 'Sep63.10180.xlsx'  # real path
    # desired_file_path: str = 'Sep63.10180_minimum.xlsx'  # test path


def main(config: CustomConfig) -> None:
    logging.info("Starting the matching process.")

    # Set up the main tkinter window for file selection and output display
    root = Tk()
    root.withdraw()  # Hide the main tkinter window

    using_file_path = _select_file_path(config.desired_file_path)
    _check_file_path(using_file_path)
    df = pd.read_excel(using_file_path)
    logging.info(f"File loaded: {using_file_path}")

    logging.info(
        f"----------------- Data loaded successfully -----------------\n")

    logging.info(f"Rows: {len(df)}")

    df.columns = df.columns.str.strip()
    if 'accounted_amount' not in df.columns or 'journal_name' not in df.columns:
        logging.error(
            "Required columns 'accounted_amount' or 'journal_name' not found.")
        return

    # Extract date from 'journal_name' and group by date
    df['date'] = df['journal_name'].apply(extract_date)
    # TODO: checking logic? use any() instead?
    if df['date'].isnull().all():
        logging.error("No valid dates found in 'journal_name'.")
        return

    df['accounted_amount'] = df['accounted_amount'].apply(amount_conversion)

    # for debugging: save a version of df
    df.to_excel('df.xlsx', index=False)

    start_time = time.time()

    current_match_id = 1
    matched_groups_3 = {}
    matched_groups_4 = {}
    matched_groups_5 = {}

    # Perform 2-number matches first
    numbers = df['accounted_amount'].dropna().tolist()
    matched_mask = [False] * len(numbers)
    logging.info(f'Dropped: {len(df) - len(numbers)}')

    # Find 2-number matches across the entire dataset using parallel processing
    matched_indices_2, matched_groups_2, current_match_id = perform_number_matches(
        numbers, matched_mask, 2, 2, 300, current_match_id, start_time)

    # Check how many unmatched items are left
    unmatched_count = matched_mask.count(False)

    if unmatched_count <= 1000:
        logging.info(
            f"Remaining unmatched lines <= 1000, performing 3-5 number matches directly.")

        for combination_size in range(3, 6):  # 3 to 5-number combinations
            # Perform 3-5 number matches for remaining items
            _, matched_groups, current_match_id = perform_number_matches(
                numbers, matched_mask, combination_size, 2, 300, current_match_id, start_time)

            if combination_size == 3:
                matched_groups_3 = matched_groups
            elif combination_size == 4:
                matched_groups_4 = matched_groups
            elif combination_size == 5:
                matched_groups_5 = matched_groups

    else:
        logging.info(
            f"Remaining unmatched lines > 1000, grouping by date and performing 3-5 number matches.")

        # Group by date and perform 3-5 number matches
        for combination_size in range(3, 6):  # 3 to 5-number combinations
            group_matched_indices, group_matched_groups, current_match_id = group_by_date(
                df, matched_mask, combination_size, 2, 1, current_match_id)
            for idx in group_matched_indices:
                matched_mask[idx] = True  # Mark these numbers as matched

        process_time = round(time.time() - start_time, 2)
        logging.info(f"Group by date used time: {process_time} seconds")

    # After date group matching, now only process the remaining unmatched items
    remaining_unmatched_indices = [i for i in range(
        len(matched_mask)) if not matched_mask[i]]
    remaining_numbers = [numbers[i] for i in remaining_unmatched_indices]

    logging.info(f"Processing remaining unmatched rows: {len(remaining_numbers)}")

    # Perform 3-5 number matches for remaining unmatched items
    for combination_size in range(3, 6):  # 3 to 5-number combinations
        remaining_matched_indices, remaining_matched_groups, current_match_id = perform_number_matches(
            remaining_numbers, [False] * len(remaining_numbers), combination_size, 2, 300, current_match_id, start_time)

        for idx in remaining_matched_indices:
            matched_mask[remaining_unmatched_indices[idx]
                         ] = True  # Mark these numbers as matched

    # Add the 'match' column and 'match_id' to the DataFrame
    df['match'] = ['matched' if matched else 'unmatched' for matched in matched_mask]
    df['match_id'] = [matched_groups_2.get(i, '') or matched_groups_3.get(
        i, '') or matched_groups_4.get(i, '') or matched_groups_5.get(i, '') for i in range(len(numbers))]

    # Save the result to a new Excel file in the same directory with the original file name
    source_dir = os.path.dirname(using_file_path)
    original_file_name = os.path.basename(using_file_path).rsplit(
        '.', 1)[0]  # Remove the extension for renaming
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_file = os.path.join(source_dir, f"matching file_{
                               original_file_name}_{current_time}.xlsx")

    df.to_excel(output_file, index=False)

    logging.info(f"Processing complete, results saved to {output_file}")


if __name__ == "__main__":
    _setup_log()
    customConfig = CustomConfig()
    main(customConfig)
