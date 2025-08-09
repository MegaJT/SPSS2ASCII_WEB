import os
import logging
from typing import Tuple
import pyreadstat
import pandas as pd
import numpy as np
import openpyxl
import re
from datetime import datetime, timedelta


def setup_logging(output_directory: str, external_logger=None):
    """Sets up logging to save logs in the specified directory."""
    
    if external_logger:
        return external_logger  # Use the passed logger
    
    log_file = os.path.join(output_directory, "process_log.log")

    # Create a specific logger for processing
    logger = logging.getLogger("spss_processing")
    logger.setLevel(logging.INFO)

    # Remove any existing handlers to avoid duplicates
    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    # Add file handler with proper buffering
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

    # Ensure logs propagate to the root logger for UI updates
    logger.propagate = False

    return logger

def close_logging(logger):
    """Properly closes all handlers for the logger to release file locks."""
    if logger and logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

def validate_file(file_path: str) -> None:
    """Validates that the file exists and is accessible."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

def validate_settings_config(settings_config: dict, logger: logging.Logger):
    if not isinstance(settings_config, dict):
        logger.error("Invalid settings_config: Expected a dictionary.")
        raise ValueError("Invalid settings_config: Expected a dictionary.")
    

def validate_map_file(external_ascii_map, df, output_map_file, logger: logging.Logger):
    """
    Validates the external ASCII map against the DataFrame `df`.
    Creates an error file with detailed issues if validation fails.
    """
    try:
        external_ascii_map["Error"] = None
        error_found = False

        # Remove duplicates from the Column_Name column
        external_ascii_map = external_ascii_map.drop_duplicates(subset=["Column_Name"]).reset_index(drop=True)

        # Checking for duplicate Start_Col or End_Col values
        duplicate_start_col = external_ascii_map.duplicated(subset=["Start_Col"], keep=False)
        duplicate_end_col = external_ascii_map.duplicated(subset=["End_Col"], keep=False)

        if duplicate_start_col.any():
            external_ascii_map.loc[duplicate_start_col, "Error"] = "Duplicate Start_Col value"
            error_found = True
            logger.error("Found duplicate Start_Col values in map file")

        if duplicate_end_col.any():
            external_ascii_map.loc[duplicate_end_col, "Error"] = "Duplicate End_Col value"
            error_found = True
            logger.error("Found duplicate End_Col values in map file")

        # Check if Start_Col is greater than End_Col and other column consistency
        for idx, (index, row) in enumerate(external_ascii_map.iterrows(), start=1):
            if pd.isna(row["Error"]):  # Only check rows without existing errors
                try:
                    start_col = int(row["Start_Col"]) if pd.notna(row["Start_Col"]) else None
                    end_col = int(row["End_Col"]) if pd.notna(row["End_Col"]) else None
                    
                    if start_col is not None and end_col is not None:
                        if start_col > end_col:
                            external_ascii_map.at[index, "Error"] = "Start_Col cannot be greater than End_Col"
                            error_found = True
                            logger.error(f"Column {row['Column_Name']}: Start_Col ({start_col}) > End_Col ({end_col})")
                        elif start_col <= 0 or end_col <= 0:
                            external_ascii_map.at[index, "Error"] = "Start_Col and End_Col must be positive integers"
                            error_found = True
                            logger.error(f"Column {row['Column_Name']}: Invalid column numbers (must be > 0)")
                    else:
                        external_ascii_map.at[index, "Error"] = "Start_Col and End_Col cannot be empty"
                        error_found = True
                        logger.error(f"Column {row['Column_Name']}: Missing Start_Col or End_Col values")
                        
                except (ValueError, TypeError):
                    external_ascii_map.at[index, "Error"] = "Start_Col and End_Col must be valid integers"
                    error_found = True
                    logger.error(f"Column {row['Column_Name']}: Invalid integer values in Start_Col or End_Col")

        # Checking for missing variables in the Map
        for idx, (index, row) in enumerate(external_ascii_map.iterrows(), start=1):
            if pd.isna(row["Error"]):  # Only check rows without existing errors
                column_name = row["Column_Name"]
                if column_name not in df.columns:
                    external_ascii_map.at[index, "Error"] = "Variable missing in the data file"
                    error_found = True
                    logger.error(f"Column {column_name}: Not found in SPSS data file")

        # Checking for invalid column numbers/insufficient width in the Map
        for idx, (index, row) in enumerate(external_ascii_map.iterrows(), start=1):
            if pd.isna(row["Error"]):  # Only check rows without existing errors
                column_name = row["Column_Name"]
                try:
                    start_col = int(row["Start_Col"])
                    end_col = int(row["End_Col"])
                    width = end_col - start_col + 1
                    max_width = df[column_name].astype(str).str.len().max()
                    if max_width > width:
                        external_ascii_map.at[index, "Error"] = "Insufficient column width/Incorrect Column numbers"
                        error_found = True
                        logger.error(f"Column {column_name}: Width {width} insufficient for max data length {max_width}")
                except (ValueError, TypeError):
                    # This should already be caught above, but adding as safety
                    if pd.isna(external_ascii_map.at[index, "Error"]):
                        external_ascii_map.at[index, "Error"] = "Invalid Start_Col or End_Col values"
                        error_found = True

        # Check for overlapping column ranges
        valid_rows = external_ascii_map[external_ascii_map["Error"].isna()]
        if len(valid_rows) > 1:
            for i, row1 in valid_rows.iterrows():
                for j, row2 in valid_rows.iterrows():
                    if i >= j:  # Skip same row and already compared pairs
                        continue
                    
                    start1, end1 = int(row1["Start_Col"]), int(row1["End_Col"])
                    start2, end2 = int(row2["Start_Col"]), int(row2["End_Col"])
                    
                    # Check if ranges overlap
                    if not (end1 < start2 or end2 < start1):
                        external_ascii_map.at[i, "Error"] = f"Column range overlaps with {row2['Column_Name']}"
                        external_ascii_map.at[j, "Error"] = f"Column range overlaps with {row1['Column_Name']}"
                        error_found = True
                        logger.error(f"Overlapping ranges: {row1['Column_Name']} ({start1}-{end1}) and {row2['Column_Name']} ({start2}-{end2})")
        
        # Final validation result
        if not error_found:
            # Remove the Error column if no errors found
            external_ascii_map = external_ascii_map.drop(columns=["Error"])
            logger.info("✓ Map validation passed. No errors found.")
            return external_ascii_map
        else:
            # Save error map file with detailed issues
            try:
                # Add summary information at the top
                error_summary = pd.DataFrame({
                    'VALIDATION_SUMMARY': [
                        f'Total rows checked: {len(external_ascii_map)}',
                        f'Rows with errors: {external_ascii_map["Error"].notna().sum()}',
                        f'Validation date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                        '',
                        'INSTRUCTIONS:',
                        '1. Fix all rows marked with errors',
                        '2. Ensure Start_Col < End_Col for all rows',
                        '3. Ensure no overlapping column ranges',
                        '4. Verify all column names exist in your SPSS file',
                        '5. Re-upload the corrected map file',
                        ''
                    ]
                })
                
                # Write to Excel with multiple sheets for better organization
                with pd.ExcelWriter(output_map_file, engine='openpyxl') as writer:
                    error_summary.to_excel(writer, sheet_name='Instructions', index=False)
                    external_ascii_map.to_excel(writer, sheet_name='Map_With_Errors', index=False)
                    
                    # Add only error rows for quick reference
                    error_rows = external_ascii_map[external_ascii_map["Error"].notna()]
                    if not error_rows.empty:
                        error_rows.to_excel(writer, sheet_name='Errors_Only', index=False)
                
                logger.info(f"Error map file created: {output_map_file}")
                logger.error("❌ Map validation failed! Please download the error map to fix issues.")
                
            except PermissionError as e:
                logger.error(f"Cannot write to {output_map_file}. The file might be open: {e}")
                raise e
            
            raise ValueError("Map validation failed, there are errors in the map file.")
            
    except Exception as e:
        logger.error(f"Error validating map file: {e}")
        raise e

def convert_spss_datetime(df: pd.DataFrame, datetime_columns: list,logger: logging.Logger) -> pd.DataFrame:
    """Converts numeric SPSS datetime values to pandas datetime objects."""
    try:
        SPSS_DATETIME_ORIGIN = datetime(1582, 10, 14)  # SPSS datetime origin
        
        for col in datetime_columns:
            if col in df.columns:
                try:
                    # Convert numeric values to datetime
                    df[col] = df[col].apply(
                        lambda x: SPSS_DATETIME_ORIGIN + timedelta(seconds=x) if pd.notna(x) else pd.NaT
                    )
                except Exception as e:
                    logger.error(f"Error converting column {col} to datetime: {e}")
        
        return df
    except Exception as e: 
        logger.error(f"Error converting SPSS datetime: {e}")
        raise e

def read_spss_file(input_spss_file: str,logger: logging.Logger) -> Tuple[pd.DataFrame, pyreadstat.metadata_container]:
    """Reads SPSS file and ensures DATETIME variables are handled properly."""
    validate_file(input_spss_file)  # Ensure the file is valid
    try:
        # Read SPSS data and metadata
        df, meta = pyreadstat.read_sav(input_spss_file,disable_datetime_conversion =True)
        
        
        # Identify DATETIME variables
        datetime_columns = [
            col for col, vtype in meta.original_variable_types.items()
            if vtype.startswith("DATETIME")
        ]
        df=convert_spss_datetime(df,datetime_columns,logger)
        return df, meta
    except Exception as e:
        logger.error(f"Error reading SPSS file: {e}")
        raise e


def Generate_Inital_Map(df: pd.DataFrame, meta: pyreadstat.metadata_container,logger: logging.Logger) -> pd.DataFrame:
    """Generates a DataFrame containing column metadata."""
    logger.info("Starting to generate final dictionary map...")
    try:
        column_var_dict = meta.variable_value_labels #A dict with variable name as key and label name as value. Label names are those described in value_labels.
        temp_dictmaps = []

        for idx, column_name in enumerate(meta.column_names, start=1):
            try:
                if column_name in column_var_dict:
                    #var = column_var_dict[column_name]
                    value_label_dict = meta.variable_value_labels.get(column_name, {})
                    temp_dictmap = pd.DataFrame(
                        list(value_label_dict.items()), columns=["Code", "Value_Label"]
                    )
                    temp_dictmap["Column_Name"] = column_name
                elif meta.readstat_variable_types[column_name] != "string":
                    temp_dictmap = pd.DataFrame(
                        [[column_name, 0, 0]], columns=["Column_Name", "Value_Label", "Code"]
                    )
                else:
                    continue

                temp_dictmaps.append(temp_dictmap)
            except KeyError as e:
                logger.error(f"KeyError for column {column_name}: {e}")

        generated_ascii_map = pd.concat(temp_dictmaps, ignore_index=True)
        generated_ascii_map = generated_ascii_map[["Column_Name", "Value_Label", "Code"]]
        return generated_ascii_map
    except Exception as e: 
        logger.error(f"Error Generating Initial Map: {e}")
        raise e

def Generate_Width_Map(df: pd.DataFrame, columns: list,widthval:int,logger: logging.Logger) -> pd.DataFrame:
    """Calculates width, start, and end columns for fixed-width mapping."""
    logger.info("Starting to generate Width Map...")
    try:
        width_map = pd.DataFrame(columns=["Column_Name", "Width", "Start_Col", "End_Col"])
        current_col = 1

        for idx, col in enumerate(columns, start=1):
            if col in df.columns:
                width = max(df[col].astype(str).str.len().max(), 1)
                start_col = current_col
                end_col = start_col + width - 1+widthval

                new_row = pd.DataFrame({
                    "Column_Name": [col],
                    "Width": [width],
                    "Start_Col": [start_col],
                    "End_Col": [end_col]
                })
                width_map = pd.concat([width_map, new_row], ignore_index=True)
                current_col = end_col + 1
        return width_map
    except Exception as e:
        logger.error(f"Error Generating Width Map: {e}")
        raise e

def clean_datafile(df: pd.DataFrame, columns_to_convert: list, variable_types: dict,logger: logging.Logger) -> pd.DataFrame:
    try:
        # Regular expression to match column types to skip integer conversion
        skip_int_conversion_pattern = re.compile(r"(A|DATETIME|F\d+\.[1-9]\d*)")
        
        for col in columns_to_convert:
            if col in df.columns:
                # Check if the column type matches the skip pattern
                if col in variable_types and skip_int_conversion_pattern.match(variable_types[col]):
                    # Skip integer conversion but apply other logic
                    df[col] = df[col].apply(lambda x: x if pd.notna(x) else "")
                else:
                    # Apply integer conversion
                    df[col] = df[col].apply(
                        lambda x: int(x) if pd.notna(x) and x != "" else ""
                    ).astype(str)
        return df
    except Exception as e:
        logger.error(f"Error while cleaning dataframe: {e}")
        raise e

def generate_fixed_width_file(df: pd.DataFrame, valid_generated_ascii_map: pd.DataFrame,zpad:bool, output_file: str,logger: logging.Logger) -> None:
    """Generates a fixed-width ASCII file using NumPy for better performance."""
    try:
        logger.info("Starting to generate fixed-width ASCII file...")
        valid_generated_ascii_map = valid_generated_ascii_map.drop_duplicates(subset="Column_Name", keep="first")

        num_rows = len(df)
        max_col = int(valid_generated_ascii_map["End_Col"].max())
        output_array = np.full((num_rows, max_col), " ", dtype="U1")
        
        for idx, meta_row in enumerate(valid_generated_ascii_map.itertuples(), start=1):
            column_name = meta_row.Column_Name
            start_col = meta_row.Start_Col - 1
            end_col = meta_row.End_Col
            width = end_col - start_col

            if column_name in df.columns:
                # Extract the values as strings and strip whitespace
                raw_values = df[column_name].astype(str).str.strip()

                if zpad:
                    # Apply zero-fill only to non-blank values
                    values = raw_values.apply(lambda x: x.zfill(width) if x != '' else x.rjust(width)).str.slice(0, width).to_numpy()
                else:
                    # Apply space-fill (right-aligned)
                    values = raw_values.str.rjust(width).str.slice(0, width).to_numpy()
            else:
                # Handle missing column by filling the space
                values = np.full(num_rows, " " * width, dtype="U1")

            for row_idx, value in enumerate(values):
                output_array[row_idx, start_col:end_col] = list(value)

        fixed_width_lines = ["".join(row) for row in output_array]
        with open(output_file, "w") as file:
            file.write("\n".join(fixed_width_lines) + "\n")

        logger.info(f"Fixed-width file '{output_file}' created successfully.")
    except Exception as e:
        logger.error(f"Error generating fixed-width file: {e}")
        raise e
    

def write_output_files(df: pd.DataFrame, ascii_map: pd.DataFrame, output_dir: str, zpad: bool, logger: logging.Logger):
    try:
        output_ascii_file = os.path.join(output_dir, "Data.asc")
        output_map_file = os.path.join(output_dir, "MAP.xlsx")
        
        generate_fixed_width_file(df, ascii_map, zpad, output_ascii_file, logger)

        try:
            ascii_map.to_excel(output_map_file, index=False)
        except PermissionError as e:
            logger.error(f"Cannot write to {output_map_file}. The file might be open: {e}")
            raise e

        
        logger.info(f"Files saved in: {output_dir}")
    except Exception as e:
        logger.error(f"Error writing output files: {e}")
        raise e

def process_spss_with_map(input_spss_file: str, map_file: str, output_directory: str,settings_config: dict,logger: logging.Logger) -> None:
    """Processes an SPSS file and a Map file to generate a fixed-width ASCII file."""
    
    try:
        validate_settings_config(settings_config, logger)

        zpad=settings_config.get("zero_padding")
        logger.info("Starting process_spss_with_map...")
        df, meta = read_spss_file(input_spss_file,logger)
        map_file_df = pd.read_excel(map_file)

        columns_to_convert = list(map_file_df["Column_Name"].unique())
        original_variable_types=meta.original_variable_types
        df = clean_datafile(df, columns_to_convert,original_variable_types,logger)

        valid_external_ascii_map = validate_map_file(map_file_df, df, os.path.join(output_directory, "Map_Error.xlsx"), logger)

        # Use the helper function to write files
        write_output_files(df, valid_external_ascii_map, output_directory, zpad, logger)
        
        logger.info("Processing completed successfully.")
        
    except Exception as e:
        logger.error(f"Error in processing SPSS and Map file: {e}")
        raise e
    finally:
        # Always close the logger to release file locks
        close_logging(logger)
    

def process_spss_wo_map(input_spss_file: str, output_directory: str,settings_config: dict,logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Processes SPSS file and returns data and final dictionary map."""
    logger.info("Starting process_spss...")
    try:
        validate_settings_config(settings_config, logger)

        zpad=settings_config.get("zero_padding")
        widthval=int(settings_config.get("width_val"))

        df, meta = read_spss_file(input_spss_file,logger)

        generated_ascii_map = Generate_Inital_Map(df, meta,logger)
        columns_to_convert = list(generated_ascii_map["Column_Name"].unique())
        original_variable_types=meta.original_variable_types
        df = clean_datafile(df, columns_to_convert,original_variable_types,logger)
        width_map = Generate_Width_Map(df, columns_to_convert,widthval,logger)
        
        generated_ascii_map = generated_ascii_map.merge(width_map, on="Column_Name", how="left")

        # Use the helper function to write files
        write_output_files(df, generated_ascii_map, output_directory, zpad, logger)
        
        logger.info("Processing completed successfully.")
        
    except Exception as e:
        logger.error(f"Error in processing SPSS and Map file: {e}")
        raise e
    finally:
        # Always close the logger to release file locks
        close_logging(logger)

## MAIN FUNCTION    

if __name__ == "__main__":
    input_spss_file = r"C:\Jijo\MACRO\SPSStoASCII_Python\2\INITIAL_FILE.sav"  # Replace with the path to your SPSS file
    process_spss_wo_map(input_spss_file,r"C:\Jijo\MACRO\SPSStoASCII_Python\2")
    # generate_fixed_width_file(df, generated_ascii_map, "Data.asc")
    # generated_ascii_map.to_excel("MAP.xlsx", index=False)

    # input_spss_file = "INITIAL_FILE.sav"
    # map_file = "MAP.xlsx"
    # process_spss_with_map(input_spss_file, map_file)
