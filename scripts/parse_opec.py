import os
import re
import pandas as pd
from sqlalchemy import create_engine

excel_path = "C:/Users/ehbai/energy-research/data/raw/DAT 2025-01-15 PSD MOMR 11 Appendix_Jan 25.xlsx"

if not os.path.exists(excel_path):
    raise FileNotFoundError(f"The file {excel_path} does not exist.")

excel_file = pd.ExcelFile(excel_path)

############################################################
# 1. General Parser for Most Sheets (parse_opec_sheet)
############################################################
def parse_opec_sheet(df_sheet, sheet_name):
    """
    General function for sheets like Table 11 - 1, Table 11 - 2, Table 11 - 4, Table 11 - 5, etc.
    that follow the usual "World demand" / "Non-DoC liquids production" logic.
    
    This is your existing parse function, simplified for illustration.
    Adjust it to match your sheet's block detection, skipping, etc.
    """
    # Example: rename first column to "region" if not present
    orig_first_col = df_sheet.columns[0]
    if "region" not in df_sheet.columns:
        df_sheet = df_sheet.rename(columns={orig_first_col: "region"})

    # Convert to string and strip whitespace
    df_sheet["region"] = df_sheet["region"].astype(str).str.strip()

    # Example: We might define markers for the start of each table
    possible_starts = [
       "World demand",
       "Non-DoC liquids production",
       "OECD closing stock levels, mb",
       "Days of forward consumption in OECD, days",
       "Memo items",
       "Closing stock levels, mb"
    ]

    all_tables = []

    # Find row indices for each known start:
    start_indices = []
    for ms in possible_starts:
        found = df_sheet.index[df_sheet["region"].str.contains(ms, case=False, na=False)]
        if not found.empty:
            start_indices.append((ms, found[0]))
    
    # Sort by ascending row index
    start_indices = sorted(start_indices, key=lambda x: x[1])

    # define function to find next row that starts with parentheses for total
    def find_next_total_row(df, start_row):
        for i in range(start_row + 1, len(df)):
            row_val = df.loc[i, "region"]
            if re.match(r"^\(.*\)", row_val.strip()):
                return i
        return None

    # Slice blocks
    for i in range(len(start_indices)):
        current_table_name, current_start = start_indices[i]
        if i < len(start_indices) - 1:
            _, next_start = start_indices[i+1]
            potential_end = next_start
        else:
            potential_end = len(df_sheet)
        
        total_row_idx = find_next_total_row(df_sheet, current_start)
        if total_row_idx and total_row_idx < potential_end:
            end_of_table = total_row_idx + 1
        else:
            end_of_table = potential_end
        
        block_df = df_sheet.iloc[current_start:end_of_table].copy()
        block_df["table_name"] = current_table_name

        # melt the wide columns (e.g. columns for years/quarters)
        id_vars = ["region", "table_name"]
        value_vars = [c for c in block_df.columns if c not in id_vars]

        block_df_melt = block_df.melt(
            id_vars=id_vars, 
            value_vars=value_vars,
            var_name="period",
            value_name="value"
        )
        
        # parse numeric
        block_df_melt["value"] = pd.to_numeric(block_df_melt["value"], errors="coerce")
        all_tables.append(block_df_melt)
    
    # Combine sub-tables
    if all_tables:
        df_parsed = pd.concat(all_tables, ignore_index=True)
    else:
        # Fallback if no recognized sub-tables found
        df_parsed = df_sheet.copy()
        df_parsed["table_name"] = "Unclassified"
        df_parsed = df_parsed.melt(
            id_vars=["region", "table_name"],
            var_name="period",
            value_name="value"
        )

    df_parsed["sheet_name"] = sheet_name
    # Reorder
    df_parsed = df_parsed[["sheet_name", "table_name", "region", "period", "value"]]

    return df_parsed

############################################################
# 2. Special Parser for Table 11 - 3 (parse_113)
############################################################
def parse_113(df_sheet):
    """
    Special parser for Table 11 - 3, which has heading+total in col B
    and breakdown regions in col C, plus numeric columns in D, E, F, etc.
    """
    # Rename columns for clarity. Suppose we have columns: B, C, D, E, F => 5 columns.
    df_sheet.columns = [
        "col_b",
        "col_c",
        "val_2021",
        "val_2022",
        "val_2023",
        "val_4Q22",
        "val_1Q23",
        "val_2Q23",
        "val_3Q23",
        "val_4Q23",
        "val_1Q24",
        "val_2Q24",
        "val_3Q24"
    ]

    df_sheet["col_b"] = df_sheet["col_b"].fillna("")
    df_sheet["col_c"] = df_sheet["col_c"].fillna("")

    # Mark heading+total row if col_b is not empty & col_c is empty
    df_sheet["is_heading_total"] = df_sheet.apply(
        lambda row: (row["col_b"].strip() != "") and (row["col_c"].strip() == ""),
        axis=1
    )

    # Forward-fill subheading from col_b
    def get_heading(row):
        return row["col_b"].strip() if row["col_b"].strip() else None

    df_sheet["subheading_temp"] = df_sheet.apply(get_heading, axis=1)
    df_sheet["subheading"] = df_sheet["subheading_temp"].ffill()

    # region = "Total" if heading+total row, else col_c
    def get_region(row):
        if row["is_heading_total"]:
            return "Total"
        else:
            return row["col_c"].strip()

    df_sheet["region"] = df_sheet.apply(get_region, axis=1)

    # Melt the value columns
    value_cols = [
        "val_2021", "val_2022", "val_2023",
        "val_4Q22", "val_1Q23", "val_2Q23", "val_3Q23", "val_4Q23",
        "val_1Q24", "val_2Q24"
    ]
    df_melted = df_sheet.melt(
        id_vars=["subheading", "region", "is_heading_total"],
        value_vars=value_cols,
        var_name="period",
        value_name="value"
    )

    # numeric conversion
    df_melted["value"] = pd.to_numeric(df_melted["value"], errors="coerce")

    # drop rows w/o data if needed
    df_melted.dropna(subset=["value"], inplace=True)
    df_melted.reset_index(drop=True, inplace=True)

    # rename columns if you like
    df_melted.rename(columns={"subheading": "table_name"}, inplace=True)

    # If you want to store the actual sheet name:
    df_melted["sheet_name"] = "Table 11 - 3"

    # reorder columns
    df_melted = df_melted[["sheet_name", "table_name", "region", "period", "value", "is_heading_total"]]
    return df_melted

all_parsed = []

for sheet_name in excel_file.sheet_names:
    # skip the 'Contents' sheet if not needed
    if sheet_name.lower() == "contents":
        continue

    # define columns for each table
    col_range_map = {
        "Table 11 - 1": "B:O",
        "Table 11 - 2": "B:O",
        "Table 11 - 3": "B:O",
        "Table 11 - 4": "B:P",
        "Table 11 - 5": "B:L"
    }
    usecols = col_range_map.get(sheet_name, None)

    # read the sheet
    df_sheet = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
        header=0,   # row 4 is the header
        skiprows=4, # adjust if needed
        usecols=usecols
    )

    # apply special parser for Table 11 - 3
    if sheet_name == "Table 11 - 3":
        df_sheet_parsed = parse_113(df_sheet)
    else:
        df_sheet_parsed = parse_opec_sheet(df_sheet, sheet_name)

    all_parsed.append(df_sheet_parsed)

# Concatenate
df_final = pd.concat(all_parsed, ignore_index=True)

# Drop rows w/ NaN 'value' if you want to remove headings
df_final = df_final.dropna(subset=["value"]).reset_index(drop=True)

print(df_final.head(50))

# write to PostgreSQL
engine = create_engine("postgresql://postgres:%401Evanb55@localhost:5433/energy-data")
df_final.to_sql("opec_data", con=engine, if_exists="replace", index=False)
