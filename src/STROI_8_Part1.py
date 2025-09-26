import pandas as pd
import logging
import os
import json
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# Initialize logging
try:
    logging.basicConfig(
        filename='./output/logs/STROI_part_1.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging initialized for STROI.")
except Exception as e:
    print("Some issue in creating log file:", e)


def transform_dataframe(df, config):
    """Transform dataframe for KPI alignment."""
    kpi = config.get('kpi_name', 'Unknown_KPI')
    logging.info(f"Transforming dataframe for KPI: {kpi}")
    df['Date'] = pd.to_datetime(df['Date'])

    for col in df.columns:
        col_split = col.split("|")
        if 'effect_essence' in col_split:
            col_split = col_split[:-2]
        if 'Impressions' in col_split:
            col_split.remove("Impressions")
        if 'Cost' in col_split:
            col_split.remove("Cost")
        df.rename(columns={col: "|".join(col_split)}, inplace=True)

    df.set_index('Date', inplace=True)
    transformed_df = pd.DataFrame(df.T.unstack()).reset_index()
    transformed_df.rename({'level_1': "Merged Granularity", 0: f'{kpi}'}, axis=1, inplace=True)
    logging.info("Dataframe transformed successfully.")
    return transformed_df


def STROI(config):
    logging.info("STROI processing started.")
    try:
        # Load LT ROI
        lt_file = f"./output/Extrapolated Data/Only_LT_lt_rroi_{config['brand']}.xlsx"
        req_format_lt = pd.read_excel(lt_file)
        req_format_lt.rename(columns={'Channel/Daypart': 'Channel'}, inplace=True)
        logging.info(f"Loaded LT ROI file: {lt_file}, shape: {req_format_lt.shape}")
        print(f"Loaded LT ROI for {config['brand']}: {req_format_lt.shape}")

        # Load ST ROI
        st_file = f"./input/Data/ST ROI.xlsx"
        st_rroi_df = pd.read_excel(st_file, sheet_name='ROI Format')
        st_rroi_df.rename(columns={'Channel/Daypart': 'Channel'}, inplace=True)
        st_rroi_df.rename(columns={'NTUs': 'Overall NTUs'}, inplace=True)  # Kraken
        st_rroi_df['Date'] = pd.to_datetime(st_rroi_df[['Year', 'Month']].assign(day=1))
        logging.info(f"Loaded ST ROI file: {st_file}, shape: {st_rroi_df.shape}")
        print(f"Loaded ST ROI: {st_rroi_df.shape}")

    except Exception as e:
        logging.error(f"Error loading input files: {e}")
        raise

    # Date ranges
    start_year = pd.to_datetime(config['expected_sales_start'], format=config['date_format']).year
    start_month = pd.to_datetime(config['expected_sales_start'], format=config['date_format']).month
    m_start_year = pd.to_datetime(config['model_start_date'], format=config['date_format']).year
    m_start_month = pd.to_datetime(config['model_start_date'], format=config['date_format']).month
    end_year = pd.to_datetime(config['model_end_date'], format=config['date_format']).year
    end_month = pd.to_datetime(config['model_end_date'], format=config['date_format']).month
    sd_temp = datetime(start_year, start_month, 1)
    ed_temp = datetime(end_year, end_month, 1)
    msd_temp = datetime(m_start_year, m_start_month, 1)
    logging.info(f"Date ranges calculated: {sd_temp} → {ed_temp}, Model start: {msd_temp}")

    # Filter ST ROI
    st_rroi_df = st_rroi_df[(st_rroi_df["Date"] >= sd_temp) & (st_rroi_df["Date"] <= ed_temp)].reset_index(drop=True)
    st_rroi_df.rename(columns={'Overall Volume': 'Overall Units'}, inplace=True)
    logging.info(f"Filtered ST ROI data between {sd_temp} and {ed_temp}, shape: {st_rroi_df.shape}")

    # Identify overall cols
    overall_cols = [col for col in st_rroi_df.columns if 'Overall' in col]
    logging.info(f"Overall columns identified: {overall_cols}")

    # Set Baseline to zero
    for col in overall_cols:
        st_rroi_df.loc[
            (st_rroi_df['Date'] >= msd_temp) &
            (st_rroi_df['Date'] <= ed_temp) &
            (st_rroi_df['Media Type'] == 'Baseline'),
            col
        ] = 0.0
        logging.info(f"Baseline set to 0 for column {col}")
    print(f"Baseline adjustments applied to {len(overall_cols)} columns.")

    # Select feature list
    if config['ProductLine_Flag'] == 1:
        Feature_list = ['Media Type', 'Product Line', 'Master Channel', 'Channel', 'Platform', 'Year', 'Month', 'Cost', 'Impression']
    elif config['ProductLine_Flag'] == 2:
        Feature_list = ['Media Type', 'Product Line', 'Master Channel', 'Channel', 'Year', 'Month', 'Cost', 'Impression']

    Feature_list.extend(overall_cols)
    st_rroi_df = st_rroi_df[Feature_list]
    logging.info(f"Selected feature columns: {Feature_list}")
    print(f"ST ROI Features: {st_rroi_df.shape}")

    # Apply masking
    for dci in config['media_cost_imp_from_daily_files'].keys():
        if config['media_cost_imp_from_daily_files'][dci]:
            mask = pd.Series(False, index=st_rroi_df.index)
            for key, condition in config['cost_imp_to_exclude_from_st_rroi'][dci].items():
                cond_mask = pd.Series(True, index=st_rroi_df.index)
                for column, value in condition.items():
                    cond_mask = cond_mask & (st_rroi_df[column] == value)
                mask = mask | cond_mask
            if dci == 'daily_imp':
                st_rroi_df.loc[mask, ['Impression']] = np.nan
                logging.info(f"Masked {mask.sum()} rows of Impressions")
            else:
                st_rroi_df.loc[mask, ['Cost']] = np.nan
                logging.info(f"Masked {mask.sum()} rows of Cost")

    # Brand-specific handling
    PC = ['Bar', 'BW', 'Deo_F', 'PW DMC', 'Deo DMC', 'Degree_M', 'Degree_F', 'Axe']
    BnW = ['Nexxus', 'Dove', 'Shea_M', 'Tresseme', 'Vaseline']
    NIC = ['Klondike', 'Talenti', 'Yasso', 'Breyers']

    if config["brand"] in PC:
        print("PC brands executing")
        st_rroi_df['Platform'] = np.where(st_rroi_df['Media Type'] == 'Paid Media', 'All', st_rroi_df['Platform'])
        logging.info("PC transformation applied.")
    elif config["brand"] in BnW:
        print("BnW brands executing")
        st_rroi_df['Platform'] = np.where(st_rroi_df['Media Type'] == 'Paid Media', 'Others', st_rroi_df['Platform'])
        logging.info("BnW transformation applied.")
    elif config["brand"] in NIC:
        print("NIC brands executing")
        logging.info("NIC brand - no special transformation.")
    elif config["brand"] == "Kraken":
        print("Kraken executing")
        logging.info("Kraken brand detected.")

    # Fill missing values
    for col in st_rroi_df.columns:
        if col not in ['Year', 'Month']:
            if st_rroi_df[col].dtype == 'object':
                st_rroi_df[col].fillna('None', inplace=True)
            else:
                st_rroi_df[col].fillna(0, inplace=True)
    logging.info("Missing values filled.")
    assert st_rroi_df.isna().sum().sum() == 0, "Missing values remain!"
    logging.info("NA check passed.")

    # Grouping
    str_lst_groupby = [col for col in st_rroi_df.columns if st_rroi_df[col].dtype == 'object' or col in ['Year', 'Month']]
    st_rroi_df = st_rroi_df.groupby(str_lst_groupby).sum().reset_index()
    st_rroi_df = st_rroi_df.replace("None", np.nan)
    logging.info(f"Data grouped by {str_lst_groupby}, new shape: {st_rroi_df.shape}")
    print(f"Grouped ST ROI: {st_rroi_df.shape}")

    # Merge LT and ST
    final_rroi = pd.merge(left=req_format_lt, right=st_rroi_df, on=str_lst_groupby, how='outer')
    logging.info(f"Merged LT & ST ROI, final shape: {final_rroi.shape}")
    print(f"Merged LT & ST: {final_rroi.shape}")

    # Handle KPI names
    kpi_names = set(kpi for kpi in config['kpi'].keys())
    relevant_cols = [col for col in final_rroi.columns if 'attributed' in col.lower() or 'overall' in col.lower()]
    for col in relevant_cols:
        final_rroi[col].fillna(0, inplace=True)
    logging.info(f"Relevant KPI cols filled: {relevant_cols}")

    # Adjust ST sales
    overall_cols = []
    for col in final_rroi.columns:
        if 'Overall' in col:
            for kpi_name in kpi_names:
                if kpi_name in col:
                    overall_cols.append(col)
                    break
    logging.info(f"Overall columns in merged file: {overall_cols}")

    ST_Sales = [i for i in relevant_cols if i.endswith("ST")]
    for st_col in ST_Sales:
        for overall_col in overall_cols:
            st_suffix = st_col.replace('Attributed ', '').replace(' - ST', '')
            overall_suffix = overall_col.replace('Overall ', '')
            if st_suffix == overall_suffix:
                final_rroi[st_col] += final_rroi[overall_col]
                logging.info(f"Adjusted {st_col} with {overall_col}")

    # Drop overall cols
    existing_overall_cols = [col for col in overall_cols if col in final_rroi.columns]
    if existing_overall_cols:
        final_rroi.drop(columns=existing_overall_cols, inplace=True)
        logging.info(f"Dropped redundant overall columns: {existing_overall_cols}")

    # Save output
    output_path = f"./output/ensemble_results/final_rroi_{config['brand']}_edited.xlsx"
    final_rroi.to_excel(output_path, index=False)
    print(f"Final RROI saved at {output_path}, shape: {final_rroi.shape}")
    logging.info(f"Final RROI saved at {output_path}, shape: {final_rroi.shape}")
    logging.info("STROI processing completed successfully.")
    logging.info("-" * 100)

    return final_rroi

    
## SKU models 
## Leading Mertics Models
## Category Models

# if __name__ == "__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Configuration loaded successfully.")
#     except Exception as e:
#         logging.error(f"Failed to load config.json: {e}")
#         raise
    
#     STROI(config)
