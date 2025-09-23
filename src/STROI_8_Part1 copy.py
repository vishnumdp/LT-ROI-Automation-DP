import pandas as pd
import logging
import os
import json
import numpy as np
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# Create output directories
path_lst = ['ensemble_results', 'Extrapolated Data', 'Weekly ROI Format', 'Weighted Cost', 'logs']
for path in path_lst:
    os.makedirs(f"./output/{path}", exist_ok=True)

# Setup logging
try:
    logging.basicConfig(
        filename='./output/logs/STROI_part 1.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging initialized.")
except Exception as e:
    print("Some Issue in creating log file:", e)


# from Weekly_ROI_Results_4 import weekly_results

# Transform DataFrame helper
def transform_dataframe(df, config):
    kpi = config.get('kpi_name', 'Unknown_KPI')
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
    logging.info("Dataframe transformed.")
    return transformed_df

# Main STROI function
def STROI(config):
    logging.info("STROI processing started.")

    try:
        # Read input files
        req_format_lt = pd.read_excel(f"./output/Extrapolated Data/Only_LT_lt_rroi_{config['brand']}.xlsx")
        req_format_lt.rename(columns={'Channel/Daypart':'Channel'},inplace=True)
        logging.info(f"Loaded {config['brand']} LT ROI file.")

        st_rroi_df = pd.read_excel(f"./input/Data/ST ROI.xlsx", sheet_name='RROI')
        st_rroi_df.rename(columns={'Channel/Daypart':'Channel'},inplace=True)
        st_rroi_df.rename(columns={'NTUs':'Overall NTUs'},inplace=True)
        st_rroi_df['Date'] = pd.to_datetime(st_rroi_df[['Year', 'Month']].assign(day=1))
        logging.info("ST ROI file loaded.")
    except Exception as e:
        logging.error(f"Error loading input files: {e}")
        raise

    # Date calculations
    start_year = pd.to_datetime(config['expected_sales_start'], format=config['date_format']).year
    start_month = pd.to_datetime(config['expected_sales_start'], format=config['date_format']).month
    m_start_year = pd.to_datetime(config['model_start_date'], format=config['date_format']).year
    m_start_month = pd.to_datetime(config['model_start_date'], format=config['date_format']).month
    end_year = pd.to_datetime(config['model_end_date'], format=config['date_format']).year
    end_month = pd.to_datetime(config['model_end_date'], format=config['date_format']).month
    sd_temp = datetime(start_year, start_month, 1)
    ed_temp = datetime(end_year, end_month, 1)
    msd_temp = datetime(m_start_year, m_start_month, 1)
    logging.info("Date ranges calculated.")

    # Filter and rename
    st_rroi_df = st_rroi_df[(st_rroi_df["Date"] >= sd_temp) & (st_rroi_df["Date"] <= ed_temp)].reset_index(drop=True)
    st_rroi_df.rename(columns={'Overall Volume': 'Overall Units'}, inplace=True)
    logging.info("Filtered and renamed ST ROI dataframe.")

    overall_cols = [col for col in st_rroi_df.columns if 'Overall' in col]
    logging.info(f"Identified overall columns: {overall_cols}")

    for col in overall_cols:
        print("col",col)
        st_rroi_df.loc[
            (st_rroi_df['Date'] >= msd_temp) &
            (st_rroi_df['Date'] <= ed_temp) &
            (st_rroi_df['Media Type'] == 'Baseline'), col
        ] = 0.0
    logging.info("Set 'Baseline' overall columns to zero for applicable dates.")

    if config['ProductLine_Flag'] == 1:
        Feature_list = ['Media Type', 'Product Line', 'Master Channel', 'Channel','Platform','Year', 'Month','Cost', 'Impression']
    elif config['ProductLine_Flag'] == 2:
        Feature_list = ['Media Type', 'Product Line', 'Master Channel', 'Channel','Year', 'Month','Cost', 'Impression'] 

    Feature_list.extend(overall_cols)
    st_rroi_df = st_rroi_df[Feature_list]
    print("st_rroi_df",st_rroi_df.columns)
    logging.info("Selected relevant features.")

    # Apply masking based on config
    for dci in config['media_cost_imp_from_daily_files'].keys():
        if config['media_cost_imp_from_daily_files'][dci]:
            mask = pd.Series(False, index=st_rroi_df.index)
            for key, condition in config['cost_imp_to_exclude_from_st_rroi'][dci].items():
                cond_mask = pd.Series(True, index=st_rroi_df.index)
                for column, value in condition.items():
                    cond_mask = cond_mask & (st_rroi_df[column] == value)    
                mask = mask | cond_mask
            if dci == 'daily_imp':
                logging.info(f"Masking Impressions for {mask.sum()} rows.")
                st_rroi_df.loc[mask, ['Impression']] = np.nan
            else:
                logging.info(f"Masking Cost for {mask.sum()} rows.")
                st_rroi_df.loc[mask, ['Cost']] = np.nan

    # Fill missing values
    # st_rroi_df['Platform'] = np.where(st_rroi_df['Media Type'] == 'Paid Media','All', st_rroi_df['Platform'])
    # logging.info("Filled 'Platform' column.")

    if config['brand'] != "Kraken" :
        st_rroi_df['Platform'] = np.where(st_rroi_df['Media Type'] == 'Paid Media','All', st_rroi_df['Platform'])
        logging.info("Filled 'Platform' column.")
    else:
        # st_rroi_df['Platform'] = np.where(st_rroi_df['Media Type'] == 'Paid Media','All', st_rroi_df['Platform'])
        logging.info("Continuing without Platform feature")

    for col in st_rroi_df.columns:
        if col not in ['Year', 'Month']:
            if st_rroi_df[col].dtype == 'object':
                st_rroi_df[col].fillna('None', inplace=True)
            else:
                st_rroi_df[col].fillna(0, inplace=True)
    logging.info("Filled missing values in dataframe.")

    assert st_rroi_df.isna().sum().sum() == 0, "Missing values still exist!"
    logging.info("Missing values assertion passed.")

    # Grouping
    str_lst_groupby = [col for col in st_rroi_df.columns if st_rroi_df[col].dtype == 'object' or col in ['Year', 'Month']]
    st_rroi_df = st_rroi_df.groupby(str_lst_groupby).sum().reset_index()
    st_rroi_df = st_rroi_df.replace("None", np.nan)
    logging.info("Grouped dataframe and replaced 'None' with NaN.")

    # Merge with LT data
    # req_format_lt.rename(columns={'Channel':'Channel/Daypart'}, inplace=True)
    final_rroi = pd.merge(left=req_format_lt, right=st_rroi_df, on=str_lst_groupby, how='outer')
    logging.info("Merged LT and ST dataframes.")

    # Fill relevant columns
    kpi_names = set(kpi for kpi in config['kpi'].keys())
    relevant_cols = [col for col in final_rroi.columns if 'attributed' in col.lower() or 'overall' in col.lower()]
    for col in relevant_cols:
        final_rroi[col].fillna(0, inplace=True)
    logging.info(f"Filled missing values in relevant columns: {relevant_cols}")

    # Identify overall columns
    overall_cols = []
    for col in final_rroi.columns:
        if 'Overall' in col:
            for kpi_name in kpi_names:
                if kpi_name in col:
                    overall_cols.append(col)
                    break
    logging.info(f"Identified overall columns: {overall_cols}")

    # Adjust ST sales
    ST_Sales = [i for i in relevant_cols if i.endswith("ST")]
    for st_col in ST_Sales:
        for overall_col in overall_cols:
            st_suffix = st_col.replace('Attributed ', '').replace(' - ST', '')
            overall_suffix = overall_col.replace('Overall ', '')
            if st_suffix == overall_suffix and st_col in final_rroi.columns and overall_col in final_rroi.columns:
                final_rroi[st_col] += final_rroi[overall_col]
    logging.info("Adjusted ST sales with overall sales.")

    existing_overall_cols = [col for col in overall_cols if col in final_rroi.columns]
    if existing_overall_cols:
        final_rroi.drop(columns=existing_overall_cols, inplace=True)
        logging.info(f"Dropped overall columns: {existing_overall_cols}")

    # Save final_rroi to Excel
    output_path = f"./output/ensemble_results/final_rroi_{config['brand']}_edited.xlsx"
    final_rroi.to_excel(output_path, index=False)
    print(f"final_rroi saved at {output_path}")
    logging.info(f"final_rroi saved at {output_path}")
    logging.info("STROI processing completed successfully.")
    logging.info(f"-"*100)
    return final_rroi
    
## SKU models 
## Leading Mertics Models
## Category Models

if __name__ == "__main__":
    # Load configuration
    try:
        with open("./input/config/config.json", "r") as file:
            config = json.load(file)
        logging.info("Configuration loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load config.json: {e}")
        raise
    
    STROI(config)
