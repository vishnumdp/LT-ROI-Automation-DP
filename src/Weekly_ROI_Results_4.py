import re
import numpy as np
import pandas as pd
import os
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(
    filename='./output/logs/weekly_roi_results.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def weekly_results(config):
    results_dict = {}
    metrics_list = config.get("metrics", [])
    metrics_list.append("Pure_Baseline")
    baseline_kpis = config.get("pure_baseline", {})
    roi_numerator = config.get("roi_base_metric", "Weekly sales")
    roi_denominator = "Weighted Impressions"

    logging.info("Starting weekly_results pipeline...")
    logging.info(f"Metrics list: {metrics_list}")
    logging.info(f"ROI Numerator: {roi_numerator}, ROI Denominator: {roi_denominator}")
    print(f"Processing metrics: {metrics_list}")

    for metrics in metrics_list:
        logging.info(f"Processing metric: {metrics}")
        print(f"\n Running for metric: {metrics}")

        if metrics == "Pure_Baseline":
            pure_base_dict = {}

            for kpi_key, kpi_name in baseline_kpis.items():
                logging.info(f"Checking Pure Baseline file for {kpi_key}")
                baseline_file = f"./input/Data/mds_{kpi_key}.xlsx"

                if not os.path.exists(baseline_file):
                    logging.warning(f"Baseline file {baseline_file} not found. Skipping {kpi_key}.")
                    continue

                logging.info(f"Reading baseline file: {baseline_file}")
                weekly_data = pd.read_excel(baseline_file)[["Date", "Baseline"]]
                weekly_data["Date"] = pd.to_datetime(weekly_data["Date"], format=config["date_format"])
                weekly_data.rename(columns={"Baseline": "Pure_Baseline"}, inplace=True)
                weekly_data.set_index('Date', inplace=True)

                weekly_data_t = weekly_data.T
                final = pd.DataFrame(weekly_data_t.unstack()).reset_index()
                final.rename({'level_1': "Metrics", 0: f"{kpi_name}"}, axis=1, inplace=True)

                pure_base_dict[kpi_key] = final.copy()
                logging.info(f"Processed Pure Baseline for {kpi_key}")

            if pure_base_dict:
                logging.info("Merging all Pure Baseline dataframes...")
                pure_base_df = None
                for i, df in enumerate(pure_base_dict.values()):
                    if i == 0:
                        pure_base_df = df
                    else:
                        pure_base_df = pd.merge(pure_base_df, df, on=["Date", "Metrics"], how="left")

                output_path = f'./output/Weekly ROI Format/{config["brand"]}_{metrics}_Weekly_results.xlsx'
                pure_base_df.to_excel(output_path, index=False)
                logging.info(f"Saved Pure Baseline results to {output_path}")
                print(f"Pure Baseline results saved: {output_path}")
            continue

        # For other metrics (MFI, DFI, SFI etc.)
        attr_dict = {}

        for kpi_key, kpi_name in baseline_kpis.items():
            attr_dict[kpi_name] = f"./input/Data/LTROI {config['brand']} Weekly {metrics}.xlsx"

        attr_dict.update({
            "Impressions": f"./input/Data/{config['brand']}_Impressions_unlagged.xlsx",
            "Weighted Impressions": f"./input/Data/{config['brand']}_Impressions_lagged_{metrics}.xlsx",
            "daily_imp": f"./input/Data/{config['brand']}_Daily_Impressions.xlsx",
            "daily_cost": f"./input/Data/{config['brand']}_Daily_Cost.xlsx"
        })

        logging.info(f"Attribute files to read: {attr_dict}")
        final_dict = {}

        for attr_type, file_path in attr_dict.items():
            if not os.path.exists(file_path):
                logging.warning(f"File {file_path} not found for {attr_type}. Skipping.")
                continue

            try:
                logging.info(f"Reading file: {file_path}")
                if attr_type in baseline_kpis.values():
                    weekly_data = pd.read_excel(file_path, sheet_name=attr_type)
                else:
                    weekly_data = pd.read_excel(file_path)
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")
                continue

            weekly_data.fillna(0, inplace=True)
            weekly_data['Date'] = pd.to_datetime(weekly_data['Date'], format=config["date_format"])

            # Rename cleanup
            rename_map = {}
            for col in weekly_data.columns:
                new_col = col
                if 'effect_essence' in col:
                    parts = col.split('|')[:-2]
                    new_col = "|".join(parts)
                if 'Impressions' in new_col or 'Cost' in new_col:
                    parts = [c for c in new_col.split('|') if c not in ["Impressions", "Cost"]]
                    new_col = "|".join(parts)
                if new_col != col:
                    rename_map[col] = new_col

            if rename_map:
                weekly_data.rename(columns=rename_map, inplace=True)
                logging.info(f"Renamed columns for {attr_type}: {rename_map}")

            weekly_data.set_index('Date', inplace=True)
            weekly_data_t = weekly_data.T
            final_dict[attr_type] = pd.DataFrame(weekly_data_t.unstack()).reset_index()
            final_dict[attr_type].rename({'level_1': "Merged Granularity", 0: f"{attr_type}"}, axis=1, inplace=True)

            output_attr_path = f'./output/Weekly ROI Format/LT_{attr_type}_{metrics}.xlsx'
            final_dict[attr_type].to_excel(output_attr_path, sheet_name=attr_type, index=False)
            logging.info(f"Saved attribute results for {attr_type} to {output_attr_path}")

        logging.info("Merging KPI dataframes...")
        merged_final = None
        for i, kpi_name in enumerate(baseline_kpis.values()):
            if kpi_name in final_dict:
                if merged_final is None:
                    merged_final = final_dict[kpi_name]
                else:
                    merged_final = pd.merge(merged_final, final_dict[kpi_name], on=['Date', 'Merged Granularity'], how='inner')

        if merged_final is None:
            logging.warning(f"No KPI data found for {metrics}. Skipping merge.")
            print(f"Skipped {metrics}: no KPI data found")
            continue

        logging.info("Adding expected sales previous weeks...")
        prev_dates = list(pd.date_range(config["expected_sales_start"], config["model_start_date"], freq='W-SUN'))
        df_prev = pd.DataFrame(columns=merged_final.columns)

        for i in prev_dates:
            df_temp = merged_final[merged_final["Date"] == config["model_end_date"]].reset_index(drop=True)
            df_temp["Date"] = i
            for col in baseline_kpis.values():
                if col in df_temp.columns:
                    df_temp[col] = 0
            df_prev = pd.concat([df_prev, df_temp], axis=0).reset_index(drop=True)

        merged_final = pd.concat([df_prev, merged_final], axis=0).reset_index(drop=True)

        for extra in ["Impressions", "Weighted Impressions"]:
            if extra in final_dict:
                merged_final = pd.merge(merged_final, final_dict[extra], on=["Date", "Merged Granularity"], how="left")

        logging.info("Calculating ROI...")
        merged_final["Actual ROI"] = 0.0
        if roi_numerator in merged_final.columns and roi_denominator in merged_final.columns:
            non_zero_mask = merged_final[roi_denominator] != 0
            merged_final.loc[non_zero_mask, "Actual ROI"] = (
                merged_final.loc[non_zero_mask, roi_numerator] / merged_final.loc[non_zero_mask, roi_denominator]
            )
            logging.info(f"ROI calculated using {roi_numerator} / {roi_denominator}")

        # Granularity split
        logging.info("Splitting granularity into dimensions...")
        if config["ProductLine_Flag"] == 1:
            merged_final[["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]] = merged_final["Merged Granularity"].str.split("|", expand=True)
        elif config["ProductLine_Flag"] == 2:
            merged_final[["Media Type", "Product Line", "Master Channel", "Channel"]] = merged_final["Merged Granularity"].str.split("|", expand=True)
        else:
            merged_final[["Media Type", "Master Channel", "Channel", "Platform"]] = merged_final["Merged Granularity"].str.split("|", expand=True)
            merged_final["Product Line"] = "ALL"

        merged_final.drop(columns=["Merged Granularity"], inplace=True)

        output_path = f"./output/Weekly ROI Format/{config['brand']}_{metrics}_Weekly_results.xlsx"
        merged_final.to_excel(output_path, index=False)
        logging.info(f"Saved weekly results for {metrics} at {output_path}")
        print(f"Weekly results saved: {output_path}")

        results_dict[metrics] = merged_final

    logging.info("All metrics processed successfully.")
    print("\nAll metrics processed successfully.")
    logging.info("-"*100)
    return results_dict


# if __name__ == "__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Config file loaded successfully.")
#         print("Config file loaded successfully.")
#     except Exception as e:
#         logging.exception("Failed to load config file.")
#         raise

#     results = weekly_results(config)
#     print("Final dictionary keys:", list(results.keys()))
