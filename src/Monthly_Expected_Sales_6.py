import re
import numpy as np
import pandas as pd
import boto3
import os
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import warnings

warnings.filterwarnings('ignore')

# Setup logging
log_dir = "./output/logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "monthly_expected_sales.log"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def generate_expected_sales(config):
    final_df_dict = {}

    try:
        # nearest Sunday
        input_date = pd.to_datetime(config["expected_sales_start"])
        days_until_sunday = (6 - input_date.weekday()) % 7
        nearest_sunday = input_date + timedelta(days=days_until_sunday)
        logging.info(f"Nearest Sunday calculated: {nearest_sunday}")
        print(f"Nearest Sunday: {nearest_sunday}")
    except Exception as e:
        logging.exception(f"Error calculating nearest Sunday: {e}")
        raise

    try:
        all_date = pd.date_range(nearest_sunday, config["model_end_date"], freq='D')
        df_ratio_temp = pd.DataFrame({"Date": all_date})
        logging.info(f"Daily date range created with {len(all_date)} days")
    except Exception as e:
        logging.exception(f"Error generating daily date range: {e}")
        raise

    try:
        daily_ratio_temp_1 = pd.read_excel(f"./input/Data/{config['brand']}_daily_ratio_for_lt.xlsx")
        df_daily_ratio = pd.merge(df_ratio_temp, daily_ratio_temp_1, on="Date", how="left").fillna(0)
        logging.info("Daily ratio file loaded and merged successfully")
        print("Daily ratio file loaded")
    except Exception as e:
        logging.exception(f"Error loading daily ratio file: {e}")
        raise

    all_date_weekly = pd.date_range(nearest_sunday, config["model_end_date"], freq='W')
    temp_all_date_weekly = pd.DataFrame({"Date": all_date_weekly})
    logging.info(f"Weekly date range created with {len(all_date_weekly)} weeks")

    div_with_sales = {
        f"Weekly {col.split('Base ')[1].replace('Ratio', '').strip()}": col
        for col in df_daily_ratio.columns
        if col.startswith("Base ")
    }
    logging.info(f"Division with sales mapping created: {div_with_sales}")
    print("Division with sales mapping:", div_with_sales)

    metrics_lst = config.get("metrics", [])
    metrics_lst.append("Pure_Baseline")
    logging.info(f"Metrics to process: {metrics_lst}")

    for metrics in metrics_lst:
        logging.info(f"Processing metric: {metrics}")
        print(f"\nProcessing metric: {metrics}")

        try:
            if metrics == "Pure_Baseline":
                input_path = f"./output/Weekly ROI Format/{config['brand']}_{metrics}_Weekly_results.xlsx"
                expected_sales_df = pd.read_excel(input_path)
                expected_sales_df = pd.merge(temp_all_date_weekly, expected_sales_df, on="Date", how="left")
                expected_sales_df["Metrics"] = "Pure_Baseline"
                expected_sales_df.fillna(0, inplace=True)
                logging.info(f"Pure_Baseline file loaded: {input_path}, shape={expected_sales_df.shape}")
            else:
                input_path = f"./output/Extrapolated Data/LTROI_{config['brand']}_rroi_{metrics}.xlsx"
                expected_sales_df = pd.read_excel(input_path)
                logging.info(f"File loaded for {metrics}: {input_path}, shape={expected_sales_df.shape}")
        except Exception as e:
            logging.exception(f"Error loading file for {metrics}: {e}")
            continue

        req_weekly_df = expected_sales_df.copy()

        if "Impressions" in req_weekly_df.columns:
            req_weekly_df.drop(columns=['Impressions'], inplace=True)

        exclude_cols = ['Date', 'Actual ROI', 'Year', 'Week', 'Expected Simple ROI', 'Expected Weighted ROI']

        num_col = [
            col for col in req_weekly_df.columns
            if col not in exclude_cols and pd.api.types.is_numeric_dtype(req_weekly_df[col])
        ]
        str_col = [
            col for col in req_weekly_df.columns
            if col not in num_col + exclude_cols
        ]

        logging.info(f"For {metrics}: String cols={str_col}, Numeric cols={num_col}")
        print(f"String columns for {metrics}: {str_col}")
        print(f"Numeric columns for {metrics}: {num_col}")

        # Fill NA in numeric columns
        for col in num_col:
            req_weekly_df[col].fillna(0, inplace=True)

        # Fill NA in string columns
        for col in str_col:
            req_weekly_df[col].fillna("None", inplace=True)

        # Prepare 'name' column
        if metrics != "Pure_Baseline":
            req_weekly_df["name"] = req_weekly_df[str_col].apply(lambda x: "|".join(x[:]), axis=1)
            req_weekly_df = req_weekly_df[["Date"] + str_col + num_col + ["name"]]
        else:
            req_weekly_df.rename(columns={"Metrics": "name"}, inplace=True)

        df_final = pd.DataFrame(columns=req_weekly_df.columns)

        for nm in req_weekly_df["name"].unique():
            logging.info(f"Processing group: {nm}")
            df_temp = req_weekly_df[req_weekly_df["name"] == nm].reset_index(drop=True)
            df_temp.set_index("Date", inplace=True)

            try:
                df_daily = df_temp.resample("D").bfill().copy().reset_index()
                assert df_daily.isna().sum().sum() == 0, f"df_daily contains null values for {nm}"
                logging.info(f"Resampled daily df for {nm}, shape={df_daily.shape}")
            except Exception as e:
                logging.exception(f"Error in resampling {nm}: {e}")
                continue

            if metrics != "Pure_Baseline":
                equal_div = ["Weighted Impressions", "Expected Simple Sales", "Expected Weighted Sales"]
                for v in equal_div:
                    if v in df_daily.columns:
                        df_daily.loc[0, v] /= (3 / 7)
                        df_daily.loc[1:, v] /= 7
                        logging.debug(f"Divided column {v} for {nm}")

            for v in div_with_sales.keys():
                ratio_col = div_with_sales[v]
                if v in df_daily.columns and ratio_col in df_daily_ratio.columns:
                    try:
                        assert len(df_daily[v]) == len(df_daily_ratio[ratio_col]), \
                            f"Length mismatch: {v} vs {ratio_col}"
                        df_daily[v] = df_daily_ratio[ratio_col] * df_daily[v]
                        logging.debug(f"Applied ratio for {v} using {ratio_col}")
                    except Exception as e:
                        logging.exception(f"Error applying ratio for {v}: {e}")

            df_daily = df_daily[req_weekly_df.columns]
            df_final = pd.concat([df_final, df_daily], axis=0).reset_index(drop=True)

        df_final["Year"] = df_final["Date"].dt.year
        df_final["Month"] = df_final["Date"].dt.month
        df_final.drop(columns=["Date", "name"], inplace=True)

        if metrics != "Pure_Baseline":
            group_cols = str_col + ["Year", "Month"]
        else:
            group_cols = ["Year", "Month"]
            if 'Media Type' in df_final.columns:
                group_cols.append('Media Type')

        logging.info(f"Grouping columns for {metrics}: {group_cols}")
        print(f"Grouping columns for {metrics}: {group_cols}")

        try:
            df_final = df_final.groupby(group_cols).sum().reset_index()
            logging.info(f"Grouped df for {metrics}, shape={df_final.shape}")
            print(f"Available columns for {metrics}:", df_final.columns.tolist())
        except Exception as e:
            logging.exception(f"Error in grouping {metrics}: {e}")
            continue

        final_df_dict[metrics] = df_final
        output_path = f"./output/Extrapolated Data/monthly_expected_sales_{config['brand']}_{metrics}.xlsx"
        try:
            df_final.to_excel(output_path, index=False)
            logging.info(f"Saved file for {metrics}: {output_path}")
            print(f"Saved file for {metrics} at {output_path}")
        except Exception as e:
            logging.exception(f"Error saving file for {metrics}: {e}")

    logging.info("-" * 100)
    logging.info("All metrics processed successfully.")
    print("All metrics processed successfully.")

    return final_df_dict


# if __name__ == "__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Config file loaded successfully.")
#     except FileNotFoundError:
#         logging.exception("Config file not found.")
#         raise
#     results = generate_expected_sales(config)
