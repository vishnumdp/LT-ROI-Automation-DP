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
logging.basicConfig(
    filename='../output/logs/monthly_expected_sales.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def generate_expected_sales(config):
    final_df_dict = {}

    # start nearest Sunday
    input_date = pd.to_datetime(config["expected_sales_start"])
    days_until_sunday = (6 - input_date.weekday()) % 7
    nearest_sunday = input_date + timedelta(days=days_until_sunday)

    all_date = pd.date_range(nearest_sunday, config["model_end_date"], freq='D')
    df_ratio_temp = pd.DataFrame({"Date": all_date})

    # Load base daily ratio file
    daily_ratio_temp_1 = pd.read_excel("../input/Data/Kraken_base_ntus_daily_ratio_for_lt.xlsx")
    df_daily_ratio = pd.merge(df_ratio_temp, daily_ratio_temp_1, on="Date", how="left").fillna(0)

    all_date_weekly = pd.date_range(nearest_sunday, config["model_end_date"], freq='W')
    temp_all_date_weekly = pd.DataFrame({"Date": all_date_weekly})

    # Mapping weekly ratio columns to daily ratio columns
    div_with_sales = {
        f"Weekly {col.split('Base ')[1].replace('Ratio', '').strip()}": col
        for col in df_daily_ratio.columns
        if col.startswith("Base ")
    }
    print(div_with_sales)

    # Metrics list
    metrics_lst = config.get("metrics", [])
    metrics_lst.append("Pure_Baseline")

    # Expected columns to always check
    expected_numeric_cols = ["Weighted Impressions", "Expected Simple Sales", "Expected Weighted Sales"]
    expected_string_cols = ["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]

    for metrics in metrics_lst:
        logging.info(f"Processing metric: {metrics}")
        print(f"\nProcessing metric: {metrics}")

        if metrics == "Pure_Baseline":
            expected_sales_df = pd.read_excel(
                f"../output/Weekly ROI Format/{config['brand']}_{metrics}_Weekly_results.xlsx"
            )
            expected_sales_df = pd.merge(temp_all_date_weekly, expected_sales_df, on="Date", how="left")
            expected_sales_df["Metrics"] = "Pure_Baseline"
            expected_sales_df.fillna(0, inplace=True)
        else:
            expected_sales_df = pd.read_excel(
                f"../output/Extrapolated Data/LTROI_{config['brand']}_rroi_{metrics}.xlsx"
            )

        req_weekly_df = expected_sales_df.copy()

        # Drop 'Impressions' if it exists
        try:
            req_weekly_df.drop(columns=['Impressions'], inplace=True)
        except:
            pass

        # Identify numeric and string columns
        exclude_cols = ['Date', 'Actual ROI', 'Year', 'Week', 'Expected Simple ROI', 'Expected Weighted ROI']

        num_col = [
            col for col in req_weekly_df.columns
            if col not in exclude_cols and pd.api.types.is_numeric_dtype(req_weekly_df[col])
        ]
        
        str_col = [
            col for col in req_weekly_df.columns
            if col not in num_col + exclude_cols
        ]

        print(f"String columns for {metrics}: {str_col}")
        print(f"Numerical columns for {metrics}: {num_col}")

        # Check for numeric columns
        for col in num_col:
            if col in req_weekly_df.columns:
                req_weekly_df[col].fillna(0, inplace=True)
            else:
                print(f"{col} not in Weekly {metrics} file")
                logging.warning(f"{col} not in Weekly {metrics} file")

        # Check for string columns
        for col in str_col:
            if col in req_weekly_df.columns:
                req_weekly_df[col].fillna("None", inplace=True)
            else:
                print(f"{col} not in Weekly {metrics} file")
                logging.warning(f"{col} not in Weekly {metrics} file")

        # Prepare 'name' column
        if metrics != "Pure_Baseline":
            req_weekly_df["name"] = req_weekly_df[str_col].apply(lambda x: "|".join(x[:]), axis=1)
            req_weekly_df = req_weekly_df[["Date"] + str_col + num_col + ["name"]]
        else:
            req_weekly_df.rename(columns={"Metrics": "name"}, inplace=True)

        df_final = pd.DataFrame(columns=req_weekly_df.columns)

        for nm in req_weekly_df["name"].unique():
            df_temp = req_weekly_df[req_weekly_df["name"] == nm].reset_index(drop=True)
            df_temp.set_index("Date", inplace=True)

            # Daily resampling
            df_daily = df_temp.resample("D").bfill().copy().reset_index()
            assert df_daily.isna().sum().sum() == 0, "df_daily contains null values"

            # Spread weekly values across days
            if metrics != "Pure_Baseline":
                equal_div = ["Weighted Impressions", "Expected Simple Sales", "Expected Weighted Sales"]
                for v in equal_div:
                    if v in df_daily.columns:
                        df_daily.loc[0, v] /= (3 / 7)
                        df_daily.loc[1:, v] /= 7

            # Multiply by ratios
            for v in div_with_sales.keys():
                ratio_col = div_with_sales[v]
                if v in df_daily.columns and ratio_col in df_daily_ratio.columns:
                    assert len(df_daily[v]) == len(df_daily_ratio[ratio_col]), "df_daily and ratio len mismatch"
                    df_daily[v] = df_daily_ratio[ratio_col] * df_daily[v]

            df_daily = df_daily[req_weekly_df.columns]
            df_final = pd.concat([df_final, df_daily], axis=0).reset_index(drop=True)

        # Add Year and Month columns
        df_final["Year"] = df_final["Date"].dt.year
        df_final["Month"] = df_final["Date"].dt.month
        df_final.drop(columns=["Date", "name"], inplace=True)

        # Define grouping columns
        if metrics != "Pure_Baseline":
            group_cols = str_col + ["Year", "Month"]
        else:
            group_cols = ["Year", "Month"]
            if 'Media Type' in df_final.columns:
                group_cols.append('Media Type')

        print(f"Grouping columns for {metrics}: {group_cols}")
        df_final = df_final.groupby(group_cols).sum().reset_index()

        print(f"Available columns for {metrics}:", df_final.columns)

        # Sorting if 'Media Type' exists
        # media_type_order = ['Baseline','Earned Media','Halo','Masterbrand','Non Media','Owned Media','Paid Media','Trade Promo']
        # if 'Media Type' in df_final.columns:
        #     df_final['Media Type'] = pd.Categorical(df_final['Media Type'], categories=media_type_order, ordered=True)
        #     df_sorted_list = []
        #     for media in media_type_order:
        #         temp = df_final[df_final['Media Type'] == media]
        #         if not temp.empty:
        #             temp_sorted = temp.sort_values(by=['Year', 'Month'])
        #             df_sorted_list.append(temp_sorted)
        #     df_final = pd.concat(df_sorted_list, axis=0).reset_index(drop=True)
        #     print(f"'Media Type', 'Year', 'Month' sorting applied for {metrics}")
        # else:
        #     df_final = df_final.sort_values(by=['Year', 'Month'])
        #     print(f"'Year' and 'Month' sorting applied for {metrics}, 'Media Type' not found.")

        # Save the file
        final_df_dict[metrics] = df_final
        output_path = f"../output/Extrapolated Data/monthly_expected_sales_{config['brand']}_{metrics}.xlsx"
        df_final.to_excel(output_path, index=False)
        print(f"Saved file for {metrics} at {output_path}")
        logging.info(f"Saved file for {metrics} at {output_path}")
        
    logging.info(f"-"*100)
    logging.info("All metrics processed successfully.")
    return final_df_dict

if __name__ == "__main__":
    # Load config file
    try:
        with open("../input/config/config.json", "r") as file:
            config = json.load(file)
        logging.info("Config file loaded successfully.")
    except FileNotFoundError:
        logging.exception("Config file not found.")
        raise
    results = generate_expected_sales(config)
