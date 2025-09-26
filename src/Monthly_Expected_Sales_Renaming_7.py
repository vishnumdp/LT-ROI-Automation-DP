import re
import numpy as np
import pandas as pd
import boto3
import os
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# Logging Setup
logging.basicConfig(
    filename='./output/logs/expected_sales.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def transform_dataframe(df_p, config):
    """Transform dataframe based on ProductLine_Flag rules."""
    logging.info("Applying transform_dataframe...")
    df = df_p.copy()

    df['Product Line'] = df.apply(
        lambda row: row['Master Channel'] if row['Media Type'] != config['brand'] else config['brand'],
        axis=1
    )

    df['Media Type'] = df['Media Type'].replace(config['brand'], 'Paid Media')
    df = df.replace("None", np.nan)

    logging.info("Transformation inside transform_dataframe completed.")
    return df


def process_expected_sales(config):
    try:
        final_df_dict = {}
        metrics_list = config.get("metrics", [])
        metrics_list.append("Pure_Baseline")
        logging.info(f"Metrics to process: {metrics_list}")
        print(f"Processing metrics: {metrics_list}")

        # Step 1: Load all metric files
        for i, metric in enumerate(metrics_list):
            file_path = f"./output/Extrapolated Data/monthly_expected_sales_{config['brand']}_{metric}.xlsx"
            logging.info(f"Loading metric {metric} from {file_path}")
            df = pd.read_excel(file_path)
            print(f"Loaded {metric}: {df.shape}")

            # Handle Pure Baseline
            if metric == "Pure_Baseline":
                rename_map = {
                    col: f"Attributed {col.split(' ', 1)[1]} - ST"
                    for col in df.columns if col.lower().startswith("weekly ")
                }
                df.rename(columns=rename_map, inplace=True)
                df['Media Type'] = 'Baseline'
                logging.info(f"Pure_Baseline renamed columns: {rename_map}")

            else:
                # Metric Handling
                rename_map = {}
                for col in df.columns:
                    if col.lower().startswith("weekly "):
                        base_name = col.split(" ", 1)[1]
                        rename_map[col] = f"Monthly {base_name}-{metric}"
                    elif col.lower().startswith("expected "):
                        base_name = col.split(" ", 1)[1]
                        rename_map[col] = f"Expected {base_name}-{metric}"

                df.rename(columns=rename_map, inplace=True)
                logging.info(f"{metric} renamed columns: {rename_map}")

                # Drop duplicate columns
                if i > 0:
                    for col in ['Weighted Impressions', 'Impressions']:
                        if col in df.columns:
                            df.drop(columns=[col], inplace=True)
                            logging.info(f"Dropped column {col} from {metric}")

            final_df_dict[metric] = df

        # Step 2: Merge metrics
        lt_res = final_df_dict[config['metrics'][0]].copy()
        logging.info("Starting merge of metrics into lt_res")

        if config['ProductLine_Flag'] == 1:
            logging.info("Merging using ProductLine_Flag = 1")
            for metric in config['metrics'][1:]:
                if metric != "Pure_Baseline":
                    lt_res = pd.merge(
                        lt_res,
                        final_df_dict[metric],
                        on=['Media Type', 'Product Line', 'Master Channel', 'Channel',
                            'Platform', 'Year', 'Month'],
                        how='left'
                    )
                    logging.info(f"Merged {metric} into lt_res, shape now {lt_res.shape}")

        elif config['ProductLine_Flag'] == 2:
            logging.info("Merging using ProductLine_Flag = 2")
            for metric in config['metrics'][1:]:
                if metric != "Pure_Baseline":
                    lt_res = pd.merge(
                        lt_res,
                        final_df_dict[metric],
                        on=['Media Type', 'Product Line', 'Master Channel', 'Channel', 'Year', 'Month'],
                        how='left'
                    )
                    logging.info(f"Merged {metric} into lt_res, shape now {lt_res.shape}")

        # Merge Pure Baseline
        lt_res = pd.merge(
            lt_res,
            final_df_dict['Pure_Baseline'],
            on=['Media Type', 'Year', 'Month'],
            how='left'
        )
        logging.info("Merged Pure_Baseline into lt_res")
        print(f"Shape after all merges: {lt_res.shape}")

        # Step 3: Create LT and Expected Groups
        monthly_groups, expected_groups = {}, {}

        for col in lt_res.columns:
            if col.startswith("Monthly "):
                base_name = col.split("-")[0].replace("Monthly ", "")
                monthly_groups.setdefault(base_name, []).append(col)

        for base_name, cols in monthly_groups.items():
            lt_res[f"Attributed {base_name} - LT"] = lt_res[cols].sum(axis=1)
            logging.info(f"Created Attributed {base_name} - LT")

        for col in lt_res.columns:
            if col.startswith("Expected "):
                base_name = col.split("-")[0].replace("Expected ", "")
                expected_groups.setdefault(base_name, []).append(col)

        for base_name, cols in expected_groups.items():
            lt_res[f"Total Expected {base_name}"] = lt_res[cols].sum(axis=1)
            logging.info(f"Created Total Expected {base_name}")

        logging.info("LT and Expected groups created.")
        print("Created LT and Expected groups.")

        # Step 4: Transformation based on ProductLine flag
        if config['ProductLine'] == True:
            logging.info("Executing transformation with ProductLine=True")
            req_format_lt = lt_res.copy().replace("None", np.nan)
        else:
            logging.info("Executing transform_dataframe function")
            req_format_lt = transform_dataframe(lt_res, config)

        save_path = f"./output/Extrapolated Data/Only_LT_lt_rroi_{config['brand']}_Original_Platform.xlsx"
        req_format_lt.to_excel(save_path, index=False)
        logging.info(f"Saved intermediate LT results to {save_path}")
        print(f"Saved Original Platform file: {save_path}")

        # Step 5: Brand Specific Handling
        PC = ['Bar','BW','Deo_F','PW DMC','Deo DMC','Degree_M','Degree_F','Axe']
        BnW = ['Nexxus','Dove','Shea_M','Tresseme','Vaseline']
        NIC = ['Klondike','Talenti','Yasso','Breyers']

        brand_save_path = f"./output/Extrapolated Data/Only_LT_lt_rroi_{config['brand']}.xlsx"

        if config["brand"] in PC:
            logging.info("PC brand detected. Applying PC transformation.")
            print("PC brands is executing")
            req_format_lt['Platform'] = np.where(
                (req_format_lt['Media Type'] == 'Paid Media') & 
                (req_format_lt['Channel'] != 'Digital Video'),
                'All',
                req_format_lt['Platform']
            )
            req_format_lt.to_excel(brand_save_path, index=False)

        elif config["brand"] in BnW:
            logging.info("BnW brand detected.")
            print("BnW brands is executing")
            req_format_lt.to_excel(brand_save_path, index=False)

        elif config["brand"] in NIC:
            logging.info("NIC brand detected.")
            print("NIC brands is executing")
            req_format_lt.to_excel(brand_save_path, index=False)

        elif config["brand"] == "Kraken":
            logging.info("Kraken brand detected.")
            print("Kraken is executing")
            req_format_lt.to_excel(brand_save_path, index=False)

        logging.info("Final results saved successfully.")
        print(f"Final results saved at {brand_save_path}")

        logging.info("-" * 100)
        return req_format_lt

    except Exception as e:
        logging.exception("Error during process_expected_sales execution.")
        print(f"Error occurred: {e}")
        raise


# if __name__ == "__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Config file loaded successfully.")
#     except FileNotFoundError:
#         logging.exception("Config file not found.")
#         raise
#     final_result = process_expected_sales(config)
#     logging.info("Expected sales processing finished successfully.")
#     print(final_result.head(10))
