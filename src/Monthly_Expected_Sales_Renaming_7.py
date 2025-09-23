import re
import numpy as np
import pandas as pd
import boto3
import os
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

logging.basicConfig(
    filename='../output/logs/expected_sales.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)




def transform_dataframe(df_p, config):
    """Transform dataframe based on ProductLine_Flag rules."""
    df = df_p.copy()

    df['Product Line'] = df.apply(
        lambda row: row['Master Channel'] if row['Media Type'] != config['brand'] else config['brand'],
        axis=1
    )

    df['Media Type'] = df['Media Type'].replace(config['brand'], 'Paid Media')
    df = df.replace("None", np.nan)

    return df


# Main Processing Function 
def process_expected_sales(config):
    try:
        final_df_dict = {}

        metrics_list = config.get("metrics", [])
        metrics_list.append("Pure_Baseline")

        for i, metric in enumerate(metrics_list):
            logging.info(f"Loading metric: {metric}")
            df = pd.read_excel(
                f"../output/Extrapolated Data/monthly_expected_sales_{config['brand']}_{metric}.xlsx"
            )

            # Pure Baseline Handling
            if metric == "Pure_Baseline":
                rename_map = {}
                for col in df.columns:
                    if col.lower().startswith("weekly "):
                        base_name = col.split(" ", 1)[1]
                        rename_map[col] = f"Attributed {base_name} - ST"
                df.rename(columns=rename_map, inplace=True)
                df['Media Type'] = 'Baseline'

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

                if i > 0:
                    for col in ['Weighted Impressions', 'Impressions']:
                        if col in df.columns:
                            df.drop(columns=[col], inplace=True)

            final_df_dict[metric] = df


        lt_res = final_df_dict[config['metrics'][0]].copy()

        if config['ProductLine_Flag'] == 1:
            for metric in config['metrics'][1:]:
                if metric == "Pure_Baseline":
                    continue
                lt_res = pd.merge(
                    lt_res,
                    final_df_dict[metric],
                    on=['Media Type', 'Product Line', 'Master Channel', 'Channel',
                        'Platform', 'Year', 'Month'],
                    how='left'
                )

        elif config['ProductLine_Flag'] == 2:
            for metric in config['metrics'][1:]:
                if metric == "Pure_Baseline":
                    continue
                lt_res = pd.merge(
                    lt_res,
                    final_df_dict[metric],
                    on=['Media Type', 'Product Line', 'Master Channel', 'Channel', 'Year', 'Month'],
                    how='left'
                )

        lt_res = pd.merge(
            lt_res,
            final_df_dict['Pure_Baseline'],
            on=['Media Type', 'Year', 'Month'],
            how='left'
        )

        # Monthly groups → Attributed LT
        monthly_groups = {}
        for col in lt_res.columns:
            if col.startswith("Monthly "):
                base_name = col.split("-")[0].replace("Monthly ", "")
                monthly_groups.setdefault(base_name, []).append(col)

        for base_name, cols in monthly_groups.items():
            lt_res[f"Attributed {base_name} - LT"] = lt_res[cols].sum(axis=1)

        expected_groups = {}
        for col in lt_res.columns:
            if col.startswith("Expected "):
                base_name = col.split("-")[0].replace("Expected ", "")
                expected_groups.setdefault(base_name, []).append(col)

        for base_name, cols in expected_groups.items():
            lt_res[f"Total Expected {base_name}"] = lt_res[cols].sum(axis=1)

        logging.info("Merging completed successfully.")


        # if config['ProductLine_Flag']:
        #     print("Executing this")
        #     req_format_lt = lt_res.copy()
        #     req_format_lt = req_format_lt.replace("None", np.nan)
        # else:
        #     print("Executing this --- ")
        #     req_format_lt = transform_dataframe(lt_res, config)

        if config['ProductLine']==True:
            print("Executing this")
            req_format_lt = lt_res.copy()
            req_format_lt = req_format_lt.replace("None", np.nan)
        else:
            print("Executing this --- ")
            req_format_lt = transform_dataframe(lt_res, config)


        # req_format_lt.to_excel(f"../output/Extrapolated Data/Only_LT_lt_rroi_{config['brand']}_Original.xlsx",index=False) 
        # logging.info("Transformation applied successfully.")


        # # For PC Brands Only
        # req_format_lt.rename(columns={"Channel":"Channel/Daypart"},inplace=True)
        # # req_format_lt['Platform'] = np.where((req_format_lt['Media Type'] == 'Paid Media') & (req_format_lt['Channel/Daypart'] != 'Digital Video'),'All', req_format_lt['Platform'])
        
        req_format_lt.to_excel(f"../output/Extrapolated Data/Only_LT_lt_rroi_{config['brand']}.xlsx",index=False) 

        logging.info(f"-"*100)
        return req_format_lt

    except Exception as e:
        logging.exception("Error during process_expected_sales execution.")
        raise

if __name__ == "__main__":
    try:
        with open("../input/config/config.json", "r") as file:
            config = json.load(file)
        logging.info("Config file loaded successfully.")
    except FileNotFoundError:
        logging.exception("Config file not found.")
        raise
    final_result = process_expected_sales(config)
    logging.info("Expected sales processing finished successfully.")
    print(final_result.head(10))
