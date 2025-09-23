import re
import numpy as np
import pandas as pd
import boto3
import os
import logging
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(
    filename='../output/logs/mds_generation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def mds_sales_and_units_generation(config):
    try:
        all_date_weekly = pd.date_range(start=config["model_start_date"], end=config["model_end_date"], freq='W')
        all_date_weekly_df = pd.DataFrame({"Date": all_date_weekly})
        logging.info("Generated weekly date range from config.")

        # mds_units = pd.read_csv(config["modelB_raw_abs"]) # Excel also
        mds_units = pd.read_excel(config["modelB_raw_abs"]) # Excel also
        mds_units.rename(columns={'Pure_Baseline': 'Baseline'}, inplace=True)

        mds_units["Date"] = pd.to_datetime(mds_units["Date"], format=config['date_format'])
        logging.info("Loaded and processed Model B raw absolute file.")

        mds_units = pd.merge(left=all_date_weekly_df, right=mds_units, on="Date", how="left")
        print(mds_units)

        if mds_units.isna().sum().sum() > 0:
            raise ValueError("Model B raw abs contains missing week(s). Check input data.")

        mds_per = mds_units.drop(columns=["Date", "Year", "Week"]).div(
            mds_units.drop(columns=["Date", "Year", "Week"]).sum(axis=1), axis=0
        )
        print(mds_per)

        logging.info("Calculated MDS proportions.")

        for kpi in config["kpi"].keys():
            try:
                # out_path = f".../input/Data/{config['brand']}_{suffix}.xlsx"
                # file_path = f".../input/Data/{config['brand']}_weekly_base{kpi}.xlsx"
                print(kpi)

                file_path = f"../input/Data/{config['brand']}_weekly {kpi}.xlsx"
                df_temp = pd.read_excel(file_path)
                df_temp["Date"] = pd.to_datetime(df_temp["Date"])
                print(df_temp)

                logging.info(f"Loaded KPI data for {kpi} from {file_path}")

                req_sales = mds_per.multiply(df_temp["kpi"], axis=0)
                req_sales["Date"] = df_temp["Date"]
                # req_sales = req_sales[["Date", "MFI", "DFI", "SFI", "Baseline"]].reset_index(drop=True)

                metric = ["Date"]  ## ---- 
                for metrics in config['metrics']:
                    metric.append(metrics)
                metric.append("Baseline")
                req_sales = req_sales[metric].reset_index(drop=True)  ## ---- This part is Re-Edited

                output_path = f"../input/Data/mds_{kpi}.xlsx"
                req_sales.to_excel(output_path, index=False)
                logging.info(f"Saved computed MDS sales for {kpi} to {output_path}")
            except Exception as e:
                logging.error(f"Error processing KPI '{kpi}': {e}")
                continue
        return req_sales

    except Exception as e:
        logging.exception("An error occurred during MDS sales and units generation.")
        raise

if __name__ == "__main__":
    try:
        with open("../input/config/config.json", "r") as file:
            config = json.load(file)
        logging.info("Configuration loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load config.json: {e}")
        raise

    try:
        mds_sales_and_units_generation(config)
        logging.info("MDS sales and units generation completed successfully.")
        logging.info(f"-"*100)
    except Exception as e:
        logging.critical("Terminated with an error.")