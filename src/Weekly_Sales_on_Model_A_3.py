import re
import numpy as np
import pandas as pd
import boto3
import os
import logging
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

logging.basicConfig(
    filename='./output/logs/weekly_sales.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def weekly_sales(config):
    try:
        all_date_weekly = pd.date_range(start=config["model_start_date"], end=config["model_end_date"], freq='W')
        all_date_weekly_df = pd.DataFrame({"Date": all_date_weekly})
        mds_kpi = {}

        for l_temp in config["kpi"].keys():
            try:
                mds_kpi[l_temp] = pd.read_excel(f"./input/Data/mds_{l_temp}.xlsx")
                logging.info(f"KPI file loaded for {l_temp}")
            except Exception as e:
                logging.error(f"Error loading KPI file for {l_temp}", exc_info=True)
                raise

        for modelA in config['metrics']: ### From Here 
            if not config[modelA]:
                raise ValueError(f"Config error: '{modelA}' is empty. Please add models to config.")
            try:
                base_model = config[modelA][0]
                df = pd.read_csv(f"{config['modelA_s3_folder_path']}/raw_abs_{config['brand']}_{base_model}.csv")  ### Till Here it is edited accordinf to requirement
                df["Date"] = pd.to_datetime(df["Date"], format=config["date_format"])
                logging.info(f"Loaded: raw_abs_{base_model}")
                df.drop('Date', axis=1, inplace=True)
            except Exception as e:
                logging.error(f"Error loading or parsing base model file for {modelA}", exc_info=True)
                raise

            if len(config[modelA]) > 1:
                for model in range(1, len(config[modelA])):
                    try:
                        model_name = config[modelA][model]
                        df1 = pd.read_csv(f"{config['modelA_s3_folder_path']}/raw_abs_{config['brand']}_{model_name}.csv")
                        df1["Date"] = pd.to_datetime(df1["Date"], format=config["date_format"])
                        df1 = df1[["Date"] + list(df.columns)]  # Ensure consistent column order
                        df = df.add(df1.drop('Date', axis=1))
                        print(f'raw_abs_{config[modelA][model]}')
                        logging.info(f"Loaded and added: raw_abs_{model_name}")
                    except KeyError:
                        logging.error(f"Column mismatch in file: raw_abs_{model_name}", exc_info=True)
                        raise
                    except Exception as e:
                        logging.error(f"Error processing model file: raw_abs_{model_name}", exc_info=True)
                        raise

            try:
                df = df / len(config[modelA])
                df = df.div(df.sum(axis=1), axis=0)
                df.insert(0, 'Date', df1['Date'])
                df = pd.merge(all_date_weekly_df, df, on="Date", how="left")
                assert df.isna().sum().sum() == 0, f"{modelA} has missing values"
                assert len(df) == len(all_date_weekly), f"{modelA} has repeated/missing dates"
                logging.info(f"{modelA} ensemble data processed and validated.")
            except AssertionError as ae:
                logging.error(f"Validation failed for {modelA}: {str(ae)}")
                raise
            except Exception as e:
                logging.error(f"Error during ensemble processing for {modelA}", exc_info=True)
                raise

            try:
                # ensemble_file_name = f'./output/ensemble_results/raw_abs_{config["brand"]}_{modelA[:3]}_Ensemble.csv'
                ensemble_file_name = f'./output/ensemble_results/raw_abs_{config["brand"]}_{modelA}_Ensemble.csv'
                df.to_csv(ensemble_file_name, index=False)
                print("Saved to :", ensemble_file_name)
                logging.info(f"Saved ensemble file: {ensemble_file_name}")
            except Exception as e:
                logging.error(f"Failed to save ensemble file for {modelA}", exc_info=True)
                raise

            ct = 0
            for l_temp in mds_kpi.keys():
                try:
                    # df_bu = df.drop(columns=['Date']).multiply(mds_kpi[l_temp][modelA[:3]], axis=0)
                    df_bu = df.drop(columns=['Date']).multiply(mds_kpi[l_temp][modelA], axis=0)
                    df_bu.insert(0, 'Date', all_date_weekly)

                    # file_path = f'./input/Data/LTROI {config["brand"]} Weekly {modelA[:3]}.xlsx'
                    file_path = f'./input/Data/LTROI {config["brand"]} Weekly {modelA}.xlsx'
                    if ct == 0:
                        df_bu.to_excel(file_path, sheet_name=f"Weekly {l_temp}", index=False)
                    else:
                        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                            df_bu.to_excel(writer, sheet_name=f"Weekly {l_temp}", index=False)
                    logging.info(f"Saved LTROI weekly sheet for {modelA} - {l_temp}")
                    ct += 1
                except Exception as e:
                    logging.error(f"Failed to save LTROI sheet for {modelA} - {l_temp}", exc_info=True)
                    raise
                
        logging.info(f"-"*100)
        return df_bu

    except Exception as e:
        logging.critical("weekly_sales() encountered a error.", exc_info=True)
        raise


# if __name__=="__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Config file loaded successfully.")
#     except Exception as e:
#         logging.error("Failed to load config file.", exc_info=True)
#         raise

#     weekly_sales(config)