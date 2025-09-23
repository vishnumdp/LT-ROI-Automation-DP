import os
import json
import logging
import pandas as pd

try:
    logging.basicConfig(
        filename='./output/logs/daily_ratio_sales.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
        )
except:
    print("Some Issue in creating file")


def process_sales_data_kraken(config):
    try:
        # daily_df = pd.read_csv("../LT/Model B/output/Daily raw abs - 05-06-2025.csv")
        daily_df = pd.read_csv(config['Daily_Units_and_sales']) 
        daily_df['Date'] = pd.to_datetime(daily_df['Date'], format=config["date_format"])
        daily_df.drop(columns=["Others"], inplace=True)
        daily_df = daily_df[(daily_df['Date']>=config["model_start_date"]) & (daily_df['Date']<=config["model_end_date"])].reset_index()


        weekly_data = daily_df.set_index('Date').rolling('7D').sum().reset_index()
        weekly_data = weekly_data[weekly_data['Date'].dt.day_name() == 'Sunday'].reset_index(drop=True)

        weekly_data[['Date','Baseline']].rename(columns={'Baseline':'kpi'}).to_excel(f"./Data/input/Data/{config["brand"]}_weekly NTUs.xlsx",index=False)
        
        weekly_data.set_index('Date', inplace=True)
        ratio_df_lower = weekly_data.resample('D').bfill().reset_index()
        left_date = pd.date_range(config["model_start_date"], config["act_model_start"], freq='D',inclusive='neither')
        ratio_df_upper = pd.DataFrame(columns=ratio_df_lower.columns)
        ratio_df_upper["Date"] = left_date
        ratio_df = pd.concat([ratio_df_upper, ratio_df_lower], axis=0).reset_index(drop=True)
        ratio_df = ratio_df.fillna(method='bfill')

        ratio_df["Base NTUs Ratio"] = daily_df["Baseline"]/ratio_df["Baseline"]
        ratio_df.drop(columns=["Baseline"],inplace=True)
        ratio_df.to_excel("./Data/input/Data/"+config["brand"]+"_base_ntus_daily_ratio_for_lt.xlsx",index=False)


    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    try:
        with open("./input/config/config.json", "r") as file:
            config = json.load(file)
        logging.info("Configuration loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load config.json: {e}")
        raise

    final_ratio_df = process_sales_data_kraken(config)
    print(final_ratio_df.head())
