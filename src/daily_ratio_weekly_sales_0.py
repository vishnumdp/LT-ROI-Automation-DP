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


path_lst = ['ensemble_results', 'Extrapolated Data', 'Weekly ROI Format', 'Weighted Cost', 'logs']
for path in path_lst:
    os.makedirs(f"./output/{path}", exist_ok=True)


def process_sales_data(config):
    if config['brand'] != "Kraken":
        print("Executing this ---- >")
        try:
            monthly_df = pd.read_excel(config['input_files']["STROI"], sheet_name="Monthly Base Sales") ## This is the Standard Format
            # monthly_df = pd.read_excel("./input/Data/Vaseline_monthly_basesales.xlsx") 
            monthly_df.dropna(inplace=True)

            monthly_df["Year-Month"] = monthly_df["Year"].astype(str) + "-" + monthly_df["Month"].astype(str)
            
            logging.info(f"Loaded monthly_df with shape {monthly_df.shape}")

            # daily_df = pd.read_csv(config["Daily_Units_and_sales"])
            daily_df = pd.read_csv(config['input_files']["Daily_Units_and_sales"])
            # daily_df = pd.read_excel(config['input_files']["Daily_Units_and_sales"])
            logging.info(f"Loaded daily_df with shape {daily_df.shape}")

            all_date_daily = pd.date_range(start=config["model_start_date"], end=config["model_end_date"], freq="D")
            all_date_daily_df = pd.DataFrame(all_date_daily, columns=["Date"])
            daily_df["Date"] = pd.to_datetime(daily_df["Date"], format=config["date_format"])

            daily_df = daily_df[["Date", "Year-Month", config["off_units_col"]]]

            daily_df = pd.merge(left=all_date_daily_df, right=daily_df, on="Date", how="left")
            daily_df["ratio"] = daily_df[config["off_units_col"]] / daily_df.groupby("Year-Month")[config["off_units_col"]].transform("sum")
            logging.info("Daily data processed successfully.")

            monthly_df.drop(columns=["Year", "Month"], inplace=True, errors="ignore")
            new_df = daily_df.merge(monthly_df, on="Year-Month", how="left")

            # KPI mapping
            kpi_map = {
                f"Base {col.split('Baseline ')[1]}": col
                for col in new_df.columns
                if col.startswith("Baseline ")
            }
            logging.info(f"KPI map created: {kpi_map}")

            # Computing KPIs
            for kpi_col, baseline_col in kpi_map.items():
                if baseline_col in new_df.columns:
                    new_df[kpi_col] = new_df["ratio"] * new_df[baseline_col]

            available_kpis = [col for col in kpi_map.keys() if col in new_df.columns]
            new_df = new_df[["Date"] + available_kpis].dropna().reset_index(drop=True)

            # Weekly aggregation
            weekly_data = new_df.set_index("Date").rolling("7D").sum().reset_index()
            weekly_data = weekly_data[weekly_data["Date"].dt.day_name() == "Sunday"].reset_index(drop=True)
            logging.info("Weekly data aggregated successfully.")

            # Saving weekly kpis to Excel
            kpi_map_weekly = {
                col: f"weekly {col.split('Base ')[1]}"
                for col in new_df.columns
                if col.startswith("Base ")
            }
            logging.info(f"kpi_map_weekly: {kpi_map_weekly}")

            for kpi_col, suffix in kpi_map_weekly.items():
                if kpi_col in weekly_data.columns:
                    logging.info(f"kpi_col: {kpi_col}")
                    out_path = f"./input/Data/{config['brand']}_{suffix}.xlsx"
                    weekly_data[["Date", kpi_col]].rename(columns={kpi_col: "kpi"}).to_excel(out_path, index=False)
                    logging.info(f"Exported {out_path}")

            
            # Resample for ratio_df
            weekly_data.set_index("Date", inplace=True)
            ratio_df_lower = weekly_data.resample("D").bfill().reset_index()
            left_date = pd.date_range(config["model_start_date"], config["act_model_start"], freq="D", inclusive="left")
            ratio_df_upper = pd.DataFrame(columns=ratio_df_lower.columns)
            ratio_df_upper["Date"] = left_date
            ratio_df = pd.concat([ratio_df_upper, ratio_df_lower], axis=0).reset_index(drop=True).fillna(method="bfill")

            # Computing ratio columns
            for kpi_col in [c for c in new_df.columns if c.startswith("Base ")]:
                print(kpi_col)
                if kpi_col in ratio_df.columns:
                    ratio_df[f"{kpi_col} Ratio"] = new_df[kpi_col] / ratio_df[kpi_col]

            ratio_df.drop(columns=["Base Units", "Base Dollar Sales"], inplace=True, errors="ignore")
            # ratio_df.drop(columns=[kpi_col], inplace=True, errors="ignore")
            logging.info("Final ratio_df created successfully.")
            out_file = f"./input/Data/{config['brand']}_daily_ratio_for_lt.xlsx"
            ratio_df.to_excel(out_file, index=False)
            logging.info(f"Saved final ratio_df to {out_file}")
            print(ratio_df.head())
            logging.info(f"-"*100)
            return ratio_df

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise
    elif config['brand'] == "Kraken":
        print("Executing this")
        try:
            # daily_df = pd.read_csv("./LT/Model B/output/Daily raw abs - 05-06-2025.csv")
            daily_df = pd.read_csv(config['Daily_Units_and_sales']) 
            logging.info("daily data is loaded  successfully.",daily_df.head())
            daily_df['Date'] = pd.to_datetime(daily_df['Date'], format=config["date_format"])
            daily_df.drop(columns=["Others"], inplace=True)
            daily_df = daily_df[(daily_df['Date']>=config["model_start_date"]) & (daily_df['Date']<=config["model_end_date"])].reset_index()

            weekly_data = daily_df.set_index('Date').rolling('7D').sum().reset_index()
            weekly_data = weekly_data[weekly_data['Date'].dt.day_name() == 'Sunday'].reset_index(drop=True)
            logging.info("Converting to Weeekly Format.",weekly_data.head())

            weekly_data[['Date','Baseline']].rename(columns={'Baseline':'kpi'}).to_excel(f"./input/Data/{config['brand']}_weekly NTUs.xlsx", index=False)
            logging.info("Weekly NTUs saved sucessfully.",weekly_data.tail())

            weekly_data.set_index('Date', inplace=True)
            ratio_df_lower = weekly_data.resample('D').bfill().reset_index()
            left_date = pd.date_range(config["model_start_date"], config["act_model_start"], freq='D',inclusive='neither')
            ratio_df_upper = pd.DataFrame(columns=ratio_df_lower.columns)
            ratio_df_upper["Date"] = left_date
            ratio_df = pd.concat([ratio_df_upper, ratio_df_lower], axis=0).reset_index(drop=True)
            ratio_df = ratio_df.fillna(method='bfill')

            logging.info("Creating Daily Ratio")
            ratio_df["Base NTUs Ratio"] = daily_df["Baseline"]/ratio_df["Baseline"]
            ratio_df.drop(columns=["Baseline"],inplace=True)
            ratio_df.to_excel("./input/Data/"+config["brand"]+"_daily_ratio_for_lt.xlsx",index=False)
            logging.info("Daily Ratio for LT is sucessfully created",ratio_df.head())
            logging.info(f"-"*100)
            
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise


# if __name__ == "__main__":
#     try:
#         with open("./input/Config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Configuration loaded successfully.")
#     except Exception as e:
#         logging.error(f"Failed to load config.json: {e}")
#         raise

#     final_ratio_df = process_sales_data(config)