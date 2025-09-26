import pandas as pd
import logging
import os
import json

path_lst = ['ensemble_results', 'Extrapolated Data', 'Weekly ROI Format', 'Weighted Cost', 'logs']
for path in path_lst:
    os.makedirs(f"./output/{path}", exist_ok=True)

try:
    logging.basicConfig(
        filename='./output/logs/data_ingestion.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
except Exception as e:
    print("Some Issue in creating log file", e)


def data_ingestion(Weekly_Imp: str, Daily_cost: str, lagged_files: list,
                   Daily_Impression: str, Model_A_Raw_Abs: str, config: dict):
    try:
        # ---------------- Unlagged Weekly Impressions ----------------
        logging.info("Reading Weekly Impressions (Unlagged)")
        unlagged = pd.read_excel(Weekly_Imp)
        unlagged.fillna(0, inplace=True)
        unlagged_path = f"./input/Data/{config['brand']}_Impressions_unlagged.xlsx"
        unlagged.to_excel(unlagged_path, index=False)
        logging.info(f"Saved Weekly Impressions to {unlagged_path}")

        # ---------------- Daily Cost ----------------
        logging.info("Reading Daily Cost")
        cost = pd.read_excel(Daily_cost)
        cost.fillna(0, inplace=True)
        cost_path = f"./input/Data/{config['brand']}_Daily_Cost.xlsx"
        cost.to_excel(cost_path, index=False)
        logging.info(f"Saved Daily Cost to {cost_path}")

        # ---------------- Lagged Impressions ----------------
        if len(lagged_files) != len(config["metrics"]):
            logging.warning(
                f"Number of lagged files ({len(lagged_files)}) "
                f"does not match number of metrics ({len(config['metrics'])}). "
                "Mapping may be incorrect."
            )

        for metric, file_path in zip(config["metrics"], lagged_files):
            try:
                logging.info(f"Processing Lagged Impressions for metric: {metric}")
                df_lagged = pd.read_excel(file_path)
                df_lagged.fillna(0, inplace=True)

                out_path = f"./input/Data/{config['brand']}_Impressions_lagged_{metric}.xlsx"
                df_lagged.to_excel(out_path, index=False)

                logging.info(f"Saved Lagged Impressions {metric} to {out_path}")
                print(f"Success for {metric}")

            except Exception as e:
                logging.error(f"Failed to process {metric}: {e}")
                print(f"Failed for {metric}")

        # ---------------- Daily Impressions ----------------
        logging.info("Reading Daily Impressions")
        daily_imp = pd.read_excel(Daily_Impression)
        daily_imp.fillna(0, inplace=True)
        daily_imp_path = f"./input/Data/{config['brand']}_Daily_Impressions.xlsx"
        daily_imp.to_excel(daily_imp_path, index=False)
        logging.info(f"Saved Daily Impressions to {daily_imp_path}")

        # ---------------- Model A Raw Abs ----------------
        logging.info("Processing Model A Raw Abs file")
        metrics = config.get("metrics")
        if not metrics:
            excel_obj = pd.ExcelFile(Model_A_Raw_Abs)
            metrics = excel_obj.sheet_names

        logging.info(f"Processing metrics/sheets: {metrics}")
        model_data = {}

        for metric in metrics:
            excel_obj = pd.ExcelFile(Model_A_Raw_Abs)
            all_sheets = excel_obj.sheet_names

            possible_names = [metric, f"{metric} Final"]
            sheet = next((s for s in all_sheets if s in possible_names), None)

            if sheet is None:
                logging.warning(f"Skipping {metric}, no matching sheet found in Excel")
                continue

            df_metric = pd.read_excel(Model_A_Raw_Abs, sheet_name=sheet)
            if "Date" in df_metric.columns:
                df_metric["Date"] = pd.to_datetime(df_metric["Date"], errors="coerce")

            output_path = f"./input/raw attribution/raw_abs_{config['brand']}_{metric}_ensemble.csv"
            df_metric.to_csv(output_path, index=False)

            model_data[metric] = df_metric
            print(f"Saved {metric} data to {output_path}")
            logging.info(f"Saved {metric} data to {output_path}")
            
        logging.info(f"-" * 100)
        return unlagged, cost, daily_imp, model_data

    except Exception as e:
        logging.error("Error during data ingestion", exc_info=True)
        return None


# if __name__ == "__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Configuration loaded successfully.")
#     except Exception as e:
#         logging.error(f"Failed to load config.json: {e}")
#         raise

#     Weekly_Imp = config["input_files"]["Weekly_Imp"]
#     Daily_cost = config["input_files"]["Daily_cost"]
#     Daily_Impression = config["input_files"]["Daily_Impression"]
#     Model_A_Raw_Abs = config["input_files"]["Model_A_Raw_Abs"]
#     lagged_files = config["lagged_files"]

#     data_ingestion(
#         Weekly_Imp,
#         Daily_cost,
#         lagged_files,
#         Daily_Impression,
#         Model_A_Raw_Abs,
#         config
#     )
