import re
import numpy as np
import pandas as pd
import logging
from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
import warnings
import os

warnings.filterwarnings("ignore")

# Setup logging
log_dir = "./output/logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "Expected_Sales.log"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def join_non_null(row):
    non_null_values = [str(val) for val in row if not pd.isna(val)]
    return '|'.join(non_null_values)


def LTROI_RROI(config):
    logging.info("LTROI_RROI execution started.")

    try:
        all_date_weekly = pd.date_range(
            start=config["expected_sales_start"],
            end=config["model_end_date"],
            freq='W'
        )
        model_range_date_weekly = pd.date_range(
            start=config["model_start_date"],
            end=config["model_end_date"],
            freq='W'
        )
        model_start_week_no = next(
            index for index, j in enumerate(all_date_weekly)
            if j == model_range_date_weekly[0]
        )
        logging.info("Date ranges generated successfully.")
    except Exception as e:
        logging.exception(f"Error generating date ranges: {e}")
        raise

    scurve_dict = {metric: {} for metric in config["metrics"]}
    try:
        lag_file_path = f"./input/Data/{config['brand']}_lag_file.xlsx"
        logging.info(f"Loading lag file from: {lag_file_path}")

        data_lag = pd.read_excel(
            lag_file_path,
            sheet_name='Lag File',
            engine='openpyxl'
        ).T

        for row in range(2, data_lag.shape[0]):
            key = '|'.join([i for i in data_lag.iloc[row, 0:5].to_list() if pd.notna(i)])

            for idx, metric in enumerate(config["metrics"]):
                start_col = 7 + idx * 4
                end_col = start_col + 2
                scurve_dict[metric][key] = data_lag.iloc[row, start_col:end_col].to_list()
        logging.info("Lag dictionaries created successfully.")
    except Exception as e:
        logging.exception(f"Error loading or parsing lag file: {e}")
        raise

    skip_metrics = config.get("baseline_key", [])
    metrics_list = [m for m in config.get("metrics", []) if m not in skip_metrics]
    logging.info(f"Metrics to process: {metrics_list}")

    required_cols = ["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]

    for metric in metrics_list:
        try:
            input_path = f"./output/Weekly ROI Format/{config['brand']}_{metric}_Weekly_results.xlsx"
            
            print(f"\nProcessing metric: {metric}")
            print(f"Input file path: {input_path}")
            logging.info(f"Processing metric: {metric} | Input file: {input_path}")

            if not os.path.exists(input_path):
                logging.warning(f"File {input_path} does not exist. Skipping {metric}.")
                print(f"File {input_path} does not exist. Skipping {metric}.")
                continue

            data_rroi = pd.read_excel(input_path)
            data_rroi['Date'] = pd.to_datetime(data_rroi['Date'])
            data_rroi['Year'] = data_rroi['Date'].dt.isocalendar()['year']
            data_rroi['Week'] = data_rroi['Date'].dt.isocalendar()['week']

            if config['ProductLine_Flag'] == 1:
                data_rroi['Feature'] = data_rroi[
                    ["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]
                ].apply(join_non_null, axis=1)
                p_list = ["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]
            elif config['ProductLine_Flag'] == 2:
                data_rroi['Feature'] = data_rroi[
                    ["Media Type", "Product Line", "Master Channel", "Channel"]
                ].apply(join_non_null, axis=1)
                p_list = ["Media Type", "Product Line", "Master Channel", "Channel"]
            else:
                logging.warning(f"Invalid ProductLine_Flag: {config['ProductLine_Flag']}")
                print(f"Invalid ProductLine_Flag: {config['ProductLine_Flag']}")
                continue

            data_rroi1 = data_rroi[
                data_rroi['Media Type'].isin(config["expected_sales_media_type"])
            ].reset_index(drop=True)

            for col in p_list:
                data_rroi1[col].fillna("None", inplace=True)
            data_rroi1["Impressions"].fillna(0, inplace=True)
            data_rroi1["Actual ROI"].fillna(0, inplace=True)

            pivot_final_aroi = data_rroi1.pivot_table(
                index=['Year', 'Week'],
                columns=p_list,
                values=['Actual ROI'],
                aggfunc=np.sum
            ).reset_index()

            p_feats = pivot_final_aroi.columns.to_numpy()
            p_cols = []
            for i in p_feats:
                col = []
                for j in i[1:]:
                    if j == "None":
                        continue
                    col.append(j)
                p_cols.append("|".join(col))
            p_cols = np.array(p_cols)
            logging.info(f"Feature columns extracted for {metric}: {p_cols}")

            no_of_weeks = len(pivot_final_aroi)
            logging.info(f"No. of weeks for {metric}: {no_of_weeks}")
            SROI, WROI = {}, {}

            for i in scurve_dict[metric].keys():
                alpha, beta = scurve_dict[metric][i]
                logging.info(f"Processing feature {i} | alpha={alpha}, beta={beta}")
                print(f"Processing feature {i} | alpha={alpha}, beta={beta}")

                x = np.arange(1, 79)
                w_raw = ((100 * alpha ** (100 * x / 78) * np.log(alpha) * beta ** (alpha ** (100 * x / 78))) *
                         (np.log(beta) - np.log(10 ** 10))) / (((10 ** 10) ** (alpha ** (100 * x / 78))) * 78)
                w = w_raw / sum(w_raw)
                logging.info(f"Weight vector created for {i}, sum={sum(w):.6f}")

                M1 = np.zeros((no_of_weeks, 78 + (no_of_weeks - 1)))
                M2 = np.zeros((no_of_weeks, 78 + (no_of_weeks - 1)))
                for wi in range(no_of_weeks):
                    M1[wi, wi:wi + 78] = w
                    M2[wi, wi:wi + 78] = 1
                logging.info(f"M1 shape={M1.shape}, M2 shape={M2.shape}")

                M3 = np.zeros((78 + no_of_weeks - 1, 1))
                M4 = np.zeros((78 + no_of_weeks - 1, 1))

                assert i in p_cols, f"{i} in lag_dict but not in Weekly RROI features: {p_cols}"
                aroi = pivot_final_aroi[p_feats[p_cols == i][0]]

                M3[:no_of_weeks, 0] = aroi.to_numpy()
                M4[model_start_week_no:no_of_weeks, 0] = 1
                logging.info(f"M3 and M4 initialized for feature {i} | M3 shape={M3.shape}, M4 shape={M4.shape}")

                Sf = (78 + (no_of_weeks - 1)) / np.matmul(M2, M4)
                Wf = 1 / np.matmul(M1, M4)
                logging.info(f"Sf shape={Sf.shape}, Wf shape={Wf.shape}")

                Sroi = np.matmul(M1, M3) * Sf
                Wroi = np.matmul(M1, M3) * Wf
                SROI[i] = Sroi
                WROI[i] = Wroi
                logging.info(f"SROI and WROI computed for feature {i}.")
                

            exp_df = pd.DataFrame(columns=["Year", "Week", "Feature", "Expected Simple ROI", "Expected Weighted ROI"])
            for f_name in SROI.keys():
                exp_df_temp = pd.DataFrame()
                exp_df_temp['Expected Simple ROI'] = SROI[f_name].flatten().tolist()
                exp_df_temp['Expected Weighted ROI'] = WROI[f_name].flatten().tolist()
                exp_df_temp['Feature'] = f_name
                exp_df_temp['Year'] = pivot_final_aroi["Year"]
                exp_df_temp['Week'] = pivot_final_aroi["Week"]
                exp_df_temp = exp_df_temp[exp_df.columns]
                exp_df = pd.concat([exp_df, exp_df_temp], axis=0).reset_index(drop=True)

            data_rroi = pd.merge(
                left=data_rroi,
                right=exp_df,
                on=['Year', 'Week', 'Feature'],
                how='left'
            )
            data_rroi.drop(columns=['Feature'], inplace=True)

            data_rroi['Expected Simple Sales'] = np.where(
                data_rroi['Expected Simple ROI'].isna() | data_rroi['Impressions'].isna(),
                np.nan,
                data_rroi['Expected Simple ROI'] * data_rroi['Impressions']
            )

            data_rroi['Expected Weighted Sales'] = np.where(
                data_rroi['Expected Weighted ROI'].isna() | data_rroi['Impressions'].isna(),
                np.nan,
                data_rroi['Expected Weighted ROI'] * data_rroi['Impressions']
            )

            output_path = f"./output/Extrapolated Data/LTROI_{config['brand']}_rroi_{metric}.xlsx"
            data_rroi.to_excel(output_path, index=False)
            print(f"Saved output for {metric}: {output_path}")
            logging.info(f"LTROI RROI output saved: {output_path}")
            logging.info(f"-"*100)


        except Exception as e:
            logging.exception(f"Failed processing metric {metric}: {e}")
            print(f"Error processing {metric}: {e}")
            continue

    logging.info("LTROI_RROI execution completed.")
    print("LTROI_RROI execution completed.")


# if __name__ == "__main__":
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Config file loaded successfully.")
#     except FileNotFoundError:
#         logging.exception("Config file not found.")
#         raise

#     LTROI_RROI(config)
