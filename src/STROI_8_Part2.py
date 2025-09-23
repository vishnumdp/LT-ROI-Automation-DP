import logging
import pandas as pd
import numpy as np
import json

# Setup logging
try:
    logging.basicConfig(
        filename='../output/logs/STROI_part 2.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging initialized.")
except Exception as e:
    print("Some Issue in creating log file:", e)

from Weekly_ROI_Results_4 import weekly_results
from STROI_8_Part1 import STROI

def transform_dataframe(df, config):
    kpi = config.get('kpi_name', 'Unknown_KPI')
    df['Date'] = pd.to_datetime(df['Date'])
    for col in df.columns:
        col_split = col.split("|")
        if 'effect_essence' in col_split:
            col_split = col_split[:-2]
        if 'Impressions' in col_split:
            col_split.remove("Impressions")
        if 'Cost' in col_split:
            col_split.remove("Cost")
        df.rename(columns={col: "|".join(col_split)}, inplace=True)
    df.set_index('Date', inplace=True)
    transformed_df = pd.DataFrame(df.T.unstack()).reset_index()
    transformed_df.rename({'level_1': "Merged Granularity", 0: f'{kpi}'}, axis=1, inplace=True)
    return transformed_df



def finalize_rroi(config):
    output_path = f"../output/ensemble_results/final_rroi_{config['brand']}_edited.xlsx"
    final_rroi = pd.read_excel(output_path)
    logging.info("Finalizing final_rroi started.")

    try:
        daily_imp = config['media_cost_imp_from_daily_files'].get('daily_imp', False)
        daily_cost = config['media_cost_imp_from_daily_files'].get('daily_cost', False)
        logging.info(f"daily_imp: {daily_imp}, daily_cost: {daily_cost}")

        if not (daily_imp or daily_cost):
            logging.info("Condition evaluated to True — No daily_imp or daily_cost provided.")
            
            expected_columns = [col for col in final_rroi.columns if 'expected' in col.lower()]
            logging.info(f"Identified expected columns: {expected_columns}")

            condition = (final_rroi['Cost'] == 0) & (final_rroi['Impression'] == 0)
            affected_rows = condition.sum()
            logging.info(f"Number of rows where Cost=0 and Impression=0: {affected_rows}")

            final_rroi.loc[condition, expected_columns] = 0

            output_file = f"../output/Extrapolated Data/final_st_lt_rroi_{config['brand']}-{config['curr_date']}.xlsx"
            final_rroi.to_excel(output_file, index=False)
            logging.info(f"Saved final file without daily adjustments at {output_file}")

        else:
            logging.info("Condition evaluated to False — Daily adjustments will be applied.")
            final_rroi_updated = final_rroi.copy()
            temp_lst = ['daily_cost', 'daily_imp']
            final_dict = weekly_results(config)
            logging.info("Loaded weekly results for daily_cost and daily_imp.")

            for ci in temp_lst:
                try:
                    new_cost_imp = final_dict[ci].copy()
                    logging.info(f"Processing {ci}.")
                except KeyError:
                    logging.warning(f"{ci} data not found in weekly results.")
                    continue

                new_cost_imp['Year'] = new_cost_imp['Date'].dt.year
                new_cost_imp['Month'] = new_cost_imp['Date'].dt.month
                new_cost_imp.drop(columns=['Date'], inplace=True)
                new_cost_imp = new_cost_imp.groupby(['Merged Granularity','Year', 'Month']).sum().reset_index()
                assert new_cost_imp.isna().sum().sum()==0
                # Split Merged Granularity based on config
                if config["ProductLine_Flag"] == 1:
                    new_cost_imp[["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]] = new_cost_imp["Merged Granularity"].str.split("|", expand=True)
                elif config["ProductLine_Flag"] == 2:
                    new_cost_imp[["Media Type", "Product Line", "Master Channel", "Channel"]] = new_cost_imp["Merged Granularity"].str.split("|", expand=True)
                else:
                    new_cost_imp[["Media Type", "Master Channel", "Channel/Daypart", "Platform"]] = new_cost_imp["Merged Granularity"].str.split("|", expand=True)
                    new_cost_imp["Product Line"] = "ALL"
                new_cost_imp.drop(columns=["Merged Granularity"], inplace=True)

                if config['ProductLine']:
                    new_cost_imp = new_cost_imp.replace("None", np.nan)
                    logging.info("Replaced 'None' with NaN in new_cost_imp.")
                else:
                    new_cost_imp = transform_dataframe(new_cost_imp, config)
                    logging.info("Transformed new_cost_imp dataframe.")

                # For PC brands only
                # if config['brand'] != "Kraken" :
                #     new_cost_imp['Platform'] = np.where(
                #         (new_cost_imp['Media Type'] == 'Paid Media') & (new_cost_imp['Channel/Daypart'] != 'Digital Video'),
                #         'All', new_cost_imp['Platform']
                #     )
                #     logging.info("Adjusted 'Platform' column for PC brands.")
                # else:
                #     logging.info("Continuing without Platform feature")

                final_rroi_updated = pd.merge(
                    left=final_rroi_updated,
                    right=new_cost_imp,
                    on=['Media Type','Product Line', 'Master Channel', 'Channel', 'Platform', 'Year', 'Month'],
                    how='left'
                )
                logging.info(f"Merged {ci} into final_rroi.")

                # final_rroi_updated = pd.merge(
                #     left=final_rroi_updated,
                #     right=new_cost_imp,
                #     on=['Media Type','Product Line', 'Master Channel', 'Channel', 'Year', 'Month'],
                #     how='left'
                # )
                # logging.info(f"Merged {ci} into final_rroi.")

            # Assign values if daily_cost or daily_imp exists
            def assign_values(row):
                if 'daily_cost' in row and pd.notna(row['daily_cost']):
                    row['Cost'] = row['daily_cost']
                if 'daily_imp' in row and pd.notna(row['daily_imp']):
                    row['Impression'] = row['daily_imp']
                return row

            if 'daily_cost' in final_rroi_updated.columns and 'daily_imp' in final_rroi_updated.columns:
                final_rroi_updated = final_rroi_updated.apply(assign_values, axis=1)
                logging.info("Assigned daily_cost and daily_imp to Cost and Impression columns.")

            expected_columns = [col for col in final_rroi_updated.columns if 'expected' in col.lower()]
            logging.info(f"Identified expected columns: {expected_columns}")

            for col in ['daily_imp', 'daily_cost']:
                if col in final_rroi_updated.columns:
                    final_rroi_updated.drop(columns=[col], inplace=True)
                    logging.info(f"Dropped column {col} from final_rroi_updated.")

            condition = (final_rroi_updated['Cost'] == 0) & (final_rroi_updated['Impression'] == 0)
            affected_rows = condition.sum()
            final_rroi_updated.loc[condition, expected_columns] = 0
            logging.info(f"Set expected columns to 0 where Cost=0 and Impression=0. Affected rows: {affected_rows}")

            output_file = f"../output/Extrapolated Data/final_st_lt_rroi_{config['brand']}-{config['curr_date']}.xlsx"
            final_rroi_updated.to_excel(output_file, index=False)
            logging.info(f"Saved final adjusted file at {output_file}")
            logging.info(f"-"*100)

    except Exception as e:
        logging.error(f"An error occurred during finalize_rroi processing: {e}")
        raise

    logging.info("Finalizing final_rroi completed.")

if __name__=="__main__":
    # Load configuration
    try:
        with open("../input/config/config.json", "r") as file:
            config = json.load(file)
        logging.info("Configuration loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load config.json: {e}")
        raise
    finalize_rroi(config)
