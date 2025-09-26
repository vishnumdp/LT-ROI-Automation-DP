import logging
import pandas as pd
import numpy as np
import json

# Setup logging
try:
    logging.basicConfig(
        filename='./output/logs/STROI_part2.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging initialized for STROI Part 2.")
except Exception as e:
    print("Some Issue in creating log file:", e)

from Weekly_ROI_Results_4 import weekly_results
from STROI_8_Part1 import STROI


def transform_dataframe(df, config):
    """Transform dataframe into required format based on KPI."""
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

    logging.info("Dataframe transformed successfully in transform_dataframe().")
    return transformed_df


def finalize_rroi(config):
    output_path = f"./output/ensemble_results/final_rroi_{config['brand']}_edited.xlsx"
    logging.info(f"Starting finalize_rroi for brand: {config['brand']}")
    logging.info(f"Reading final_rroi from: {output_path}")

    try:
        final_rroi = pd.read_excel(output_path)
        logging.info(f"final_rroi loaded with shape {final_rroi.shape}")
        print(f"Loaded final_rroi with {final_rroi.shape[0]} rows and {final_rroi.shape[1]} columns")
    except Exception as e:
        logging.error(f"Error reading final_rroi file: {e}")
        raise

    try:
        daily_imp = config['media_cost_imp_from_daily_files'].get('daily_imp', False)
        daily_cost = config['media_cost_imp_from_daily_files'].get('daily_cost', False)
        logging.info(f"Flags - daily_imp: {daily_imp}, daily_cost: {daily_cost}")

        if not (daily_imp or daily_cost):
            logging.info("No daily_imp or daily_cost adjustments required.")
            
            expected_columns = [col for col in final_rroi.columns if 'expected' in col.lower()]
            logging.info(f"Expected columns identified: {expected_columns}")

            condition = (final_rroi['Cost'] == 0) & (final_rroi['Impression'] == 0)
            affected_rows = condition.sum()
            final_rroi.loc[condition, expected_columns] = 0
            logging.info(f"Set expected columns to 0 for {affected_rows} rows where Cost=0 & Impression=0")
            print(f"Zeroed expected values for {affected_rows} rows (Cost=0 & Impression=0)")

            output_file = f"./output/Extrapolated Data/final_st_lt_rroi_{config['brand']}-{config['curr_date']}.xlsx"
            final_rroi.to_excel(output_file, index=False)
            logging.info(f"Saved final file (no daily adjustments) at {output_file}")
            print(f"Final file saved at {output_file}")

        else:
            logging.info("Daily adjustments enabled. Starting merge with weekly results.")
            final_rroi_updated = final_rroi.copy()
            temp_lst = ['daily_cost', 'daily_imp']

            final_dict = weekly_results(config)
            logging.info("Weekly results successfully retrieved from weekly_results().")

            for ci in temp_lst:
                try:
                    new_cost_imp = final_dict[ci].copy()
                    logging.info(f"Processing {ci} data with shape {new_cost_imp.shape}")
                    print(f"Processing {ci} data → {new_cost_imp.shape}")
                except KeyError:
                    logging.warning(f"{ci} not found in weekly_results. Skipping.")
                    continue

                # Preprocessing
                new_cost_imp['Year'] = new_cost_imp['Date'].dt.year
                new_cost_imp['Month'] = new_cost_imp['Date'].dt.month
                new_cost_imp.drop(columns=['Date'], inplace=True)
                new_cost_imp = new_cost_imp.groupby(['Merged Granularity','Year','Month']).sum().reset_index()
                assert new_cost_imp.isna().sum().sum() == 0, f"NaNs found in {ci} data after grouping"

                # Split Merged Granularity based on ProductLine_Flag
                if config["ProductLine_Flag"] == 1:
                    new_cost_imp[["Media Type", "Product Line", "Master Channel", "Channel", "Platform"]] = new_cost_imp["Merged Granularity"].str.split("|", expand=True)
                elif config["ProductLine_Flag"] == 2:
                    new_cost_imp[["Media Type", "Product Line", "Master Channel", "Channel"]] = new_cost_imp["Merged Granularity"].str.split("|", expand=True)
                else:
                    new_cost_imp[["Media Type", "Master Channel", "Channel/Daypart", "Platform"]] = new_cost_imp["Merged Granularity"].str.split("|", expand=True)
                    new_cost_imp["Product Line"] = "ALL"
                new_cost_imp.drop(columns=["Merged Granularity"], inplace=True)
                logging.info(f"{ci} granularity columns split successfully.")

                if config['ProductLine']:
                    new_cost_imp = new_cost_imp.replace("None", np.nan)
                    logging.info(f"Replaced 'None' with NaN for {ci}.")
                else:
                    new_cost_imp = transform_dataframe(new_cost_imp, config)
                    logging.info(f"Transformed dataframe for {ci} using transform_dataframe().")

                # Brand-specific handling
                PC = ['Bar','BW','Deo_F','PW DMC','Deo DMC','Degree_M','Degree_F','Axe']
                BnW = ['Nexxus','Dove','Shea_M','Tresseme','Vaseline']
                NIC = ['Klondike','Talenti','Yasso','Breyers']

                if config["brand"] in PC:
                    print("PC brands logic executing")
                    new_cost_imp['Platform'] = np.where(
                        (new_cost_imp['Media Type'] == 'Paid Media') & 
                        (new_cost_imp['Channel/Daypart'] != 'Digital Video'),
                        'All', new_cost_imp['Platform']
                    )
                elif config["brand"] in BnW:
                    print("BnW brands logic executing")
                elif config["brand"] in NIC:
                    print("NIC brands logic executing")
                elif config["brand"] == "Kraken":
                    print("Kraken brand logic executing")

                # Merge into final_rroi
                final_rroi_updated = pd.merge(
                    left=final_rroi_updated,
                    right=new_cost_imp,
                    on=['Media Type','Product Line','Master Channel','Channel','Platform','Year','Month'],
                    how='left'
                )
                logging.info(f"Merged {ci} data into final_rroi. Shape now {final_rroi_updated.shape}")
                print(f"Merged {ci} → final_rroi now {final_rroi_updated.shape}")

            # Assign values if available
            def assign_values(row):
                if 'daily_cost' in row and pd.notna(row['daily_cost']):
                    row['Cost'] = row['daily_cost']
                if 'daily_imp' in row and pd.notna(row['daily_imp']):
                    row['Impression'] = row['daily_imp']
                return row

            if 'daily_cost' in final_rroi_updated.columns and 'daily_imp' in final_rroi_updated.columns:
                final_rroi_updated = final_rroi_updated.apply(assign_values, axis=1)
                logging.info("Reassigned Cost/Impression from daily_cost/daily_imp.")
                print("Applied daily_cost & daily_imp overrides.")

            expected_columns = [col for col in final_rroi_updated.columns if 'expected' in col.lower()]
            logging.info(f"Expected columns identified: {expected_columns}")

            # Drop temp cols
            for col in ['daily_imp','daily_cost']:
                if col in final_rroi_updated.columns:
                    final_rroi_updated.drop(columns=[col], inplace=True)
                    logging.info(f"Dropped {col} column after assignment.")

            condition = (final_rroi_updated['Cost'] == 0) & (final_rroi_updated['Impression'] == 0)
            affected_rows = condition.sum()
            final_rroi_updated.loc[condition, expected_columns] = 0
            logging.info(f"Zeroed expected values for {affected_rows} rows where Cost=0 & Impression=0")
            print(f"Zeroed expected for {affected_rows} rows after daily merge.")

            # Save file
            output_file = f"./output/Extrapolated Data/final_st_lt_rroi_{config['brand']}-{config['curr_date']}.xlsx"
            final_rroi_updated.to_excel(output_file, index=False)
            logging.info(f"Saved adjusted final_rroi at {output_file}")
            print(f"Final adjusted file saved at {output_file}")

    except Exception as e:
        logging.error(f"Error in finalize_rroi: {e}")
        raise

    logging.info("Finalizing final_rroi completed successfully.")
    logging.info("-" * 100)


# if __name__=="__main__":
#     # Load configuration
#     try:
#         with open("./input/config/config.json", "r") as file:
#             config = json.load(file)
#         logging.info("Configuration loaded successfully.")
#     except Exception as e:
#         logging.error(f"Failed to load config.json: {e}")
#         raise
#     finalize_rroi(config)



# In this code add logging for remaining part and give me fully updated code and also print important thing