import os
import sys
import json
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

def Execute_LTROI(config: dict):
    Weekly_Imp = config["input_files"]["Weekly_Imp"]
    Daily_cost = config["input_files"]["Daily_cost"]
    Daily_Impression = config["input_files"]["Daily_Impression"]
    Model_A_Raw_Abs = config["input_files"]["Model_A_Raw_Abs"]
    lagged_files_path = config["lagged_files"]


    from src.daily_ratio_weekly_sales_0 import process_sales_data
    process_sales_data(config)

    from src.data_ingestion_1 import data_ingestion
    data_ingestion(Weekly_Imp, Daily_cost, lagged_files_path, Daily_Impression, Model_A_Raw_Abs, config)

    from src.MDS_Sales_Generation_2 import mds_sales_and_units_generation
    mds_sales_and_units_generation(config)

    from src.Weekly_Sales_on_Model_A_3 import weekly_sales
    weekly_sales(config)

    from src.Weekly_ROI_Results_4 import weekly_results
    weekly_results(config)

    from src.Extrapolated_weighted_ROI_5 import LTROI_RROI
    LTROI_RROI(config)

    from src.Monthly_Expected_Sales_6 import generate_expected_sales
    generate_expected_sales(config)

    from src.Monthly_Expected_Sales_Renaming_7 import process_expected_sales
    process_expected_sales(config)

    from src.STROI_8_Part1 import STROI
    STROI(config)

    from src.STROI_8_Part2 import finalize_rroi
    finalize_rroi(config)

    return {"status": "Pipeline executed successfully"}

