# LT-ROI-Automation-DP

A complete automation pipeline for **Long-Term ROI (LT-ROI)** ..

---

## Features

- **Config-driven pipeline** (flexible JSON configuration).
- Handles **granularity**:  
  - `ProductLine_Flag: 1` → Platform level granularity.  
  - `ProductLine_Flag: 2` → Channel level granularity.  
- **Dynamic input sources**:  
  - Daily cost/impression from files.  
  - Or fallback to ST-RROI if disabled.  
- **Lagged file ingestion** for advanced modeling.  
- Automated generation of **expected sales, ROI, and attribution** results.  
- Modular pipeline steps for easier debugging and maintenance.  

---

## Project Structure

    LT-ROI-Automation-DP/
    │── input/
    │ ├── Config/ # Stores uploaded config.json
    │ ├── Data/ # Daily/Weekly/ABS/STROI input files
    │ ├── lagged_files/ # Uploaded lagged files
    │ └── raw attribution/ # Model A attribution
    │
    │── src/ # Source code modules
    │ ├── daily_ratio_weekly_sales_0.py
    │ ├── data_ingestion_1.py
    │ ├── MDS_Sales_Generation_2.py
    │ ├── Weekly_Sales_on_Model_A_3.py
    │ ├── Weekly_ROI_Results_4.py
    │ ├── Extrapolated_weighted_ROI_5.py
    │ ├── Monthly_Expected_Sales_6.py
    │ ├── Monthly_Expected_Sales_Renaming_7.py
    │ ├── STROI_8_Part1.py
    │ └── STROI_8_Part2.py
    │
    │── UI.py # Streamlit UI for config + pipeline execution
    │── README.md # Documentation


---

## Input Files Required

    | File Type                    | Description                                            |
    | ---------------------------- | ------------------------------------------------------ |
    | Daily Cost (Unlagged)        | Daily cost data (before lag).                          |
    | Daily Impression (Unlagged)  | Daily impressions data (before lag).                   |
    | Weekly Impression (Unlagged) | Weekly impressions.                                    |
    | Model A Raw ABS              | Raw attribution data for Model A.                      |
    | Model B Abs Attribution      | Raw attribution for Model B.                           |
    | Brand Lag File               | Lag configuration per brand.                           |
    | Brand STROI                  | Must contain 2 sheets: ROI Format, Monthly_Base_Sales. |
    | Daily Units and Sales        | Daily offline + online sales & units.                  |
    | Lagged Files                 | Example: `Metric_Lagged_Impression.xlsx`.              |


## Running the Pipeline
    Option 1 – Streamlit UI

        streamlit run UI.py


        Upload config.json

        Upload input files & lagged files

        Select brand, auto-fill current date

        Run the LT ROI pipeline

    Option 2 – Direct Python Execution

        from your_pipeline_file import Execute_LTROI
        import json

        with open("./input/Config/config.json", "r") as f:
            config = json.load(f)

        result = Execute_LTROI(config)
        print(result)

## Pipeline Steps

    The pipeline executes the following steps in order:

    process_sales_data → Prepare sales data

    data_ingestion → Ingest weekly & daily inputs + lagged files

    mds_sales_and_units_generation → Generate MDS sales and units

    weekly_sales → Weekly sales on Model A

    weekly_results → Weekly ROI results

    LTROI_RROI → Extrapolated weighted LT ROI & RROI

    generate_expected_sales → Monthly expected sales generation

    process_expected_sales → Rename/format expected sales

    STROI → Short-term ROI calculations

    finalize_rroi → Final ROI & RROI outputs


## Example Output

    ROI results by week, channel, and platform

    Expected sales projections

    Long-term & short-term ROI reports

    Model attribution outputs



---

## Configuration (`config.json`)

The pipeline is **config-driven**. Below is an example template:

```json
{
  "kpi": {
    "Units": {
      "offline": "Offline Units",
      "online": "Online Units"
    },
    "Dollar Sales": {
      "offline": "Offline Revenue",
      "online": "Online Revenue"
    }
  },
  "brand": "Vaseline",
  "curr_date": "25-08-2025",
  "ProductLine_Flag": 1,
  "ProductLine": true,
  "date_format": "%Y-%m-%d",
  "media_cost_imp_from_daily_files": {
    "daily_imp": true,
    "daily_cost": true
  },
  "metrics": ["MFI", "DFI", "SFI"],
  "cost_imp_to_exclude_from_st_rroi": {
    "daily_imp": {
      "cond1": {"Media Type": "Paid Media"},
      "cond2": {"Media Type": "Halo"}
    },
    "daily_cost": {
      "cond1": {"Media Type": "Paid Media"},
      "cond2": {"Media Type": "Halo"}
    }
  },
  "model_start_date": "2021-07-01",
  "act_model_start": "2021-07-04",
  "model_end_date": "2024-06-30",
  "expected_sales_start": "2021-01-03",
  "expected_sales_media_type": [
    "Paid Media",
    "Earned Media",
    "Halo",
    "Masterbrand",
    "Owned Media"
  ],
  "MFI": ["MFI_ensemble", "MFI_ensemble"],
  "DFI": ["DFI_ensemble", "DFI_ensemble"],
  "SFI": ["SFI_ensemble", "SFI_ensemble"],
  "input_files": {
    "Weekly_Imp": "",
    "Daily_cost": "",
    "Daily_Impression": "",
    "STROI": "",
    "Daily_Units_and_sales": "",
    "lag_file_path": "",
    "Model_A_Raw_Abs": "",
    "modelB_raw_abs": ""
  },
  "lagged_files": [],
  "modelA_s3_folder_path": "./input/raw attribution",
  "pure_baseline": {
    "Dollar Sales": "Weekly Dollar Sales",
    "Units": "Weekly Units"
  },
  "baseline_key": "Pure_Baseline",
  "roi_base_metric": "Weekly Dollar Sales",
  "off_units_col": "Axe|Offline|Units"
}