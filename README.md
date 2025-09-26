# LT-ROI-Automation-DP

1.  config["ProductLine_Flag": 1]

        This represent Granularity Level upto Platform Level.

    config["ProductLine_Flag": 2]

        This represent Granularity Level upto Channel Level.


2.  "media_cost_imp_from_daily_files": {
        "daily_imp": true,
        "daily_cost": true
        },

        If FALSE then Cost and Impression  it will be taken from only ST-RROI....
    
3. ### Input Files Required 

    1. Daily Cost Unlagged.
    2. Daily Impression Unlagged.
    3. Weekly Impression Unlagged.
    4. Model A Raw ABS.
    5. Model B Abs Attribution.
    6. Brand_Lag_file.
    7. Brand STROI.
    8. Daily Units and Sales.

        Brand STROI ---- > This should contains two sheets 1. ROI Format.
                                                           2. Monthly_Base_Sales.

    9. Lagged Files 

        Example : Metric_Lagged_Impression





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
                "brand": "",
                "curr_date": "",
                "ProductLine_Flag": 1,
                "ProductLine": true,
                "date_format": "%Y-%m-%d",
                "media_cost_imp_from_daily_files": {
                    "daily_imp": true,
                    "daily_cost": true
                },
                "metrics": [
                    "MFI",
                    "DFI",
                    "SFI"
                ],
                "cost_imp_to_exclude_from_st_rroi": {
                    "daily_imp": {
                    "cond1": {
                        "Media Type": "Paid Media"
                    },
                    "cond2": {
                        "Media Type": "Halo"
                    },
                    "cond3": {
                        "Media Type": "Masterbrand","Product Line":"Influencer"
                    },
                    "cond4": {
                        "Media Type": "Owned Media"
                    },
                    "cond5": {
                        "Media Type": "Masterbrand","Product Line":"PR"
                    }
                    },
                    "daily_cost": {
                    "cond1": {
                        "Media Type": "Paid Media"
                    },
                    "cond2": {
                        "Media Type": "Halo"
                    },
                    "cond3": {
                        "Media Type": "Masterbrand","Product Line":"Influencer"
                    },
                    "cond4": {
                        "Media Type": "Owned Media"
                    },
                    "cond5": {
                        "Media Type": "Masterbrand","Product Line":"PR"
                    }
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
                "Owned Media"],

                "MFI": [
                    "MFI_ensemble",
                    "MFI_ensemble"
                ],
                "DFI": [
                    "DFI_ensemble",
                    "DFI_ensemble"
                ],
                "SFI": [
                    "SFI_ensemble",
                    "SFI_ensemble"
                ],
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
                "lagged_files": [
                
                ],

                "modelA_s3_folder_path": "./input/raw attribution",
                
                "pure_baseline": {
                    "Dollar Sales": "Weekly Dollar Sales",
                    "Units": "Weekly Units"
                },

                "baseline_key": "Pure_Baseline",
                "roi_base_metric": "Weekly Dollar Sales",
                "off_units_col": "Axe|Offline|Units"
                }



                


                
