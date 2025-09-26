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



    


    
