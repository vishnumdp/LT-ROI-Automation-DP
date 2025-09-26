import streamlit as st
import json
from pathlib import Path
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),)))
from Main import Execute_LTROI
config_data_path = Path("./input/Config")
config_input_path = Path("./input/Data")
lagged_files_path = Path("./input/lagged_files")

config_data_path.mkdir(parents=True, exist_ok=True)
config_input_path.mkdir(parents=True, exist_ok=True)
lagged_files_path.mkdir(parents=True, exist_ok=True)

st.title("LT RROI")

uploaded_config = st.file_uploader("Upload your config.json file", type=["json"])

if uploaded_config is not None:
    try:
        config_save_path = config_data_path / uploaded_config.name
        with open(config_save_path, "wb") as f:
            f.write(uploaded_config.getbuffer())

        with open(config_save_path, "r") as f:
            config = json.load(f)

        st.success(f"Config file loaded from: {config_save_path}")
        st.json(config)

        st.subheader("Select Brand")
        brand_list = ["Vaseline", "Dove", "Axe", "Pond's", "Lakme"]  # extend as needed
        selected_brand = st.selectbox("Choose a brand", brand_list)

        config["brand"] = selected_brand

        today_date = datetime.today().strftime("%d-%m-%Y")
        config["curr_date"] = today_date
        st.write(f"Current Date: {today_date}")

        st.subheader("Upload Input Data Files")
        for key in config.get("input_files", {}):
            file = st.file_uploader(f"Upload file for {key}", key=key)
            if file is not None:
                save_path = config_input_path / file.name
                with open(save_path, "wb") as f:
                    f.write(file.getbuffer())

                config["input_files"][key] = f"./{save_path.as_posix()}"

        st.subheader("Upload Lagged Files")
        lagged_files = st.file_uploader(
            "Upload multiple lagged files",
            type=["xlsx"],
            accept_multiple_files=True,
            key="lagged_files"
        )

        if lagged_files:
            saved_paths = []
            for file in lagged_files:
                save_path = lagged_files_path / file.name
                with open(save_path, "wb") as f:
                    f.write(file.getbuffer())
                saved_paths.append(f"./{save_path.as_posix()}")

            config["lagged_files"] = saved_paths

        if st.button("Run LT ROI Pipeline"):
            try:
                # Save updated config.json
                with open(config_save_path, "w") as f:
                    json.dump(config, f, indent=4)

                st.info("Running pipeline... please wait ⏳")

                result = Execute_LTROI(config)

                st.success(result["status"])
                st.json(config)

            except Exception as e:
                st.error(f"Pipeline execution failed: {e}")


    except Exception as e:
        
        st.error(f"Error handling config file: {e}")
