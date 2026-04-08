import pandas as pd
import json
import os
from datetime import datetime, timedelta, timezone
from smb.SMBConnection import SMBConnection


def get_measurements(init_time, iteration):
    """
    Connects to the SMB server, retrieves measurement files, processes them, and saves the aggregated data in JSON format.
    Parameters:
        - init_time: The start time of the experiment in ISO format (e.g., "2024-09-04T12:00:00").
        - iteration: The iteration number for the experiment.
    """

    username = os.environ.get("SMB_USERNAME")
    password = os.environ.get("SMB_PASSWORD")
    my_name = os.environ.get("SMB_MY_NAME")
    server = os.environ.get("SMB_SERVER")
    server_ip = os.environ.get("SMB_IP")

    try:
        conn = SMBConnection(username, password, my_name, server)
        conn.connect(server_ip, 139)
    except Exception as e:
        print(f"Error connecting to SMB server: {e}")
        return
    
    br_name = "BioFlo"

    # convert init_time to GMT-3 and calculate the start time of the experiment
    gmt_minus_3 = timezone(timedelta(hours=-3))    
    start_time = datetime.fromisoformat(init_time).astimezone(gmt_minus_3)

    nodered_file_path = "/danilo/Documents/PublicNodeRed/lab_doc/docs/pruebaFede.csv"
    nodered_variables = ["t", "pH", "temperature"]

    others_file_path = "/ExperimentosBioFlo/ExperimentBioFlo01_09042026/measurements_atline.csv"
    others_variables = ["t", "biomass", "glucose"]

    root_dir = f"data/it_{iteration}"

    os.makedirs(root_dir, exist_ok=True)

    # read nodered measurements
    try:
        with open(f"{root_dir}/nodered_output.csv", "wb") as f:
            conn.retrieveFile("Users", nodered_file_path, f)

        df_node = pd.read_csv(f"{root_dir}/nodered_output.csv", delimiter=';', names=nodered_variables, skip_blank_lines=True, na_values=["", " ", ";;;", "; ;", "nan"])
        
        # clean blank spaces
        df_node = df_node.dropna(axis=1, how="all")
        df_node = df_node.dropna(how="all")
        
    except Exception as e:
        print(f"Error retrieving nodered file")
        df_node = pd.DataFrame(columns=nodered_variables)

    # read manual measurements
    try:
        with open(f"{root_dir}/others_output.csv", "wb") as f:
            conn.retrieveFile("LabCompartido", others_file_path, f)

        df_others = pd.read_csv(f"{root_dir}/others_output.csv", delimiter=';', skip_blank_lines=True, na_values=["", " ", ";;;", "; ;", "nan"])

        # clean blank spaces
        df_others = df_others.dropna(axis=1, how="all")
        df_others = df_others.dropna(how="all")

    except Exception as e:
        print(f"Error retrieving others file")
        df_others = pd.DataFrame(columns=others_variables)


    # initialize JSON structure
    json_data = {br_name: {"measurements_aggregated": {}}}

    for measurement_type in nodered_variables[1:] + others_variables[1:]:
        json_data[br_name]["measurements_aggregated"][measurement_type] = {"measurement_time": [], measurement_type: []}

    # NODERED: iterate through rows and fill JSON file
    df_node_processed = process_times(start_time, df_node, nodered_variables)

    for measurement_type in nodered_variables[1:]:
        json_data[br_name]["measurements_aggregated"][measurement_type]["measurement_time"] = df_node_processed["delta"].tolist()
        json_data[br_name]["measurements_aggregated"][measurement_type][measurement_type] = df_node_processed[measurement_type].tolist()

    # OTHERS: iterate through rows and fill JSON file
    df_others_processed = process_times(start_time, df_others, others_variables)

    for measurement_type in others_variables[1:]:
        json_data[br_name]["measurements_aggregated"][measurement_type]["measurement_time"] = df_others_processed["delta"].tolist()
        json_data[br_name]["measurements_aggregated"][measurement_type][measurement_type] = df_others_processed[measurement_type].tolist()

    with open(f"{root_dir}/db_output.json", "w") as file:
        json.dump(json_data, file)
    


def process_times(start_time, df_measurements, variables):
    """
    Processes the time column in the measurements DataFrame to calculate the delta in hours from the start time of the experiment.
    """

    # if the DataFrame is empty, return it with an empty delta column
    if df_measurements.empty:
        return pd.DataFrame(columns=variables + ["delta"])

    df = df_measurements.copy()

    df["time_obj"] = pd.to_datetime(df["t"], format="%H:%M:%S").dt.time

    # detect rollover and create day offset
    df["rollover"] = (df["time_obj"] < df["time_obj"].shift(1)).astype(int)
    df["day_offset"] = df["rollover"].cumsum()

    # create datetime objects for measurement times
    df["measurement_time"] = df["time_obj"].apply(
        lambda t: datetime.combine(start_time.date(), t).replace(tzinfo=start_time.tzinfo)
    ) + pd.to_timedelta(df["day_offset"], unit="D")

    # calculate delta in hours from the start time
    df["delta"] = round((df["measurement_time"] - start_time).dt.total_seconds() / 3600, 3)

    # filter out measurements that are before the start time
    valid_mask = df["delta"] >= 0

    if valid_mask.any():
        first_valid_pos = valid_mask.values.argmax()
        df = df.iloc[first_valid_pos:]
    else:
        df = df.iloc[0:0] 

    # clean up the DataFrame
    df = df.drop(columns=["time_obj", "rollover", "day_offset"])

    return df
