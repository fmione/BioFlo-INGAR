import pandas as pd
import json
import os
from datetime import datetime, timedelta, timezone
from smb.SMBConnection import SMBConnection


def get_measurements(init_time, iteration):

    username = os.environ.get("SMB_USERNAME")
    password = os.environ.get("SMB_PASSWORD")
    server = os.environ.get("SMB_SERVER")
    server_ip = os.environ.get("SMB_IP")

    conn = SMBConnection(username, password, "", server)
    status = conn.connect(server_ip, 139)

    # convert init_time to GMT-3 and calculate the start time of the experiment
    gmt_minus_3 = timezone(timedelta(hours=-3))    
    start_time = datetime.fromisoformat(init_time).astimezone(gmt_minus_3)

    if status:
        br_name = "BioFlo"

        nodered_file_path = "Node_Red/Datos/Prueba3.csv"
        nodered_variables = ["Time", "pH", "Temperature"]

        others_file_path = "Test.csv"
        others_variables = ["Time", "Biomass"]

        root_dir = f"data/it_{iteration}"

        os.makedirs(root_dir, exist_ok=True)

        # read manual measurements
        try:
            with open(f"{root_dir}/others_output.csv", "wb") as f:
                conn.retrieveFile("LabCompartido", others_file_path, f)

            df_others = pd.read_csv(f"{root_dir}/others_output.csv", delimiter=';', names=others_variables)
        except Exception as e:
            print(f"Error retrieving others file")
            df_others = pd.DataFrame(columns=others_variables)
        
        # read nodered measurements
        try:
            with open(f"{root_dir}/nodered_output.csv", "wb") as f:
                conn.retrieveFile("LabCompartido", nodered_file_path, f)

            df_node = pd.read_csv(f"{root_dir}/nodered_output.csv", delimiter=';', names=nodered_variables)
        except Exception as e:
            print(f"Error retrieving nodered file")
            df_node = pd.DataFrame(columns=nodered_variables)


        # initialize JSON structure
        json_data = {br_name: {"measurements_aggregated": {}}}

        for measurement_type in nodered_variables[1:] + others_variables[1:]:
            json_data[br_name]["measurements_aggregated"][measurement_type] = {"measurement_time": [], measurement_type: []}

        # NODERED: iterate through rows and fill JSON file
        for row in df_node.itertuples():
            row_dict = row._asdict()
            measurement_time = round((datetime.strptime(start_time.strftime("%Y-%m-%d") + " " + row_dict["Time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=gmt_minus_3) - start_time).total_seconds() / 3600, 4)

            for measurement_type in nodered_variables[1:]:
                json_data[br_name]["measurements_aggregated"][measurement_type]["measurement_time"].append(measurement_time)
                json_data[br_name]["measurements_aggregated"][measurement_type][measurement_type].append(row_dict[measurement_type])


        # OTHERS: iterate through rows and fill JSON file
        for row in df_others.itertuples():
            row_dict = row._asdict()
            measurement_time = datetime.strptime(start_time.strftime("%Y-%m-%d") + " " + row_dict["Time"], "%Y-%m-%d %H:%M:%S")

            for measurement_type in others_variables[1:]:
                json_data[br_name]["measurements_aggregated"][measurement_type]["measurement_time"].append(measurement_time)
                json_data[br_name]["measurements_aggregated"][measurement_type][measurement_type].append(row_dict[measurement_type])


        with open(f"{root_dir}/db_output.json", "w") as file:
            json.dump(json_data, file)
        
    else:
        print("Connection failed")
