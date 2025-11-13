import pandas as pd


def process_sensor_data(
    sensor_id: int, resample: bool = False, merge: bool = False
) -> pd.DataFrame:
    """Process a single sensor's data and return the merged DataFrame."""
    pwr_file_path = f"Powers/{sensor_id}.csv"
    vlt_file_path = f"PhaseVoltages/{sensor_id}.csv"

    # Read and process power data
    data_pwr = pd.read_csv(
        pwr_file_path, parse_dates=["Time"], date_format="%d-%b-%Y %H:%M:%S.%f"
    )
    data_pwr.rename(
        columns={
            "Time": "timestamp",
            "Pplus_kW_": "P_plus",
            "Pminus_kW_": "P_minus",
            "Qplus_kvar_": "Q_plus",
            "Qminus_kvar_": "Q_minus",
        },
        inplace=True,
    )
    data_pwr.set_index("timestamp", inplace=True)
    full_range_pwr = pd.date_range(
        start=data_pwr.index.min(), end=data_pwr.index.max(), freq="15min"
    )
    data_pwr = data_pwr.reindex(full_range_pwr)

    # Read and process voltage data
    data_vlt = pd.read_csv(
        vlt_file_path, parse_dates=["time"], date_format="%d-%b-%Y %H:%M:%S.%f"
    )
    data_vlt.rename(columns={"time": "timestamp"}, inplace=True)
    data_vlt.drop(columns=["serialno"], inplace=True)

    data_vlt = data_vlt.pivot_table(
        index="timestamp", columns="variable", values="value", dropna=False
    )
    data_vlt.columns.name = None
    data_vlt.rename(columns={"V_L1": "V_1", "V_L2": "V_2", "V_L3": "V_3"}, inplace=True)
    full_range_vlt = pd.date_range(
        start=data_vlt.index.min(), end=data_vlt.index.max(), freq="10min"
    )
    data_vlt = data_vlt.reindex(full_range_vlt)

    if resample:
        # Resample to 30-minute intervals
        data_pwr = data_pwr.resample("30Min").mean()
        data_vlt = data_vlt.resample("30Min").mean()

    if not merge:
        data_pwr["sensor_id"] = sensor_id
        data_vlt["sensor_id"] = sensor_id
        return [data_pwr, data_vlt]

    # MERGE power and voltage data if merge is True
    data_merged = pd.merge(
        data_pwr, data_vlt, left_index=True, right_index=True, how="outer"
    )
    data_merged["sensor_id"] = sensor_id

    return data_merged


def get_data_info(df: pd.DataFrame, sensor_id: int) -> pd.DataFrame:
    total_rows = len(df)

    columns = ["P_plus", "Q_plus", "P_minus", "Q_minus", "V_1", "V_2", "V_3"]
    info = {}

    for col in columns:
        if col in df.columns:
            missing = df[col].isna().sum()
        else:
            missing = total_rows

        info[col] = {
            "missing_count": int(missing),
            "missing_percentage": round(missing / total_rows * 100, 2),
        }

    # Create DataFrame with MultiIndex
    result = pd.DataFrame(info, index=["missing_count", "missing_percentage"])
    result.index = pd.MultiIndex.from_product(
        [[sensor_id], result.index], names=["sensor_id", "metric"]
    )

    return result


def calculate_power_complex(df):
    P = df["P_plus"] - df["P_minus"]
    Q = df["Q_plus"] - df["Q_minus"]
    return pd.DataFrame({"P": P, "Q": Q})


# main function
if __name__ == "__main__":
    # Main processing
    sensor_ids = [i for i in range(1, 162) if i != 157]
    all_data_pwr = []
    all_data_vlt = []

    for i in sensor_ids:
        print(f"Processing sensor {i}...")
        [data_merged_pwr, data_merged_vlt] = process_sensor_data(i)
        all_data_pwr.append(data_merged_pwr)
        all_data_vlt.append(data_merged_vlt)

    data_merged_pwr_all = pd.concat(all_data_pwr)
    data_merged_vlt_all = pd.concat(all_data_vlt)
    data_merged_pwr_all.to_csv("processed_data/all_sensors_power_data.csv")
    data_merged_vlt_all.to_csv("processed_data/all_sensors_voltage_data.csv")

    # # Concatenate all sensor df
    # data_merged_all = pd.DataFrame(
    #     columns=[
    #         "sensor_id",
    #         "P_plus",
    #         "P_minus",
    #         "Q_plus",
    #         "Q_minus",
    #         "V_1",
    #         "V_2",
    #         "V_3",
    #     ],
    #     index=pd.DatetimeIndex([], name="timestamp"),
    # )
    # data_merged_all = pd.concat(all_data, axis=0)
    # data_merged_all.to_csv("processed_data/all_sensors_data.csv", index=True)

    # for i in sensor_ids:
    #     data_merged = process_sensor_data(i, resample=True)
    #     data_info = get_data_info(df=data_merged, sensor_id=i)
    #     all_data.append(data_info)

    # # Concatenate all information df
    # data_info_all = pd.concat(all_data, axis=0)
    # data_info_all.to_csv("processed_data/all_sensors_info_resampled.csv", index=True)

    # # Resample data to 30-minute intervals
    # data_pwr_30m = data_pwr.resample("30Min").mean()
    # data_vlt_30m = data_vlt.resample("30Min").mean()

    # # Merge the resampled data on the timestamp index
    # data_merged = pd.merge(data_pwr_30m, data_vlt_30m, on="timestamp", how="outer")
    # data_merged.to_csv(f"processed_data/sensor_{i}_30min_merged.csv", index=True)
