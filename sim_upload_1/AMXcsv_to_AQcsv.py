import pandas as pd

def AMX_to_AQ(path):
    df_amx = pd.read_csv(path)
    df_amx["HOME-IMSI"] = df_amx["IMSI"]
    df_amx["ICCID_1"] = ""
    df_amx["MSSIDN_1"] = ""
    df_amx["DONOR-IMSI"] = ""
    len_df_amx = len(df_amx)
    for i in range(len_df_amx):
        if df_amx["simType"][i] == 3:
            print("sim")
            df_amx["eid"][i] = ""

    df_aq = df_amx[["ICCID", "HOME-IMSI", "MSSIDN", "ICCID_1", "DONOR-IMSI", "MSSIDN_1", "eid", "simType"]]
    df_csv = df_aq
    df_csv.to_csv("AQ.csv", header=None, index=False)
    return df_amx["country(ISO-3166)"] , df_amx["enterpriseId"]


