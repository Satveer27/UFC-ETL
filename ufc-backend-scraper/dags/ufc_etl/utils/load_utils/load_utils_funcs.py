import pandas as pd


def getCorrectAggregatedData(full_fighter_data, aggregated_data):
    list = []
    aggregated_data['average_time_per_fight'] = aggregated_data['average_time_per_fight'].astype(str)
    aggregated_data['average_time_per_fight'] = aggregated_data['average_time_per_fight'].apply(
    lambda x: str(x).split(" days ")[-1] if pd.notnull(x) else None
    )
    for i in full_fighter_data:
        fighter_id = i['fighter_id']
        pd_id = aggregated_data[aggregated_data['fighter_id'] == fighter_id].values.tolist()
        list.append(pd_id[0])   
    list = [(row[1], row[2], row[8], row[3], row[4], row[5], row[6], row[7], row[0]) for row in list]
    return list