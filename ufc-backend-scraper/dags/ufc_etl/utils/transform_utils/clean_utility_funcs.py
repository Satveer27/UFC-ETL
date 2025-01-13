
#Cleaning data for ufc_fighter database
from ufc_etl.utils.transform_utils.transform_utility_funcs import checkFormat, checkIfNumeric, checkIfTime, splitter

def transform_fighter_data_to_integers(fighter_inside_database_updated_data, fighter_not_inside_database_updated_data):
    #this is to split percentages and check if int
    if(fighter_inside_database_updated_data.empty):
        checker =fighter_inside_database_updated_data
        temp_array = [fighter_not_inside_database_updated_data]
    elif(fighter_not_inside_database_updated_data.empty):
        checker = fighter_not_inside_database_updated_data
        temp_array = [fighter_inside_database_updated_data]
    else:
        temp_array = [fighter_inside_database_updated_data, fighter_not_inside_database_updated_data]

    for i in temp_array:
        i["str_acc"] = i["str_acc"].apply(splitter)
        i["str_def"] = i["str_def"].apply(splitter)
        i["td_acc"] = i["td_acc"].apply(splitter)
        i["td_def"] = i["td_def"].apply(splitter)

        i["splm"] = i["splm"].apply(checkIfNumeric)
        i["sapm"] = i["sapm"].apply(checkIfNumeric)
        i["td_avg"] = i["td_avg"].apply(checkIfNumeric)
        i["sub_avg"] = i["sub_avg"].apply(checkIfNumeric)
        
    if(fighter_inside_database_updated_data.empty or fighter_not_inside_database_updated_data.empty):
        return temp_array[0], checker
    else:
        return temp_array[0], temp_array[1]

#clean data for the fights database
def check_if_fights_data_is_valid(fights):
    fights["fighter1_kd"] = fights["fighter1_kd"].apply(checkIfNumeric)
    fights["fighter2_kd"] = fights["fighter2_kd"].apply(checkIfNumeric)
    fights["fighter1_str"] = fights["fighter1_str"].apply(checkIfNumeric)
    fights["fighter2_str"] = fights["fighter2_str"].apply(checkIfNumeric)

    fights["fighter1_td"] = fights["fighter1_td"].apply(checkIfNumeric)
    fights["fighter2_td"] = fights["fighter2_td"].apply(checkIfNumeric)
    fights["fighter1_sub"] = fights["fighter1_sub"].apply(checkIfNumeric)
    fights["fighter2_sub"] = fights["fighter2_sub"].apply(checkIfNumeric)

    fights["round"] = fights["round"].apply(checkIfNumeric)
    fights["time"] = fights["time"].apply(checkIfTime)
    
    return fights

#clean data for fight details
def clean_fight_details_without_rounds(fight_details):
    fight_details['fighter1_sig_str'] = fight_details['fighter1_sig_str'].apply(checkFormat)
    fight_details['fighter2_sig_str'] = fight_details['fighter2_sig_str'].apply(checkFormat)

    fight_details['fighter1_total_str'] = fight_details['fighter1_total_str'].apply(checkFormat)
    fight_details['fighter2_total_str'] = fight_details['fighter2_total_str'].apply(checkFormat)

    fight_details['fighter1_total_td'] = fight_details['fighter1_total_td'].apply(checkFormat)
    fight_details['fighter2_total_td'] = fight_details['fighter2_total_td'].apply(checkFormat)

    fight_details['fighter1_sub_att'] = fight_details['fighter1_sub_att'].apply(checkIfNumeric)
    fight_details['fighter2_sub_att'] = fight_details['fighter2_sub_att'].apply(checkIfNumeric)

    fight_details['fighter1_total_control_time'] = fight_details['fighter1_total_control_time'].apply(checkIfTime)
    fight_details['fighter2_total_control_time'] = fight_details['fighter2_total_control_time'].apply(checkIfTime)

    fight_details['fighter1_str_head'] = fight_details['fighter1_str_head'].apply(checkFormat)
    fight_details['fighter2_str_head'] = fight_details['fighter2_str_head'].apply(checkFormat)

    fight_details['fighter1_str_body'] = fight_details['fighter1_str_body'].apply(checkFormat)
    fight_details['fighter2_str_body'] = fight_details['fighter2_str_body'].apply(checkFormat)

    fight_details['fighter1_str_leg'] = fight_details['fighter1_str_leg'].apply(checkFormat)
    fight_details['fighter2_str_leg'] = fight_details['fighter2_str_leg'].apply(checkFormat)

    fight_details['fighter1_str_clinch'] = fight_details['fighter1_str_clinch'].apply(checkFormat)
    fight_details['fighter2_str_clinch'] = fight_details['fighter2_str_clinch'].apply(checkFormat)

    fight_details['fighter1_str_ground'] = fight_details['fighter1_str_ground'].apply(checkFormat)
    fight_details['fighter2_str_ground'] = fight_details['fighter2_str_ground'].apply(checkFormat)

    return fight_details
