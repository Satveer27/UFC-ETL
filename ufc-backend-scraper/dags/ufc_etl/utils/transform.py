import pandas as pd

from ufc_etl.utils.transform_utils.aggregate_utility_funcs import add_to_aggregation_array, average_time_taken_per_fight, avg_succesful_clinches_per_fight, calculate_sig_strikes_body_part_percent, finishing_rate, total_sig_strikes_pff
from ufc_etl.utils.transform_utils.clean_utility_funcs import check_if_fights_data_is_valid, clean_fight_details_without_rounds, transform_fighter_data_to_integers
from ufc_etl.utils.transform_utils.transform_utility_funcs import transform_array_type

#Overall clean function
def clean_data(fighter_details_in_db, fighter_details_not_in_db, fight_details, fights, events):
    if(events != False and fighter_details_in_db != False and fighter_details_not_in_db != False and fight_details != False and fights != False):
        events = pd.DataFrame(events)
        fighter_details_in_db_pd = pd.DataFrame(fighter_details_in_db)
        fighter_details_not_in_db_pd = pd.DataFrame(fighter_details_not_in_db)
        fight_details_pd = pd.DataFrame(fight_details)
        fights_pd = pd.DataFrame(fights)

        fighter_details_in_db_cleaned, fighter_details_not_in_db_cleaned = transform_fighter_data_to_integers(fighter_details_in_db_pd, fighter_details_not_in_db_pd)
        fights_cleaned = check_if_fights_data_is_valid(fights_pd)
        fight_details_cleaned = clean_fight_details_without_rounds(fight_details_pd)

        fighter_details_not_in_db_cleaned,fighter_details_in_db_cleaned, fights_cleaned, fight_details_cleaned, events = transform_array_type(fighter_details_in_db_cleaned, fighter_details_not_in_db_cleaned, fights_cleaned, fight_details_cleaned, events)

        return fighter_details_in_db_cleaned,fighter_details_not_in_db_cleaned, fights_cleaned, fight_details_cleaned, events
    else:
        raise ValueError("could not get fighters")


def add_extra_aggregate_data_to_db(conn, all_fights_changed):
    cursor = conn.cursor()
    raw_data_fights = []
    fighter_ids = [row['fighter_id'] for row in all_fights_changed]

    try:
        cursor.execute("""
        CREATE TABLE #TempFighterIDs (
            fighter_id NVARCHAR(50)
        )
        """)
        print("Temporary table created.")
        
        insert_query = "INSERT INTO #TempFighterIDs (fighter_id) VALUES (?)"
        cursor.executemany(insert_query, [(fighter_id,) for fighter_id in fighter_ids])
        print(f"Inserted {len(fighter_ids)} rows into the temporary table.")
        
        query = """
        SELECT fights.fight_id, fights.fighter1_id, fights.fighter2_id, fights.fighter1_str, fights.fighter2_str, fights.methodOfKnockout, fights.time, 
        fights.winner_of_fight, fight_details.fighter1_fight_id, fight_details.fighter2_fight_id, fights.result, fight_details.fighter1_str_head, 
        fight_details.fighter2_str_head, fight_details.fighter1_str_body, fight_details.fighter2_str_body, fight_details.fighter1_str_leg, 
        fight_details.fighter2_str_leg, fight_details.fighter2_str_clinch, fight_details.fighter1_str_clinch
        FROM fights
        INNER JOIN fight_details ON fights.fight_id = fight_details.fight_id
        WHERE fight_details.fighter1_fight_id IN (SELECT fighter_id FROM #TempFighterIDs)
        OR fight_details.fighter2_fight_id IN (SELECT fighter_id FROM #TempFighterIDs)
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        raw_data_fights = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"Fetched {len(raw_data_fights)} rows from the database.")

        pd_fight_details = pd.DataFrame(raw_data_fights)

        #convert any bad values to Nan and then 0 to avoid mathematical errors
        print(pd_fight_details)
        pd_fight_details['fighter1_str'] = pd.to_numeric(pd_fight_details['fighter1_str'], errors='coerce')
        pd_fight_details['fighter1_str'].fillna(0, inplace=True)
        pd_fight_details['fighter2_str'] = pd.to_numeric(pd_fight_details['fighter2_str'], errors='coerce')
        pd_fight_details['fighter2_str'].fillna(0, inplace=True)
        
        #aggregate data for total tkos, if fighter no ko then ensure its 0 when aggregating data ltr on
        tko_fights = pd_fight_details[pd_fight_details["methodOfKnockout"] == "KO/TKO"]
        tko_per_fighter = tko_fights.groupby(['winner_of_fight']).size()

        tko_per_fighter_df = tko_per_fighter.reset_index(name='tko_per_fighter')
        tko_per_fighter_df.rename(columns={'winner_of_fight': 'fighter_id'}, inplace=True)

        #aggregate data for total sig strikes
        total_sig_strikes = total_sig_strikes_pff(pd_fight_details)
        total_sig_strikes_df = total_sig_strikes.reset_index(name='total_sig_strikes')
        aggregated_data = pd.merge(
          tko_per_fighter_df, total_sig_strikes,
          on='fighter_id', 
          how='outer'
        )
        aggregated_data.fillna(0, inplace=True)
     
        #aggregate data for total_sub_wins
        sub_fights = pd_fight_details[pd_fight_details["methodOfKnockout"] == "SUB"]
        sub_per_fighter = sub_fights.groupby(['winner_of_fight']).size()
        aggregated_data = add_to_aggregation_array(aggregated_data, sub_per_fighter, 'winner_of_fight', 'sub_per_fighter', 'num')
        
        #aggregate data for finishing rate sub + tkos, this is for fight they won
        finishing_rpf_win, total_fights = finishing_rate(pd_fight_details, sub_per_fighter, tko_per_fighter)
        aggregated_data = add_to_aggregation_array(aggregated_data, finishing_rpf_win, 'index', 'finishing_per_fighter', 'num')
  
        #significant strikes landed to head percentage -> so get the total head strike, leg strike, body strike add total then get aggregate
        head_strikes_percentage, body_strikes_percentage, leg_strikes_percentage = calculate_sig_strikes_body_part_percent(pd_fight_details)
        aggregated_data = add_to_aggregation_array(aggregated_data, head_strikes_percentage, 'index', 'head_strikes_percentage', 'num')
        aggregated_data = add_to_aggregation_array(aggregated_data, body_strikes_percentage, 'index', 'body_strikes_percentage', 'num')
        aggregated_data = add_to_aggregation_array(aggregated_data, leg_strikes_percentage, 'index', 'leg_strikes_percentage', 'num')

        #Average time taken for fights
        average_time_pft_obj = average_time_taken_per_fight(pd_fight_details, total_fights)
        aggregated_data = add_to_aggregation_array(aggregated_data, average_time_pft_obj, 'index', 'average_time_per_fight', 'time')

        #average succesful clinches per fight
        average_clinches_pf = avg_succesful_clinches_per_fight(pd_fight_details, total_fights)
        aggregated_data = add_to_aggregation_array(aggregated_data, average_clinches_pf, 'index', 'average_clinches_per_fight', 'time')
        cursor.execute("DROP TABLE #TempFighterIDs")
        print("Temporary table dropped.")
        
        # return aggregated_data
        return aggregated_data

    except Exception as e:
            print("Error could not aggregate data:", e)
            cursor.execute("DROP TABLE #TempFighterIDs")
            print("Temporary table dropped.")
            raise(e)
