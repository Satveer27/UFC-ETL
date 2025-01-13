#use after processing webscraped data to db
import pandas as pd

from ufc_etl.utils.load_utils.load_utils_funcs import getCorrectAggregatedData
from ufc_etl.utils.transform import add_extra_aggregate_data_to_db


def add_data_to_database(conn, fight_details, fights, events, fighter_details_in_db_cleaned):
    #sql querys
    sql_query_events = """
    MERGE INTO ufc_events AS target
    USING (VALUES (?, ?, ?, ?, ?)) AS source (event_id, event_name, event_date, event_location, event_link)
    ON target.event_id = source.event_id
    WHEN MATCHED THEN
        UPDATE SET
            event_name = source.event_name,
            event_date = source.event_date,
            event_location = source.event_location,
            event_link = source.event_link
    WHEN NOT MATCHED THEN
        INSERT (event_id, event_name, event_date, event_location, event_link)
        VALUES (source.event_id, source.event_name, source.event_date, source.event_location, source.event_link);
    """

    sql_query_fights = """
    MERGE INTO fights AS target
    USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (event_id, fight_id, link, fighter1, fighter2, fighter1_kd, fighter2_kd, fighter1_str, fighter2_str, fighter1_td, fighter2_td, 
    fighter1_sub, fighter2_sub, weightclass, methodOfKnockout, round, time, winner_of_fight, fighter1_id, fighter2_id, result)
    ON target.fight_id = source.fight_id
    WHEN MATCHED THEN
        UPDATE SET
            event_id = source.event_id,
            link = source.link,
            fighter1 = source.fighter1,
            fighter2 = source.fighter2,
            fighter1_kd = source.fighter1_kd,
            fighter2_kd = source.fighter2_kd,
            fighter1_str = source.fighter1_str,
            fighter2_str = source.fighter2_str,
            fighter1_td = source.fighter1_td,
            fighter2_td = source.fighter2_td,
            fighter1_sub = source.fighter1_sub,
            fighter2_sub = source.fighter2_sub,
            weightclass = source.weightclass,
            methodOfKnockout = source.methodOfKnockout,
            round = source.round,
            time = source.time,
            winner_of_fight = source.winner_of_fight,
            fighter1_id = source.fighter1_id,
            fighter2_id = source.fighter2_id,
            result = source.result
    WHEN NOT MATCHED THEN
        INSERT (event_id, fight_id, link, fighter1, fighter2, fighter1_kd, fighter2_kd, fighter1_str, fighter2_str, fighter1_td, fighter2_td, 
                fighter1_sub, fighter2_sub, weightclass, methodOfKnockout, round, time, winner_of_fight, fighter1_id, fighter2_id, result)
        VALUES (source.event_id, source.fight_id, source.link, source.fighter1, source.fighter2, source.fighter1_kd, source.fighter2_kd, source.fighter1_str, source.fighter2_str, 
                source.fighter1_td, source.fighter2_td, source.fighter1_sub, source.fighter2_sub, source.weightclass, source.methodOfKnockout, source.round, 
                source.time, source.winner_of_fight, source.fighter1_id, source.fighter2_id, source.result);
    """

    sql_query_fight_details = """
    MERGE INTO fight_details AS target
    USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (fight_id, fight_link, fighter1_name, fighter2_name, fighter1_fight_id, 
    fighter2_fight_id, fighter1_sig_str, fighter2_sig_str, fighter1_total_str, fighter2_total_str, 
    fighter1_total_td, fighter2_total_td, fighter1_sub_att, fighter2_sub_att, fighter1_total_control_time, fighter2_total_control_time, fighter1_str_head, 
    fighter2_str_head, fighter1_str_body, fighter2_str_body, fighter1_str_leg, fighter2_str_leg, fighter1_str_clinch, fighter2_str_clinch, fighter1_str_ground, 
    fighter2_str_ground, [rounds-main], sig_str_rounds_results, fight_result, fight_winner)
    ON target.fight_id = source.fight_id
    WHEN MATCHED THEN
        UPDATE SET
        fight_link = source.fight_link,
        fighter1_name = source.fighter1_name,
        fighter2_name = source.fighter2_name,
        fighter1_fight_id = source.fighter1_fight_id,
        fighter2_fight_id = source.fighter2_fight_id,
        fighter1_sig_str = source.fighter1_sig_str,
        fighter2_sig_str = source.fighter2_sig_str,
        fighter1_total_str = source.fighter1_total_str,
        fighter2_total_str = source.fighter2_total_str,
        fighter1_total_td = source.fighter1_total_td,
        fighter2_total_td = source.fighter2_total_td,
        fighter1_sub_att = source.fighter1_sub_att,
        fighter2_sub_att = source.fighter2_sub_att,
        fighter1_total_control_time = source.fighter1_total_control_time,
        fighter2_total_control_time = source.fighter2_total_control_time,
        fighter1_str_head = source.fighter1_str_head,
        fighter2_str_head = source.fighter2_str_head,
        fighter1_str_body = source.fighter1_str_body,
        fighter2_str_body = source.fighter2_str_body,
        fighter1_str_leg = source.fighter1_str_leg,
        fighter2_str_leg = source.fighter2_str_leg,
        fighter1_str_clinch = source.fighter1_str_clinch,
        fighter2_str_clinch = source.fighter2_str_clinch,
        fighter1_str_ground = source.fighter1_str_ground,
        fighter2_str_ground = source.fighter2_str_ground,
        [rounds-main] = source.[rounds-main],
        sig_str_rounds_results = source.sig_str_rounds_results,
        fight_result = source.fight_result,
        fight_winner = source.fight_winner
    WHEN NOT MATCHED THEN
    INSERT (fight_id, fight_link, fighter1_name, fighter2_name, fighter1_fight_id, fighter2_fight_id, fighter1_sig_str, fighter2_sig_str, fighter1_total_str, fighter2_total_str, 
            fighter1_total_td, fighter2_total_td, fighter1_sub_att, fighter2_sub_att, fighter1_total_control_time, fighter2_total_control_time, fighter1_str_head, 
            fighter2_str_head, fighter1_str_body, fighter2_str_body, fighter1_str_leg, fighter2_str_leg, fighter1_str_clinch, fighter2_str_clinch, fighter1_str_ground, 
            fighter2_str_ground, [rounds-main], sig_str_rounds_results, fight_result, fight_winner)
    VALUES (source.fight_id, source.fight_link, source.fighter1_name, source.fighter2_name, source.fighter1_fight_id, source.fighter2_fight_id, source.fighter1_sig_str, source.fighter2_sig_str, 
            source.fighter1_total_str, source.fighter2_total_str, source.fighter1_total_td, source.fighter2_total_td, source.fighter1_sub_att, source.fighter2_sub_att, 
            source.fighter1_total_control_time, source.fighter2_total_control_time, source.fighter1_str_head, source.fighter2_str_head, source.fighter1_str_body, 
            source.fighter2_str_body, source.fighter1_str_leg, source.fighter2_str_leg, source.fighter1_str_clinch, source.fighter2_str_clinch, source.fighter1_str_ground, 
            source.fighter2_str_ground, source.[rounds-main], source.sig_str_rounds_results, source.fight_result, source.fight_winner);
    """
    
    sql_query_upd_fighters = """
    UPDATE ufc_fighters
    SET height = ?, weight = ?, reach = ?, stance = ?, dob = ?, splm = ?, str_acc = ?, sapm = ?, str_def = ?, td_avg = ?, td_acc = ?, td_def = ?,
    sub_avg = ?, wins = ?, losses = ?, draws = ?, [no-contest] = ?
    WHERE fighter_id = ?
    """
    tester = pd.DataFrame(fighter_details_in_db_cleaned)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql_query_events, events)
        cursor.executemany(sql_query_fights, fights)
        cursor.executemany(sql_query_fight_details, fight_details)
        if(tester.empty != True):
            cursor.executemany(sql_query_upd_fighters, fighter_details_in_db_cleaned)
        conn.commit()
        print(f"Inserted {len(events)} rows successfully.")
        print(f"Inserted {len(fights)} rows successfully.")
        print(f"Inserted {len(fight_details)} rows successfully.")
        print(f"Updated {len(fighter_details_in_db_cleaned)} rows successfully.")
    
    except Exception as e:
        conn.rollback()
        raise e
    
    return True

def add_non_added_fighters_to_database(conn, fighter_details_not_in_db_cleaned):
    tester = pd.DataFrame(fighter_details_not_in_db_cleaned)
    if(tester.empty != True):
        sql_query_fighters = """
        MERGE INTO ufc_fighters AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (fighter_id, fighter_first_name, fighter_last_name, height, weight, 
        reach, stance, dob, splm, str_acc, sapm, str_def, td_avg, td_acc, td_def, sub_avg, wins, losses, draws, [no-contest])
        ON target.fighter_id = source.fighter_id
        WHEN MATCHED THEN
            UPDATE SET
            fighter_first_name = source.fighter_first_name,
            fighter_last_name = source.fighter_last_name,
            height = source.height,
            weight = source.weight,
            reach = source.reach,
            stance = source.stance,
            dob = source.dob,
            splm = source.splm,
            str_acc = source.str_acc,
            sapm = source.sapm,
            str_def = source.str_def,
            td_avg = source.td_avg,
            td_acc = source.td_acc,
            td_def = source.td_def,
            sub_avg = source.sub_avg,
            wins = source.wins,
            losses = source.losses,
            draws = source.draws,
            [no-contest] = source.[no-contest]
        WHEN NOT MATCHED THEN
            INSERT (fighter_id, fighter_first_name, fighter_last_name, height, weight, reach, stance, dob, splm, str_acc, sapm, str_def, td_avg, td_acc, td_def, sub_avg, wins, losses, draws, [no-contest])
            VALUES (source.fighter_id, source.fighter_first_name, source.fighter_last_name, source.height, source.weight, source.reach, source.stance, source.dob, source.splm, source.str_acc, source.sapm, source.str_def, source.td_avg, source.td_acc, source.td_def, source.sub_avg, source.wins, source.losses, source.draws, source.[no-contest]);
        """
        
        cursor = conn.cursor()
        try:
            cursor.executemany(sql_query_fighters, fighter_details_not_in_db_cleaned)
            conn.commit()
            print(f"Inserted {len(fighter_details_not_in_db_cleaned)} rows successfully.")
        except Exception as e:
            conn.rollback()
            raise e
        
        return True
    else:
        print("Nothing to add in db")
        return True
    
def aggregated_data_to_database(conn, fighter_details_in_db, fighter_details_not_in_db):
    full_fighter_data = fighter_details_in_db + fighter_details_not_in_db
    aggregated_data = add_extra_aggregate_data_to_db(conn, full_fighter_data)
    correct_data_list = getCorrectAggregatedData(full_fighter_data, aggregated_data)
    
    #for sql ensure we only update the fighter id from the database, this means first extract out 
    sql_for_aggregation = """
    UPDATE ufc_fighters
    SET total_tko = ?, total_strikes = ?, average_fight_duration = ?, total_sub_wins = ?, finishing_rate = ?, sig_strikes_landed_head = ?,
     sig_strikes_landed_body = ?, sig_strikes_landed_legs = ?
    WHERE fighter_id = ?
    """
    cursor = conn.cursor()
    try:
        cursor.executemany(sql_for_aggregation, correct_data_list)
        conn.commit()
        print(f"Updated {len(correct_data_list)} rows successfully.")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print("Error aggregating data")
        raise(e)
