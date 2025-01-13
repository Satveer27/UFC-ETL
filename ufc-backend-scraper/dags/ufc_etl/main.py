from ufc_etl.utils.db_connec import connect_to_database
from ufc_etl.utils.extract import getEventsWhichIsNotInDatabase, getFightDetails, getFighterDetails
from ufc_etl.utils.load import add_data_to_database, add_non_added_fighters_to_database, aggregated_data_to_database
from ufc_etl.utils.transform import clean_data
import logging

def main_runner():
    logging.info("ETL process started...")
    conn = connect_to_database()
    #Extract
    fights, events = getEventsWhichIsNotInDatabase(conn)
    conn.close()
    fight_details = getFightDetails(fights)

    conn = connect_to_database()
    fighter_details_in_db, fighter_details_not_in_db = getFighterDetails(conn, fight_details)
    conn.close()
    
    if(events != False or fights != False):
        if(len(fighter_details_in_db) != 0 or len(fighter_details_not_in_db) != 0):
            #Clean
            fighter_details_in_db_cleaned,fighter_details_not_in_db_cleaned, fights_cleaned, fight_details_cleaned, events_cleaned = clean_data(fighter_details_in_db, fighter_details_not_in_db, fight_details, fights, events)
        
            #Load
            conn = connect_to_database()
            add_non_added_fighters_to_database(conn, fighter_details_not_in_db_cleaned)
            add_data_to_database(conn, fight_details_cleaned, fights_cleaned, events_cleaned, fighter_details_in_db_cleaned)
            check = aggregated_data_to_database(conn, fighter_details_in_db, fighter_details_not_in_db)
            if check == True:
                logging.info("ETL process completed successfully.")
        else:
            logging.info("No data to be added to the database.")
    else:
        logging.info("No data to be added to the database.")

if __name__ == "__main__":
    main_runner()