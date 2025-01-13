from datetime import timedelta
import json

#utility function for cleaning data
def splitter(record):
    try:
        splitter = record.split("%")[0]
        if(splitter.isnumeric() and isinstance(splitter, str)):
            return splitter
        elif(isinstance(splitter, (int, float))):
            return splitter 
        else:
            return None
    except Exception as e: 
         print(e)
         return None

def checkIfNumeric(record):
    try:
        if(isinstance(record, str) and record.isnumeric()):
            return record
        elif(isinstance(record, (int, float))):
            return record
        elif(isinstance(float(record), float)):
            return float(record)
        else: 
            return None
    except Exception as e:
        print(e)
        return None

def checkIfTime(value):
    try:
        minutes, seconds = map(int, value.split(":"))
        timedelta_time = timedelta(minutes=minutes, seconds=seconds)
        time_timedelta_str = str(timedelta_time)
        return time_timedelta_str
    
    except Exception:
        return None
        
def checkFormat(record):
    try:
        splitter = record.split(" ")
        if splitter[0].isnumeric() and splitter[2].isnumeric() and splitter[1] == "of":
            return record
        else:
            return None
    except:
        return None

def extract_landed_strikes(value):
     try:
          if isinstance(value, str) and 'of' in value:
               return int(value.split()[0])
          else:
               return 0
     except:
          return 0
     
def time_to_timedelta(t):
    try:
        return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    except: 
        return timedelta(0)

#convert field row to json
def convert_to_json(array):
    fight_details_list_reordered = [
        list(row) for row in array  
    ]

    for row in fight_details_list_reordered:
        if(row[26] != None):
            row[26] = json.dumps(row[26])  
        if (row[27] != None):
            row[27] = json.dumps(row[27])  

    fight_details_tuple_reordered = [tuple(row) for row in fight_details_list_reordered]

    return fight_details_tuple_reordered

#transform array 
def transform_array_type(fighter_details_in_db_cleaned, fighter_details_not_in_db_cleaned, fights_cleaned, fight_details_cleaned, events):
    fights_data_to_tuple = fights_cleaned.to_records(index=False).tolist()
    fight_details_data_to_tuple = fight_details_cleaned.to_records(index=False).tolist()
    events_data_to_tuple = events.to_records(index=False).tolist()

    if(fighter_details_in_db_cleaned.empty != True):
        fighter_inside_database_to_tuple = fighter_details_in_db_cleaned.to_records(index=False).tolist()
        fighter_in_db_tuple_reordered = [(row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[1], row[2], row[3], row[4], row[0]) for row in fighter_inside_database_to_tuple]
    
    if(fighter_details_not_in_db_cleaned.empty != True):
        fighter_not_inside_database_to_tuple = fighter_details_not_in_db_cleaned.to_records(index=False).tolist()
        fighter_tuple_reordered = [(row[0], row[1], row[2], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[3], row[4], row[5], row[6]) for row in fighter_not_inside_database_to_tuple]
    
    fight_tuple_reordered = [(row[0], row[1], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[4], row[2], row[3], row[5]) for row in fights_data_to_tuple]
    fight_details_tuple_reordered = [(row[0], row[1], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[3], row[2]) for row in fight_details_data_to_tuple]
    fight_details_tuple_reordered_json = convert_to_json(fight_details_tuple_reordered)

    if(fighter_details_in_db_cleaned.empty):
        return fighter_tuple_reordered, fighter_details_in_db_cleaned, fight_tuple_reordered, fight_details_tuple_reordered_json, events_data_to_tuple
    elif(fighter_details_not_in_db_cleaned.empty):
        return fighter_details_not_in_db_cleaned, fighter_in_db_tuple_reordered, fight_tuple_reordered, fight_details_tuple_reordered_json, events_data_to_tuple
    else:
        return fighter_tuple_reordered, fighter_in_db_tuple_reordered, fight_tuple_reordered, fight_details_tuple_reordered_json, events_data_to_tuple