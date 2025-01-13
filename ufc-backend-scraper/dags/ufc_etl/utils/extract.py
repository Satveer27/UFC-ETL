from bs4 import BeautifulSoup
import requests
import time

#Trying to get all ufc events
def getEvents():
    max_attempts = 10
    attempts = 0
    while attempts < max_attempts:
        try:
            url = 'http://ufcstats.com/statistics/events/completed?page=all'
            web_page = requests.get(url)
            soup = BeautifulSoup(web_page.text, features="html.parser")

            table_soup = soup.find('table')
            rows = table_soup.find_all('tr', class_='b-statistics__table-row')
            events = []

            for row in rows:
                link = row.find('a', class_='b-link')
                if link:
                    href = link.get('href')
                    if href:
                        event_id = href.split('/')[-1]
                        event_name = link.get_text(strip=True)
                        date_span = row.find('span', class_='b-statistics__date')
                        event_date = date_span.get_text(strip=True) if date_span else 'N/A'
                        location = row.find('td', class_ ='b-statistics__table-col b-statistics__table-col_style_big-top-padding')
                        event_location = location.get_text(strip=True) if location else 'N/A'

                        if not event_name:
                            event_name = "Unknown Event Name"

                        events.append({
                        'event_id': event_id,
                        'event_name': event_name,
                        'event_date': event_date,
                        'event_location': event_location,
                        'event_link': href
                        })

            return events
        except Exception as e:
            attempts += 1
            print(e)
            if attempts<max_attempts:
                print("Retrying to scrap the events...")
                time.sleep(2)
            else:
                raise(e) 

def getFightsFromEvent(events_not_inside_database):
    fights = [] 
    for events in events_not_inside_database:
            try:
                event_links = events['event_link']
                eventId = events["event_id"]
                attempt = 0
                while attempt < 5: 
                    try: 
                        ufc_event_page = requests.get(event_links)
                        ufc_event_page_soup = BeautifulSoup(ufc_event_page.text, features="html.parser")
                        attempt += 7
                    except Exception as e:
                        if attempt <= 5:
                            attempt += 1
                            print(f"retrying to get event id: {events}")
                        else:
                            raise(e)
                           
                rows = ufc_event_page_soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click')

                for row in rows:
                        href = row['data-link']
                        fight_id = href.split('/')[-1]

                        fighter1 = row.find_all('a', class_='b-link b-link_style_black')[0].get_text(strip=True)
                        fighter2 = row.find_all('a', class_='b-link b-link_style_black')[1].get_text(strip=True)
                        result = row.find_all('i', class_='b-flag__text')[0].get_text(strip=True)

                        fighter1_id = row.find_all('a', class_='b-link b-link_style_black')[0].get('href').split('/')[-1]
                        fighter2_id = row.find_all('a', class_='b-link b-link_style_black')[1].get('href').split('/')[-1]

                        if(result != "win"):
                            winner_id = None
                        else:
                            winner_id = fighter1_id

                        fighter1_kd = row.find_all('p', class_='b-fight-details__table-text')[3].get_text(strip=True)
                        fighter2_kd = row.find_all('p', class_='b-fight-details__table-text')[4].get_text(strip=True)

                        fighter1_str = row.find_all('p', class_='b-fight-details__table-text')[5].get_text(strip=True)
                        fighter2_str = row.find_all('p', class_='b-fight-details__table-text')[6].get_text(strip=True)

                        fighter1_td = row.find_all('p', class_='b-fight-details__table-text')[7].get_text(strip=True)
                        fighter2_td = row.find_all('p', class_='b-fight-details__table-text')[8].get_text(strip=True)

                        fighter1_sub = row.find_all('p', class_='b-fight-details__table-text')[9].get_text(strip=True)
                        fighter2_sub = row.find_all('p', class_='b-fight-details__table-text')[10].get_text(strip=True)

                        weightclass = row.find_all('p', class_='b-fight-details__table-text')[11].get_text(strip=True)
                                
                        methodOfKnockout = row.find_all('p', class_='b-fight-details__table-text')[12].get_text(strip=True)

                        round = row.find_all('p', class_='b-fight-details__table-text')[14].get_text(strip=True)

                        time_rec = row.find_all('p', class_='b-fight-details__table-text')[15].get_text(strip=True)  
                                
                        fights.append({
                                'event_id': eventId,
                                'fight_id': fight_id,
                                'fighter1_id': fighter1_id,
                                'fighter2_id': fighter2_id,
                                'winner': winner_id,
                                'result': result,
                                'link': href,
                                'fighter1': fighter1,
                                'fighter2': fighter2,
                                'fighter1_kd': fighter1_kd,
                                'fighter2_kd': fighter2_kd,
                                'fighter1_str': fighter1_str,
                                'fighter2_str': fighter2_str,
                                'fighter1_td': fighter1_td,
                                'fighter2_td': fighter2_td,
                                'fighter1_sub': fighter1_sub,
                                'fighter2_sub': fighter2_sub,
                                'weightclass': weightclass, 
                                'methodOfKnockout': methodOfKnockout,
                                'round': round,
                                'time': time_rec
                        })

            except Exception as e:
                print(f"Could not get all the fight data in the event id: {events}") 
                raise(e)
            
    return fights

def getEventsWhichIsNotInDatabase(conn):
    web_scraped_events = getEvents()
    
    if conn:   
        try:
            #check for events in database and then once such is done ensure we get store the events that isnt in the database inside a array
            events_not_inside_database = []
            cursor = conn.cursor()
            database_event_ids = []
            cursor.execute("SELECT event_id FROM ufc_events")

            for row in cursor.fetchall():
                for field in row:
                    database_event_ids.append(field)
            
            for web_scraped_event in web_scraped_events:
                check_id_exist_in_db = False
                web_scraped_event_id = web_scraped_event['event_id']

                for db_event_id in database_event_ids:
                    if db_event_id == web_scraped_event_id:
                        check_id_exist_in_db = True
                    

                if check_id_exist_in_db == False:
                    events_not_inside_database.append(web_scraped_event)

            # Loop thru such array, get the fights associated with such array and store them in an array
            event_fights = getFightsFromEvent(events_not_inside_database)

            if(len(event_fights) == 0):
                print("No fights available and No events available")
                return False, False
            if not event_fights:
                raise Exception("Error trying to get fights from event")
            
            return event_fights, events_not_inside_database
    
        except Exception as e:
            print(f"Error: {e}")
            raise(e)
        
    else:
        print("no connection to database")

def getFightDetails(fights):
    if(fights):
        fight_details = []
        session = requests.Session()
        # Extract data row by row
        for row in fights:
            fight_id = row['fight_id']
            fight_link = row['link']
            result = row['result']
            winner = row['winner']
            fighter1_id_check = row['fighter1_id']
            fighter2_id_check = row['fighter2_id']
            fighter1_name_check = row['fighter1']
            fighter2_name_check = row['fighter2']
            rounds = []
            sig_str_rounds_results = []
            round_count = 1
            sig_str_round_count = 1
            max_attempts = 3
            attempts = 0
            
            while attempts < max_attempts:
                try:
                    web_page = session.get(fight_link)
                    web_page.raise_for_status()  
                    soup = BeautifulSoup(web_page.text, features="html.parser")
                
                    fighter1_name = soup.find_all('a', class_='b-link b-link_style_black')[0].get_text(strip=True)
                    fighter2_name = soup.find_all('a', class_='b-link b-link_style_black')[1].get_text(strip=True)
                    
                    fighter1_id = soup.find_all('a', class_='b-link b-link_style_black')[0].get('href').split('/')[-1]
                    fighter2_id = soup.find_all('a', class_='b-link b-link_style_black')[1].get('href').split('/')[-1]
                

                    fighter1_sig_str = soup.find_all('p', class_='b-fight-details__table-text')[4].get_text(strip=True)
                    fighter2_sig_str = soup.find_all('p', class_='b-fight-details__table-text')[5].get_text(strip=True)
                    
                    fighter1_total_str = soup.find_all('p', class_='b-fight-details__table-text')[8].get_text(strip=True)
                    fighter2_total_str = soup.find_all('p', class_='b-fight-details__table-text')[9].get_text(strip=True)

                    fighter1_total_td = soup.find_all('p', class_='b-fight-details__table-text')[10].get_text(strip=True)
                    fighter2_total_td = soup.find_all('p', class_='b-fight-details__table-text')[11].get_text(strip=True)

                    fighter1_sub_att = soup.find_all('p', class_='b-fight-details__table-text')[14].get_text(strip=True)
                    fighter2_sub_att = soup.find_all('p', class_='b-fight-details__table-text')[15].get_text(strip=True)

                    fighter1_total_control_time = soup.find_all('p', class_='b-fight-details__table-text')[18].get_text(strip=True)
                    fighter2_total_control_time = soup.find_all('p', class_='b-fight-details__table-text')[19].get_text(strip=True)


                    #in round depth(hard to get data per round)
                    round_table = soup.find_all('table', class_='b-fight-details__table js-fight-table')[0]
                    round_rows = round_table.find_all('tr')[1:]
                    
                    
                    for row in round_rows:
                        fighter1_rd_kd = row.find_all('p', class_='b-fight-details__table-text')[2].get_text(strip=True)
                        fighter2_rd_kd = row.find_all('p', class_='b-fight-details__table-text')[3].get_text(strip=True)

                        fighter1_rd_sig_str = row.find_all('p', class_='b-fight-details__table-text')[4].get_text(strip=True)
                        fighter2_rd_sig_str = row.find_all('p', class_='b-fight-details__table-text')[5].get_text(strip=True)

                        fighter1_rd_tot_str = row.find_all('p', class_='b-fight-details__table-text')[8].get_text(strip=True)
                        fighter2_rd_tot_str = row.find_all('p', class_='b-fight-details__table-text')[9].get_text(strip=True)

                        fighter1_rd_td = row.find_all('p', class_='b-fight-details__table-text')[10].get_text(strip=True)
                        fighter2_rd_td = row.find_all('p', class_='b-fight-details__table-text')[11].get_text(strip=True)

                        fighter1_rd_sub_att = row.find_all('p', class_='b-fight-details__table-text')[14].get_text(strip=True)
                        fighter2_rd_sub_att = row.find_all('p', class_='b-fight-details__table-text')[15].get_text(strip=True)

                        fighter1_rd_ctrl = row.find_all('p', class_='b-fight-details__table-text')[18].get_text(strip=True)
                        fighter2_rd_ctrl = row.find_all('p', class_='b-fight-details__table-text')[19].get_text(strip=True)

                        rounds.append({
                            "round": round_count,
                            "fighter1_rd_kd": fighter1_rd_kd,
                            "fighter2_rd_kd": fighter2_rd_kd,
                            "fighter1_rd_sig_str": fighter1_rd_sig_str,
                            "fighter2_rd_sig_str": fighter2_rd_sig_str,
                            "fighter1_rd_tot_str": fighter1_rd_tot_str,
                            "fighter2_rd_tot_str": fighter2_rd_tot_str,
                            "fighter1_rd_td": fighter1_rd_td,
                            "fighter2_rd_td": fighter2_rd_td,
                            "fighter1_rd_sub_att": fighter1_rd_sub_att,
                            "fighter2_rd_sub_att": fighter2_rd_sub_att,
                            "fighter1_rd_ctrl": fighter1_rd_ctrl,
                            "fighter2_rd_ctrl": fighter2_rd_ctrl
                        })

                        round_count += 1
                        
                    
                    #sig-str data in depth
                    sig_strike_table = soup.find_all('table')[2]

                    fighter1_str_head = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[6].get_text(strip=True)
                    fighter2_str_head = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[7].get_text(strip=True)

                    fighter1_str_body = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[8].get_text(strip=True)
                    fighter2_str_body = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[9].get_text(strip=True)

                    fighter1_str_leg = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[10].get_text(strip=True)
                    fighter2_str_leg = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[11].get_text(strip=True)

                    fighter1_str_clinch = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[14].get_text(strip=True)
                    fighter2_str_clinch = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[15].get_text(strip=True)

                    fighter1_str_ground = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[16].get_text(strip=True)
                    fighter2_str_ground = sig_strike_table.find_all('p', class_='b-fight-details__table-text')[17].get_text(strip=True)


                    #sig-str data per round
                    sig_str_round_table = soup.find_all('table', class_='b-fight-details__table js-fight-table')[1]
                    sig_str_round_rows = sig_str_round_table.find_all('tr')[1:]

                    for row in sig_str_round_rows:
                        fighter1_head = row.find_all('p', class_='b-fight-details__table-text')[6].get_text(strip=True)
                        fighter2_head = row.find_all('p', class_='b-fight-details__table-text')[7].get_text(strip=True)

                        fighter1_body = row.find_all('p', class_='b-fight-details__table-text')[8].get_text(strip=True)
                        fighter2_body = row.find_all('p', class_='b-fight-details__table-text')[9].get_text(strip=True)

                        fighter1_leg = row.find_all('p', class_='b-fight-details__table-text')[10].get_text(strip=True)
                        fighter2_leg = row.find_all('p', class_='b-fight-details__table-text')[11].get_text(strip=True)

                        fighter1_clinch = row.find_all('p', class_='b-fight-details__table-text')[14].get_text(strip=True)
                        fighter2_clinch = row.find_all('p', class_='b-fight-details__table-text')[15].get_text(strip=True)

                        fighter1_ground = row.find_all('p', class_='b-fight-details__table-text')[16].get_text(strip=True)
                        fighter2_ground = row.find_all('p', class_='b-fight-details__table-text')[17].get_text(strip=True)


                        sig_str_rounds_results.append({
                            "round": sig_str_round_count,
                            "fighter1_head": fighter1_head,
                            "fighter2_head": fighter2_head,
                            "fighter1_body": fighter1_body,
                            "fighter2_body": fighter2_body,
                            "fighter1_leg": fighter1_leg,
                            "fighter2_leg": fighter2_leg,
                            "fighter1_clinch": fighter1_clinch,
                            "fighter2_clinch": fighter2_clinch,
                            "fighter1_ground": fighter1_ground,
                            "fighter2_ground": fighter2_ground,
                        })

                        sig_str_round_count += 1

                    fight_details.append({
                        "fight_id": fight_id,
                        "fight_link": fight_link,
                        'fight_winner': winner,
                        "fight_result": result,
                        "fighter1_name": fighter1_name,
                        "fighter2_name": fighter2_name,
                        "fighter1_id": fighter1_id,
                        "fighter2_id": fighter2_id,
                        "fighter1_sig_str": fighter1_sig_str,
                        "fighter2_sig_str": fighter2_sig_str,
                        "fighter1_total_str": fighter1_total_str,
                        "fighter2_total_str": fighter2_total_str,
                        "fighter1_total_td": fighter1_total_td,
                        "fighter2_total_td": fighter2_total_td,
                        "fighter1_sub_att": fighter1_sub_att,
                        "fighter2_sub_att": fighter2_sub_att,
                        "fighter1_total_control_time": fighter1_total_control_time, #clean this data to ensure its time data for consistency
                        "fighter2_total_control_time": fighter2_total_control_time, #clean this data to ensure its time data for consistency
                        "fighter1_str_head": fighter1_str_head,
                        "fighter2_str_head": fighter2_str_head,
                        "fighter1_str_body": fighter1_str_body,
                        "fighter2_str_body": fighter2_str_body,
                        "fighter1_str_leg": fighter1_str_leg,
                        "fighter2_str_leg": fighter2_str_leg,
                        "fighter1_str_clinch": fighter1_str_clinch,
                        "fighter2_str_clinch": fighter2_str_clinch,
                        "fighter1_str_ground": fighter1_str_ground,
                        "fighter2_str_ground": fighter2_str_ground,
                        "rounds-main": rounds,
                        "sig_str_rounds_results": sig_str_rounds_results
                    })

                    attempts += 5
                    
                except Exception as e:
                    attempts += 1
                    if attempts < max_attempts:
                        print("Retrying...")
                        time.sleep(2)
                    else:
                        print(f"Error retrieving {fight_link}: {e}")
                        fight_details.append({
                        "fight_id": fight_id,
                        "fight_link": fight_link,
                        'fight_winner': winner,
                        "fight_result": result,
                        "fighter1_name": fighter1_name_check,
                        "fighter2_name": fighter2_name_check,
                        "fighter1_id": fighter1_id_check,
                        "fighter2_id": fighter2_id_check,
                        "fighter1_sig_str": None,
                        "fighter2_sig_str": None,
                        "fighter1_total_str": None,
                        "fighter2_total_str": None,
                        "fighter1_total_td": None,
                        "fighter2_total_td": None,
                        "fighter1_sub_att": None,
                        "fighter2_sub_att": None,
                        "fighter1_total_control_time": None,
                        "fighter2_total_control_time": None,
                        "fighter1_str_head": None,
                        "fighter2_str_head": None,
                        "fighter1_str_body": None,
                        "fighter2_str_body": None,
                        "fighter1_str_leg": None,
                        "fighter2_str_leg": None,
                        "fighter1_str_clinch": None,
                        "fighter2_str_clinch": None,
                        "fighter1_str_ground": None,
                        "fighter2_str_ground": None,
                        "rounds-main": None,
                        "sig_str_rounds_results": None
                    })
    
        return fight_details
    else:
        print("No fights available")
        return False
    
def getFighterDetails(conn, fight_details):
    if(fight_details and conn):
            try:
                cursor = conn.cursor()
                database_fighter_ids = []
                cursor.execute("SELECT fighter_id FROM ufc_fighters")

                for row in cursor.fetchall():
                    for field in row:
                        database_fighter_ids.append(field)
                
                fighter_inside_databse = []
                fighter_not_inside_database = []

                for row in fight_details:
                    fighter1_id = row['fighter1_id']
                    fighter2_id = row['fighter2_id']
                    
                    if fighter1_id in database_fighter_ids:
                        fighter_inside_databse.append(fighter1_id)
                    else:
                        fighter_not_inside_database.append(fighter1_id)

                    if fighter2_id in database_fighter_ids:
                        fighter_inside_databse.append(fighter2_id)
                    else:
                        fighter_not_inside_database.append(fighter2_id)

                fighter_inside_databse = list(set(fighter_inside_databse))
                fighter_not_inside_database = list(set(fighter_not_inside_database))

                fighter_inside_database_updated_data = []
                
                for fighter_id in fighter_inside_databse:
                    try:
                        update_fighter_retry = 0
                        while update_fighter_retry < 5:
                            ufc_event_page = requests.get(f"http://ufcstats.com/fighter-details/{fighter_id}")
                            ufc_event_page_soup = BeautifulSoup(ufc_event_page.text, 'html')

                            fighter_stats = ufc_event_page_soup.find_all("ul", class_="b-list__box-list")[:3]
                            fighter_characteristics = fighter_stats[0].find_all("li")
                            fighter_career_stats_1 = fighter_stats[1].find_all("li")
                            fighter_career_stats_2 = fighter_stats[2].find_all("li")

                            #fighter characteristics
                            height_label = fighter_characteristics[0].find('i').text.strip() 
                            height = fighter_characteristics[0].get_text(strip=True).replace(height_label, '').strip()

                            fighter_record = ufc_event_page_soup.find_all("span", class_="b-content__title-record")[0].get_text(strip=True)
                            main_record = fighter_record.split(":")[1].strip().split(" ")[0]
                            wins, losses, draws = map(int, main_record.split("-"))
                            if "(" in fighter_record:
                                nc = int(fighter_record.split("(")[1].split(" ")[0])
                            else:
                                nc = 0
                            
                            weight_label = fighter_characteristics[1].find('i').text.strip() 
                            weight = fighter_characteristics[1].get_text(strip=True).replace(weight_label, '').strip()

                            reach_label = fighter_characteristics[2].find('i').text.strip() 
                            reach = fighter_characteristics[2].get_text(strip=True).replace(reach_label, '').strip()
                            
                            stance_label = fighter_characteristics[3].find('i').text.strip() 
                            stance = fighter_characteristics[3].get_text(strip=True).replace(stance_label, '').strip()

                            dob_label = fighter_characteristics[4].find('i').text.strip() 
                            dob = fighter_characteristics[4].get_text(strip=True).replace(dob_label, '').strip()

                            #fighter statistics
                            Splm_label = fighter_career_stats_1[0].find('i').text.strip() 
                            Splm = fighter_career_stats_1[0].get_text(strip=True).replace(Splm_label, '').strip()
                            
                            str_acc_label = fighter_career_stats_1[1].find('i').text.strip() 
                            str_acc = fighter_career_stats_1[1].get_text(strip=True).replace(str_acc_label, '').strip()

                            sapm_label = fighter_career_stats_1[2].find('i').text.strip() 
                            sapm = fighter_career_stats_1[2].get_text(strip=True).replace(sapm_label, '').strip()
                            
                            str_def_label = fighter_career_stats_1[3].find('i').text.strip() 
                            str_def = fighter_career_stats_1[3].get_text(strip=True).replace(str_def_label, '').strip()

                            td_avg_label = fighter_career_stats_2[1].find('i').text.strip() 
                            td_avg = fighter_career_stats_2[1].get_text(strip=True).replace(td_avg_label, '').strip()

                            td_acc_label = fighter_career_stats_2[2].find('i').text.strip() 
                            td_acc = fighter_career_stats_2[2].get_text(strip=True).replace(td_acc_label, '').strip()

                            td_def_label = fighter_career_stats_2[3].find('i').text.strip() 
                            td_def = fighter_career_stats_2[3].get_text(strip=True).replace(td_def_label, '').strip()

                            sub_avg_label = fighter_career_stats_2[4].find('i').text.strip() 
                            sub_avg = fighter_career_stats_2[4].get_text(strip=True).replace(sub_avg_label, '').strip()

                            fighter_inside_database_updated_data.append({
                            "fighter_id": fighter_id,
                            "wins": wins,
                            "losses": losses,
                            "draws": draws,
                            "nc": nc,
                            "height": height,
                            "weight": weight,
                            "reach": reach,
                            "stance": stance,
                            "dob": dob,
                            "splm": Splm,
                            "str_acc": str_acc,
                            "sapm": sapm,
                            "str_def": str_def,
                            "td_avg": td_avg,
                            "td_acc": td_acc,
                            "td_def": td_def,
                            "sub_avg": sub_avg
                            })

                            update_fighter_retry += 10

                    except Exception as e:
                        update_fighter_retry += 1
                        if update_fighter_retry <= 5:
                            print(f"Retrying to get fighter id {fighter_id}")
                        else:
                            print(f"Could not get {fighter_id}")
                            return False, False
                    
                fighter_not_inside_database_updated_data = []
                for fighter_id in fighter_not_inside_database:
                    try:
                        update_fighter_retry = 0
                        while update_fighter_retry < 5:
                            ufc_event_page = requests.get(f"http://ufcstats.com/fighter-details/{fighter_id}")
                            ufc_event_page_soup = BeautifulSoup(ufc_event_page.text, 'html')

                            fighter_stats = ufc_event_page_soup.find_all("ul", class_="b-list__box-list")[:3]
                            fighter_characteristics = fighter_stats[0].find_all("li")
                            fighter_career_stats_1 = fighter_stats[1].find_all("li")
                            fighter_career_stats_2 = fighter_stats[2].find_all("li")

                            #fighter characteristics
                            full_name = ufc_event_page_soup.find_all("span", class_="b-content__title-highlight")[0].get_text(strip=True)
                            split_name = full_name.split(" ")
                            first_name = split_name[0]
                            last_name = split_name[1]

                            height_label = fighter_characteristics[0].find('i').text.strip() 
                            height = fighter_characteristics[0].get_text(strip=True).replace(height_label, '').strip()

                            fighter_record = ufc_event_page_soup.find_all("span", class_="b-content__title-record")[0].get_text(strip=True)
                            main_record = fighter_record.split(":")[1].strip().split(" ")[0]
                            wins, losses, draws = map(int, main_record.split("-"))
                            if "(" in fighter_record:
                                nc = int(fighter_record.split("(")[1].split(" ")[0])
                            else:
                                nc = 0
                            
                            weight_label = fighter_characteristics[1].find('i').text.strip() 
                            weight = fighter_characteristics[1].get_text(strip=True).replace(weight_label, '').strip()

                            reach_label = fighter_characteristics[2].find('i').text.strip() 
                            reach = fighter_characteristics[2].get_text(strip=True).replace(reach_label, '').strip()
                            
                            stance_label = fighter_characteristics[3].find('i').text.strip() 
                            stance = fighter_characteristics[3].get_text(strip=True).replace(stance_label, '').strip()

                            dob_label = fighter_characteristics[4].find('i').text.strip() 
                            dob = fighter_characteristics[4].get_text(strip=True).replace(dob_label, '').strip()

                            #fighter statistics
                            Splm_label = fighter_career_stats_1[0].find('i').text.strip() 
                            Splm = fighter_career_stats_1[0].get_text(strip=True).replace(Splm_label, '').strip()
                            
                            str_acc_label = fighter_career_stats_1[1].find('i').text.strip() 
                            str_acc = fighter_career_stats_1[1].get_text(strip=True).replace(str_acc_label, '').strip()

                            sapm_label = fighter_career_stats_1[2].find('i').text.strip() 
                            sapm = fighter_career_stats_1[2].get_text(strip=True).replace(sapm_label, '').strip()
                            
                            str_def_label = fighter_career_stats_1[3].find('i').text.strip() 
                            str_def = fighter_career_stats_1[3].get_text(strip=True).replace(str_def_label, '').strip()

                            td_avg_label = fighter_career_stats_2[1].find('i').text.strip() 
                            td_avg = fighter_career_stats_2[1].get_text(strip=True).replace(td_avg_label, '').strip()

                            td_acc_label = fighter_career_stats_2[2].find('i').text.strip() 
                            td_acc = fighter_career_stats_2[2].get_text(strip=True).replace(td_acc_label, '').strip()

                            td_def_label = fighter_career_stats_2[3].find('i').text.strip() 
                            td_def = fighter_career_stats_2[3].get_text(strip=True).replace(td_def_label, '').strip()

                            sub_avg_label = fighter_career_stats_2[4].find('i').text.strip() 
                            sub_avg = fighter_career_stats_2[4].get_text(strip=True).replace(sub_avg_label, '').strip()

                            fighter_not_inside_database_updated_data.append({
                            "fighter_id": fighter_id,
                            "fighter_first_name": first_name,
                            "fighter_last_name": last_name,
                            "wins": wins,
                            "losses": losses,
                            "draws": draws,
                            "nc": nc,
                            "height": height,
                            "weight": weight,
                            "reach": reach,
                            "stance": stance,
                            "dob": dob,
                            "splm": Splm,
                            "str_acc": str_acc,
                            "sapm": sapm,
                            "str_def": str_def,
                            "td_avg": td_avg,
                            "td_acc": td_acc,
                            "td_def": td_def,
                            "sub_avg": sub_avg,
                            })

                            update_fighter_retry += 10
        
                    except Exception as e:
                        update_fighter_retry += 1
                        if update_fighter_retry <= 5:
                            print(f"Retrying to get fighter id {fighter_id}")
                        else:
                            fighter_not_inside_database_updated_data.append({
                                "fighter_id": fighter_id,
                                "fighter_first_name": None,
                                "fighter_last_name": None,
                                "wins": None,
                                "losses": None,
                                "draws": None,
                                "nc": None,
                                "height": None,
                                "weight": None,
                                "reach": None,
                                "stance": None,
                                "dob": None,
                                "splm": None,
                                "str_acc": None,
                                "sapm": None,
                                "str_def": None,
                                "td_avg": None,
                                "td_acc": None,
                                "td_def": None,
                                "sub_avg": None
                            })
                            print(f"Could not get {fighter_id}")

                return fighter_inside_database_updated_data, fighter_not_inside_database_updated_data

            except Exception as e:
                print(f"Error: {e}")   
                raise(e)  
                
    else:
        print("Could not get the fighter data")
        return False, False