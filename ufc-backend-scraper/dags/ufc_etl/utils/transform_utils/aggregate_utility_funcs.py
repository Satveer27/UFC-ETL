import pandas as pd
from ufc_etl.utils.transform_utils.transform_utility_funcs import extract_landed_strikes, time_to_timedelta

#Aggregate data
def calculate_sig_strikes_body_part_percent(pd_fight_details):
        pd_fight_details['fighter1_str_head'] = pd_fight_details['fighter1_str_head'].apply(extract_landed_strikes)
        pd_fight_details['fighter2_str_head'] = pd_fight_details['fighter2_str_head'].apply(extract_landed_strikes)
        sig_head_strikes = pd_fight_details.groupby(['fighter1_fight_id'])['fighter1_str_head'].sum()
        sig_head_strikes2 = pd_fight_details.groupby(['fighter2_fight_id'])['fighter2_str_head'].sum()
        total_sig_head_strikes = sig_head_strikes.add(sig_head_strikes2, fill_value=0)

        pd_fight_details['fighter1_str_body'] = pd_fight_details['fighter1_str_body'].apply(extract_landed_strikes)
        pd_fight_details['fighter2_str_body'] = pd_fight_details['fighter2_str_body'].apply(extract_landed_strikes)
        sig_body_strikes = pd_fight_details.groupby(['fighter1_fight_id'])['fighter1_str_body'].sum()
        sig_body_strikes2 = pd_fight_details.groupby(['fighter2_fight_id'])['fighter2_str_body'].sum()
        total_sig_body_strikes = sig_body_strikes.add(sig_body_strikes2, fill_value=0)

        pd_fight_details['fighter1_str_leg'] = pd_fight_details['fighter1_str_leg'].apply(extract_landed_strikes)
        pd_fight_details['fighter2_str_leg'] = pd_fight_details['fighter2_str_leg'].apply(extract_landed_strikes)
        sig_leg_strikes = pd_fight_details.groupby(['fighter1_fight_id'])['fighter1_str_leg'].sum()
        sig_leg_strikes2 = pd_fight_details.groupby(['fighter2_fight_id'])['fighter2_str_leg'].sum()
        total_sig_leg_strikes = sig_leg_strikes.add(sig_leg_strikes2, fill_value=0)

        total_strikes_landed = total_sig_leg_strikes.add(total_sig_head_strikes.add(total_sig_body_strikes, fill_value=0), fill_value=0)

        head_strikes_percentage = total_sig_head_strikes.div(total_strikes_landed, fill_value=0).fillna(0)
        body_strikes_percentage = total_sig_body_strikes.div(total_strikes_landed, fill_value=0).fillna(0)
        leg_strikes_percentage = total_sig_leg_strikes.div(total_strikes_landed, fill_value=0).fillna(0)

        return head_strikes_percentage, body_strikes_percentage, leg_strikes_percentage

def average_time_taken_per_fight(pd_fight_details, total_fights):
    pd_fight_details['time'] = pd_fight_details['time'].apply(time_to_timedelta)
    fighter_1_time = pd_fight_details.groupby(['fighter1_fight_id'])['time'].sum()
    fighter_2_time = pd_fight_details.groupby(['fighter2_fight_id'])['time'].sum()
    time_total_concat_fighters = pd.concat([fighter_1_time, fighter_2_time])
    time_total_bfr_avg = time_total_concat_fighters.groupby(time_total_concat_fighters.index).sum()
    average_time_pft_obj = time_total_bfr_avg.div(total_fights, fill_value=pd.Timedelta(0)).fillna(pd.Timedelta(0))
    return average_time_pft_obj

def avg_succesful_clinches_per_fight(pd_fight_details, total_fights):
    pd_fight_details['fighter1_str_clinch'] = pd_fight_details['fighter1_str_clinch'].apply(extract_landed_strikes)
    pd_fight_details['fighter2_str_clinch'] = pd_fight_details['fighter2_str_clinch'].apply(extract_landed_strikes)
    clinches_pf_f1 = pd_fight_details.groupby(['fighter1_fight_id'])['fighter1_str_clinch'].sum()
    clinches_pf_f2 = pd_fight_details.groupby(['fighter2_fight_id'])['fighter2_str_clinch'].sum()
    total_clinches_pf = clinches_pf_f2.add(clinches_pf_f1, fill_value=0)
    average_clinches_pf = total_clinches_pf.div(total_fights, fill_value=0).fillna(0)
    return average_clinches_pf

def finishing_rate(pd_fight_details, sub_per_fighter, tko_per_fighter):
    combined1 = pd.concat([sub_per_fighter, tko_per_fighter])
    finish_rate_per_fighter = combined1.groupby(['winner_of_fight']).sum()
    total_fights_1 = pd_fight_details.groupby(['fighter1_fight_id']).size()
    total_fights_2 = pd_fight_details.groupby(['fighter2_fight_id']).size()
    total_fights = total_fights_1.add(total_fights_2, fill_value=0)
    finishing_rpf_win = (finish_rate_per_fighter.div(total_fights, fill_value=0).fillna(0)) * 100
    return finishing_rpf_win, total_fights

def total_sig_strikes_pff(pd_fight_details):
    fighter1_total_sig_strikes = pd_fight_details.groupby(['fighter1_id'])['fighter1_str'].sum().reset_index(name='total_f1_sig_strikes')
    fighter2_total_sig_strikes = pd_fight_details.groupby(['fighter2_id'])['fighter2_str'].sum().reset_index(name='total_f2_sig_strikes')
    fighter1_total_sig_strikes = fighter1_total_sig_strikes.rename(columns={'fighter1_id': 'fighter_id', 'total_f1_sig_strikes': 'total_sig_strikes'})
    fighter2_total_sig_strikes = fighter2_total_sig_strikes.rename(columns={'fighter2_id': 'fighter_id', 'total_f2_sig_strikes': 'total_sig_strikes'})
    combined = pd.concat([fighter1_total_sig_strikes, fighter2_total_sig_strikes])
    total_sig_strikes = combined.groupby('fighter_id')['total_sig_strikes'].sum()
    return total_sig_strikes

def add_to_aggregation_array(aggregated_data, array, column_name, columname_dt, datatype):
      array_df = array.reset_index(name=columname_dt)
      array_df.rename(columns={column_name: 'fighter_id'}, inplace=True)

      aggregated_data = pd.merge(
          aggregated_data, array_df,
          on='fighter_id', 
          how='outer'
      )
      
      if datatype == "time":
        aggregated_data.fillna(0, inplace=True)
      else:
        aggregated_data.fillna(0, inplace=True)
      
      return aggregated_data

