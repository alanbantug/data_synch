import requests
import psycopg2
import json
import os

from datetime import datetime, timedelta

def create_connection():
    ''' Create connection to PostgreSQL database
    '''
    with open(r"c:\users\alan\creds\credentials.json", "r") as credentials:
        creds = json.loads(credentials.read())

    conn = psycopg2.connect(database=creds['database'],
    user=creds['user'],
    password=creds['password'],
    host=creds['host'],
    port=creds['port'])

    return conn

def init_combo_table(combo_table):
    
    conn = create_connection()
    conn.autocommit = True
    
    init_sql= f'''
    update {combo_table}
    set win_count = 0,
        win_date = '          '
    '''

    cur = conn.cursor()
    
    try:
        cur.execute(init_sql)
    except:
        return False
    
    cur.close()
    conn.close()

def get_winners(conn, data_table):
    
    select_sql = f'''
    select to_char(draw_date, 'YYYY-MM-DD'), numa, numb, numc, numd, nume
    from {data_table}
    order by draw_date desc
    '''

    cur = conn.cursor()

    cur.execute(select_sql)

    winners = cur.fetchall()
    
    cur.close()
    
    return winners

def get_combo_data(conn, combo_key, combo_table):
    
    select_sql = f'''
    select win_count, win_date
    from {combo_table}
    where combo_key = '{combo_key}'
    '''
    
    cur = conn.cursor()
    
    cur.execute(select_sql)
    
    combo_data = cur.fetchall()
    
    return combo_data

def update_combo_data(conn, combo_key, combo_win_count, combo_win_date, combo_table):
    
    update_sql= f'''
    update {combo_table}
    set win_count = {combo_win_count},
        win_date = '{combo_win_date}'
    where combo_key = '{combo_key}'
    
    '''

    cur = conn.cursor()
    
    try:
        
        cur.execute(update_sql)
        
    except:
        
        return False
    
    cur.close()
    
    return True

def update_combo_table(data_table, combo_table):
    
    rec = 0
    upd = 0
    dup = 0
    
    conn = create_connection()
    conn.autocommit = True
    
    winners = get_winners(conn, data_table)
    
    for winner in winners:
        
        rec += 1
        
        win_data = list(winner)
        
        win_date = win_data[:1][0]
        
        combo_key = ''.join(['{:02d}'.format(int(num)) for num in win_data[1:]])
        
        combo_data = get_combo_data(conn, combo_key, combo_table)
        
        combo_win_count, combo_win_date = combo_data[0]
        
        combo_win_count += 1
        
        if combo_win_date == '          ':
            combo_win_date = win_date
        else:
            dup += 1
        
        if update_combo_data(conn, combo_key, combo_win_count, combo_win_date, combo_table):
            upd += 1
        
    conn.close()

def validate_counts(data_table, combo_table):

    conn = create_connection()
    cur = conn.cursor()

    select_sql = f'''
    select count(*) from {data_table}
    '''

    cur.execute(select_sql)

    data_count = cur.fetchall()[0][0]

    select_sql = f'''
    select sum(win_count) from {combo_table}
    '''

    cur.execute(select_sql)

    winner_count = cur.fetchall()[0][0]

    print(f'Data count      : {data_count}')
    print(f'Winner count    : {winner_count}')

if __name__ == '__main__':
    res = input('Synch Fantasy Five tables (Y/N)? : ')
    if res.lower() == 'y':
        init_combo_table('fantasy_combos')
        update_combo_table('fantasy_five', 'fantasy_combos')
        validate_counts('fantasy_five', 'fantasy_combos')

    res = input('Synch Super Lotto tables (Y/N)? : ')
    if res.lower() == 'y':
        init_combo_table('super_combos')
        update_combo_table('super_lotto', 'super_combos')
        validate_counts('super_lotto', 'super_combos')

    res = input('Synch Mega Lotto tables (Y/N)? : ')
    if res.lower() == 'y':
        init_combo_table('mega_combos')
        update_combo_table('mega_lotto', 'mega_combos')
        validate_counts('mega_lotto', 'mega_combos')

    res = input('Synch Powerball tables (Y/N)? : ')
    if res.lower() == 'y':
        init_combo_table('power_combos')
        update_combo_table('power_ball', 'power_combos')
        validate_counts('power_ball', 'power_combos')

