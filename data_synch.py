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

def init_combo_table(data_table, combo_table):

    conn = create_connection()
    conn.autocommit = True

    init_combo_columns(conn, combo_table)
    combo_keys = get_combo_keys(conn, combo_table)

    res = input('Synch top number matches (Y/N)? : ')
    if res.lower() == 'y':
        set_top_counts(conn, combo_table, data_table, combo_keys)

    conn.close()

def init_combo_columns(conn, combo_table):

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


def get_combo_keys(conn, combo_table):
    
    select_sql= f'''
    select combo_key
    from {combo_table}
    '''

    cur = conn.cursor()
    
    try:
        cur.execute(select_sql)
        combos = list(cur.fetchall())

    except:
        return False
    
    cur.close()

    return combos

def set_top_counts(conn, combo_table, data_table, combo_keys):

    init_top_sql= f'''
    update {combo_table}
    set top_count = 0,
        bot_count = 0
    '''

    cur = conn.cursor()
    
    try:
        cur.execute(init_top_sql)

    except:
        return False
    
    cur.close()

    all_numbers = get_top_numbers(conn, data_table)
    top_numbers = all_numbers[:25]
    bot_numbers = all_numbers[-25:]

    for combo_key in list(combo_keys):

        numbers = split_keys(combo_key[0])

        top_count = len(set(top_numbers).intersection(set(numbers)))
        bot_count = len(set(bot_numbers).intersection(set(numbers)))

        update_sql= f'''
        update {combo_table}
        set top_count = {top_count}, bot_count = {bot_count}
        where combo_key = '{combo_key[0]}'
        '''

        cur = conn.cursor()
    
        try:
            cur.execute(update_sql)
        
        except Exception as e:
            print(e)
            return False
    
        cur.close()


def split_keys(combo_key):
        
    numbers = []
    for i in range(0,10,2):
        numbers.append(int(combo_key[i:i+2]))
        
    return numbers

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
        
    except Exception as e:
        print(e)
        return False
    
    cur.close()
    
    return True

def get_top_numbers(conn, data_table):

    select = f'''
    select A.num, sum(A.tot)
    from (
        select numa as num, count(*) as tot from {data_table} group by numa
        union
        select numb as num, count(*) as tot from {data_table} group by numb
        union
        select numc as num, count(*) as tot from {data_table} group by numc
        union
        select numd as num, count(*) as tot from {data_table} group by numd
        union
        select nume as num, count(*) as tot from {data_table} group by nume
        ) A

    group by A.num
    
    order by sum(A.tot) desc, A.num asc

    '''

    cur = conn.cursor()

    cur.execute(select)

    number_counts = cur.fetchall()
    
    number_counts = [n for n, c in number_counts]

    cur.close()

    return number_counts

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

def validate_counts(data_table, combo_table, mode=0):

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

    if mode == 0:
        print(f'Pre-synch stats')
    else:
        print(f'Post-synch stats')

    print(f'Data count      : {data_count}')
    print(f'Winner count    : {winner_count}')

def synch_combo_and_data(data_table, combo_table):

    start = datetime.now()
    print(start)
    
    validate_counts(data_table, combo_table)
    init_combo_table(data_table, combo_table)
    update_combo_table(data_table, combo_table)
    validate_counts(data_table, combo_table,1)

    end = datetime.now()
    print(end)
    print("Time elapsed: ", end - start)

if __name__ == '__main__':
    res = input('Synch Fantasy Five tables (Y/N)? : ')
    if res.lower() == 'y':
        synch_combo_and_data('fantasy_five', 'fantasy_combos')

    res = input('Synch Super Lotto tables (Y/N)? : ')
    if res.lower() == 'y':
        synch_combo_and_data('super_lotto', 'super_combos')

    res = input('Synch Mega Lotto tables (Y/N)? : ')
    if res.lower() == 'y':
        synch_combo_and_data('mega_lotto', 'mega_combos')

    res = input('Synch Powerball tables (Y/N)? : ')
    if res.lower() == 'y':
        synch_combo_and_data('power_ball', 'power_combos')

