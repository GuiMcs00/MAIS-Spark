import os
import re
from datetime import datetime
from pyspark.sql import Row




def is_date(line):

    date_pattern = re.compile(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}')
    date_match = date_pattern.search(line)
    if date_match:
        date = datetime.strptime(date_match.group(0), '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
        return date
    return False


def is_group(line):
    group_pattern = re.compile(r'(@[^\s]+|G[A-Z]-[\w-]+|Domain Users|M_SECRE)')
    group_access_pattern = re.compile(r'\((N|R|RX|W|RX, W|M|F)\)')
    group_match = group_pattern.search(line)
    if group_match:
        current_group = group_match.group(1)
        group_access_match =  group_access_pattern.search(line)
        current_group_access_level = group_access_match.group(1) if group_access_match else None
        return current_group, current_group_access_level
    return False


def is_user(line, current_group, current_access_level):
    
    user_data = {}
    user_pattern = re.compile(r'([A-Z]{5}\.[A-Z]+|[AB]\d{7})(?:\((N|R|RX|W|RX, W|M|F)\))?')
    user_match = user_pattern.search(line)
    if user_match:
        
        user = user_match.group(1)
        access_level = user_match.group(2) if user_match.group(2) else current_access_level

        if "(" in user_match.group(0) and ")" in user_match.group(0):
            current_group = "SEM GRUPO ASSOCIADO"
        
        user_data[user] = [{'group': current_group, 'access_level': access_level}]

        return user_data
    
    return False


def extract_data(line, state):
    print(line)
    if  r'\\sbcd' in line:
        state['Folder'] = line
        
    elif is_date(line):
        state['Date'] = is_date(line)

    elif is_group(line):
        state['current_group'], state['current_group_access_level'] = is_group(line)

    elif is_user(line, state['current_group'], state['current_group_access_level']):
        user = is_user(line, state['current_group'], state['current_group_access_level'])
        for key, value in user.items():
            if key in state['Users']:
                state['Users'][key].extend(value)
            else:
                state['Users'][key] = value

    return state


def build_data(state):

    complete_data ={
            'Folder': state.get('Folder'),
            'Date': state.get('Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            'Users': state.get('Users', {})
        }

    return complete_data


def process_lines(lines):

    state = {'Folder': None,
                   'Date': None,
                   'Users': {},
                   'current_group': None,
                   'current_group_access_level': None}
    server_data =[]
    
    for line in lines:
        line = line.value
        if r'\\sbcd' in line:
            if state['Folder'] is not None:
                server_data.append(build_data(state))
                state = {'Folder': None,
                   'Date': None,
                   'Users': {},
                   'current_group': None,
                   'current_group_access_level': None}

        state = extract_data(line, state)

    if state['Folder'] is not None:
             server_data.append(build_data(state))
        

    return [Row(**data) for data in server_data]


def reading_task(spark):
    pasta_info = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'info')
    s103 = 'USUARIOSxPASTA103.txt'
    s103Teste = 'TESTE103.txt'
    s207 = 'USUARIOSxPASTA207.txt'
    s207Teste = 'TESTE207.txt'

    df_103 = spark.read.text(os.path.join(pasta_info, s103))
    df_207 = spark.read.text(os.path.join(pasta_info, s207))

    rdd_103_extracted = df_103.rdd.mapPartitions(process_lines)
    rdd_207_extracted = df_207.rdd.mapPartitions(process_lines)

    df_grouped_103 = rdd_103_extracted.toDF()
    df_grouped_207 = rdd_207_extracted.toDF()

    # df_grouped_103.printSchema()
    df_grouped_103.show(truncate=False)
    df_grouped_207.show(truncate=False)
