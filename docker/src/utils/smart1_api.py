import os
import sys
import requests
import pandas as pd

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta



class Read:
    '''
    Read data sensors from https://data.smart1.eu
    Link on documentation API:
    https://data.smart1.eu/s/W3M4E8EkqMAPWqL?dir=undefined&path=%2F04%20SOFTWARE%20%26%20FIRMWARE%2F02%20PORTAL%2FEN&openfile=674
    input: token, static_link, read_interval
        where token - token from https://data.smart1.eu
            static_link - link for receiving data about sensor. About link in documentation API
            read_interval - link's parameters. About link's parameters in documentation API
    output: dict_data_sensor in format dict{key=Name : value=df_sensor}
        where Name - name sensor
            df_sensor - dataframe with columns=['Timestamp', 'Measurements']
    example: data = Read(
                token = <your token> ,
                static_link = 'https://portal.smart1.eu/export/data/csv/376/linear/',
                read_interval = 'month'
             )
            dict_data_sensor = data.general_period(name_sensor="PV Over-Production", sensor_id='arithmetic_1464947907')
            print(dict_data_sensor)
            > {'PV Over-Production':                  Timestamp Measurements
                0      2023-01-01 00:04:36            0
                1      2023-01-01 00:09:36            0
                ...
              }
    '''

    def __init__(self, token, static_link, read_interval):
        self.token = token
        self.static_link = static_link
        self.read_interval = read_interval

    def read_data_api(self, sensor_id, last_date, name_sensor):
        name_sensor = self.name_to_format(name_sensor)
        str_date = last_date.strftime("%Y%m%d") + "/"
        api_link = self.static_link + self.read_interval + '/detailed/' + str_date + sensor_id + self.token
        response = requests.head(api_link)

        log_file_path = "read_write_log.txt"

        if not os.path.exists(log_file_path):
            open(log_file_path, 'w').close()

        with open(log_file_path, "a") as log_file:
            sys.stdout = log_file

            print(f'api_link: {api_link}')
            print(f'HTTP status code: {response.status_code}')
            print(f'Server answer time: {response.elapsed.total_seconds()}')
            print(f'Size answer byte: {len(response.content)}')
            print(f'Cookies: {response.cookies}')

            now = datetime.now()
            time_formatted = now.strftime("%H:%M:%S")
            milliseconds = now.microsecond // 1000
            milliseconds_formatted = f"{milliseconds:02d}"
            date_formatted = now.strftime("%d.%m.%Y")
            formatted_datetime = f"{date_formatted} - {time_formatted}:{milliseconds_formatted}"
            print(f'Start reading in: {formatted_datetime}')

        sys.stdout = sys.__stdout__

        df = pd.read_csv(api_link, sep=';')

        with open(log_file_path, "a") as log_file:
            sys.stdout = log_file

            now = datetime.now()
            time_formatted = now.strftime("%H:%M:%S")
            milliseconds = now.microsecond // 1000
            milliseconds_formatted = f"{milliseconds:02d}"
            date_formatted = now.strftime("%d.%m.%Y")
            formatted_datetime = f"{date_formatted} - {time_formatted}:{milliseconds_formatted}"
            print(f'End reading in: {formatted_datetime}')

        sys.stdout = sys.__stdout__

        df = df.rename(columns={'Value1': name_sensor})
        return df

    def general_period(self, name_sensor, sensor_id, last_date):
        name_sensor = self.name_to_format(name_sensor)

        '''
        date_start - date start of measurements.
        Real date_start = '2023-01-01', for example date_start = '2023-01-01'
        '''
        date_start = '2016-06-10'
        date_start = datetime.strptime(date_start, '%Y-%m-%d')
        # last_date = recive_last_date(table_name)
        '''
        last_date pull up from table influxdb with name as name_sensor
        last_date need for definition of the last upload date. 
        It need for definition necessary read interval
        '''
        '''if last_date is not None:
            last_date = datetime.strptime(last_date, '%Y-%m-%dT%H:%M:%SZ')
        else:
            last_date = date_start'''
        if last_date is None:
            last_date = date_start
        df_general_period = pd.DataFrame(columns=["Timestamp", name_sensor])
        df_general_period['Timestamp'] = pd.to_datetime(df_general_period['Timestamp'])
        df_general_period[name_sensor] = df_general_period[name_sensor].astype(float)
        yesterday_date = datetime.today() - timedelta(days=1)
        while yesterday_date > last_date:
            df_sensor = self.read_data_api(sensor_id=sensor_id, last_date=last_date,
                                           name_sensor=name_sensor)
            if df_sensor.columns.tolist() == ['Errorcode', 'Errormessage']:
                if isinstance(last_date, str):
                    last_date = datetime.strptime(last_date, '%Y-%m-%d')
                last_date = last_date + relativedelta(months=1)
                #last_date = last_date.strftime('%Y-%m-%d')
                print(f' Отработало я прибавил 1 месяц дата {last_date}')
                continue
            df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)
            dict_par = {'month': 'months', 'day': 'days', 'year': 'years'}
            last_date += relativedelta(**{dict_par[self.read_interval]: 1})
            df_general_period['Timestamp'] = pd.to_datetime(df_general_period['Timestamp'])

            df_general_period = df_general_period[['Timestamp', name_sensor, 'Value2', 'LinearId']]

            # print(last_date, type(last_date))
        return name_sensor, df_general_period

    def name_to_format(self, name):
        name = ''.join(['_' if not c.isalnum() else c for c in name])
        name = name.rstrip('_')
        name = '_'.join([w for w in name.split('_') if w])
        name = name.lower()
        return name

    def read_sensors(self):
        sensors_link = self.static_link + self.token
        print(sensors_link)
        df = pd.read_csv(sensors_link, sep=';')
        return df
