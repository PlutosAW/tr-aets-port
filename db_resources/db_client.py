#import pymysql.cursors
import json, time, os
import pandas as pd
from sqlalchemy import create_engine
from db_resources import resources as RES

try: 
    url_db = os.environ['DATABASE_URL']
except: 
    url_db = input("Enter DATABASE_URL: ")


class CVDB:
    class DBError(Exception): pass
    class ConnectionError(Exception): pass

    def __init__(self, url_db=url_db, 
                 verbose=True, safe=True):
        self.verbose = verbose
        self._safe = safe
        self._query_queue = []
        if "postgresql" not in url_db:
            self.url_db = "postgresql://samuelhwang:test123@10.137.23.103:3932/pdtdist"
        else: 
            self.url_db = url_db
        self.engine = self._create_engine()

    def _create_engine(self, connectivity_error=False):
        try:
            connection_string = self.url_db
            engine = create_engine(connection_string)
            if not connectivity_error and self.verbose: 
                print('database started and connection resolved')
            return engine
        except Exception as e:
            print(e)
            if connectivity_error and self.verbose:
                print('database connection issue')
                raise self.DBError
            if self.verbose:
                print('database handler defaulting to localhost')

            # self._user = 'root'
            # self._host_ip = 'localhost'
            # self._password = os.environ['db_local_pass']
            # if self.verbose:
            #     print('failed to log into local database, attempting to start mysql server')
            # os.system('service mysql start')
            # return self._create_engine(connectivity_error=True)

    def _create_connection(self):
        return self.engine.connect()

    def reconnect(self):
        self.engine = self._create_engine()

    def fetch_query(self, sql):
        try:
            with self.engine.connect() as conn:
                response = conn.execute(sql)
                data = response.fetchall()
                conn.close()
        except Exception as e:
            print('Error in query:', sql)
            print('fetch error:', type(e), e)
            raise e
        return data

    def post_query(self, sql, args=None, commit=True):
        self._query_queue.append(sql)
        if args is None: args = []
        while len(self._query_queue) > 1:
            time.sleep(1)
        # print(args)
        try:
            with self.engine.connect() as conn:
                conn.execute(sql, args)
                conn.close()
        except Exception as e:
            if len(sql) < 100:
                print('Error in query:', sql)
            else:
                print('Error in query:', sql[:30] + '...')
            print('do error:', type(e), e)
            raise e
        else:
            self._query_queue.pop(0)

    def push_timeseries(self, strat_id, ts_id, df_p=None, 
                        file_name=None, time_start='2017-01-01 00:00:00', 
                        time_end='2018-10-01 00:00:00', select_timestamp=None,
                        verbose=True, if_exists='append'):
        if df_p is None:
            df = pd.read_csv(file_name)
        elif file_name is None:
            df = df_p
        else:
            raise ValueError("No filename or dataframe passed through")

        if time_start is not None: time_start = RES.parse_time(time_start)
        if time_end is None: pass
        elif time_end == 'NOW': time_end = time.time()
        else: time_end = RES.parse_time(time_end)

        #filter to timeframe
        if time_start is not None:
            df = df.loc[df.trade_time > time_start]
        if time_end is not None:
            df = df.loc[df.trade_time < time_end]

        if select_timestamp is None:
            table_name = strat_id + "_" + str(ts_id) + "_" + str(int(time.time()))
        else:
            table_name = strat_id + "_" + str(ts_id) + "_" + str(select_timestamp)

        #Batch into sub dataframes if too large
        if df.shape[0] > 50000:
            sections = df.shape[0] // 50000
            sub_dfs = RES.split_df(df, sections)
            for i in range(len(sub_dfs)):
                if verbose: print("Uploading part", i, "of", len(sub_dfs)-1, table_name, end=' ')
                s_time = time.time()
                with self.engine.connect() as conn:
                    sub_dfs[i].to_sql(con=conn, name=table_name, if_exists=if_exists)
                    if verbose: print(time.time() - s_time)
                    conn.close()

        else:
            if verbose: print("Uploading", table_name)
            with self.engine.connect() as conn:
                df.to_sql(con=conn, name=table_name, if_exists=if_exists)
                conn.close()

    def get_old_table_names(self, strat_id, ts_id):
        table_names = self.engine.table_names()
        table_names = [data[0] for data in table_names]
        select_names = [name for name in table_names if strat_id + "_" + ts_id + "_" in name]
        timestamps = [int(name.split('_')[-1]) for name in select_names]
        print(select_names)
        # print(timestamps)
        if len(timestamps) != 0: select_names.remove(strat_id + "_" + ts_id + "_" + str(max(timestamps)))
        return select_names

    def get_most_recent_table_name(self, strat_id, ts_id):
        # table_names = self.fetch_query("SHOW TABLES")
        # table_names = [data[0] for data in table_names]
        table_names = self.engine.table_names()
        select_names = [name for name in table_names 
                        if strat_id + "_" + ts_id + "_" in name]
        timestamps = [int(name.split('_')[-1]) for name in select_names]
        # print(select_names)
        # print(timestamps)
        return strat_id + "_" + ts_id + "_" + str(max(timestamps))

    def pull_timeseries(self, strat_id, select_timeseries=None):
        """
        :param strat_id: strategy id ie "IBETH0"
        :param select_timeseries: a dict of form {ts_id: select epoch version} None epoch gives most recent
        :return: a dict of the same ts_ids with their values as the dataframe ie {'ts1': df_ts1, 'ts2a': df_ts2a}
        """
        if select_timeseries is None: select_timeseries = {'ts1': None, 'ts2a': None}
        rtrn_dict = {}
        for ts_id in select_timeseries.keys():
            print(ts_id)
            if select_timeseries[ts_id] is None: curr_tbl_name = self.get_most_recent_table_name(strat_id, ts_id)
            else: curr_tbl_name = strat_id + "_" + ts_id + "_" + str(select_timeseries[ts_id])
            conn = self.engine.connect().connection
            query = '''SELECT * FROM "{}" '''.format(curr_tbl_name), 
            curr_df = pd.read_sql(query,
                con=conn, index_col='index')
            rtrn_dict[ts_id] = curr_df
            conn.close()

        return rtrn_dict

    def create_table(self, table_name, table_schema = None):
        if table_schema:
            query = '''
            CREATE TABLE "{table}"  {schema}
            '''.format(table = table_name, schema = table_schema)
        else:
            query = '''
            CREATE TABLE "{}"  ( 
                item TEXT PRIMARY KEY, 
                data TEXT
                ) '''.format(table_name)
        with self.engine.connect() as conn:
            response = conn.execute(query)
            conn.close()
        
        return response

    def drop_table(self, table_name):
        query = '''
        DROP TABLE {}
        '''.format(table_name)
        with self.engine.connect() as conn:
            response = conn.execute(query)
            conn.close()
        
        return response

    def truncate_tables(self, must_include=''):
        if self._safe: return
        table_names = [data['Tables_in_' + self._db_name] 
            for data in self.engine.table_names()]
        print(table_names)
        for table_name in table_names:
            if must_include in table_name or must_include == '':
                self.post_query("TRUNCATE TABLE " + table_name)
        table_names = [data['Tables_in_' + self._db_name] for data in 
                       self.engine.table_names()]
        print(table_names)

    def drop_tables(self, must_include=''):
        if self._safe: return
        table_names = [data['Tables_in_' + self._db_name] for data in 
                       self.engine.table_names()]
        print(table_names)
        for table_name in table_names:
            if must_include in table_name or must_include == '':
                self.post_query("DROP TABLE " + table_name)
        table_names = [data['Tables_in_' + self._db_name] for data in 
                       self.engine.table_names()]
        print(table_names)

    def add_new_row(self, strat_id, ts_id, stats):
        table_name = self.get_most_recent_table_name(strat_id, ts_id)
        cols = ', '.join(stats.keys())
        values = ', '.join(stats.values())
        sql_query = "INSERT INTO " + table_name + " (" + cols + ") VALUES (" + values + ")"
        print(sql_query)
        self.post_query(sql_query)

    def update_data(self, data_input, table='Ex_Historical_Data'):
        '''
        Parameters
        ----------
        table : table name
        data_input : dict {cross: data}
        '''
        params = []
        for cross in data_input:            
            data = json.dumps(data_input[cross])    
            params.append((cross, data))

        query = ''' INSERT INTO "{table_name}" (item, data) 
        VALUES (%s, %s)
        ON CONFLICT (item) DO UPDATE 
          SET data = excluded.data;
        '''.format( table_name=table)
   
        with self.engine.connect() as conn:  
            conn.execute(query, tuple(params) )       
            conn.close()

    # revise push data
    def push_data(self, data_input, table='Ex_Historical_Data'):
        '''
        Parameters
        ----------
        table : table name
        data_input : dict {cross: data}
        '''
        # find column names
        count = 0
        try: 
            columns = self._req_db_cols(table)
            print ('data loaded!')    
        except Exception as e:
            if count < 2:
                columns = self._req_db_cols(table)
                count += 1
            else:    
                print(e)
        # prepare data
        params = []
        for cross in data_input:            
            if type(data_input[cross]) == type([]):
                data = json.dumps(data_input[cross][0]) 
                t = (cross, data, data_input[cross][1])
            else:
                data = json.dumps(data_input[cross])  
                if 'update_time' in columns:
                    t = (cross, data, int(time.time()))
                else:
                    t = (cross, data)
            params.append(t)
            
        if 'update_time' in columns:
            query = ''' INSERT INTO "{table_name}" (item, data, update_time) 
            VALUES (%s, %s, %s)
            '''.format( table_name=table)
        else:
            query = ''' INSERT INTO "{table_name}" (item, data) 
            VALUES (%s, %s)
            '''.format( table_name=table)
           
        with self.engine.connect() as conn:  
            conn.execute(query, tuple(params) )       
            conn.close()
        return
    
    def _req_db_cols (self, table):
        query = ''' SELECT
            column_name FROM information_schema.columns
        WHERE
            table_name = '{table_name}';
        '''.format( table_name=table) 
        with self.engine.connect() as conn:  
            response = conn.execute(query)
            resq = response.fetchall()
            conn.close()
        columns = [item[0] for item in resq]
        return columns
        # query = ''' select * from "{table_name}"  
        #     where false '''.format( table_name=table)
        # conn = self.engine.connect().connection
        # df = pd.read_sql_query(query, con=conn)  
        # conn.close()
        # return df
        
    ## revised to add update_time
    def load_data(self, table='Ex_Historical_Data', 
                  item_list = None,
                  cutoff_time= None):
        '''
        Parameters
        ----------
        table : table name
        ---------
        return data_input : dict {cross: data}
        '''
        # find column names
        count = 0
        try: 
            columns = self._req_db_cols(table)
            print ('data loaded!')    
        except Exception as e:
            if count < 2:
                columns = self._req_db_cols(table)
                count += 1
            else:    
                print(e)
        
        # pull data        
        if 'update_time' in columns:
            if not cutoff_time:
                cutoff_time = int(time.time())
            query = ''' SELECT * FROM "{table_name}"  A
                        INNER JOIN
                        (select item, max(update_time) as update_time from "{table_name}" 
                                 where update_time < {cutoff_time}
                                 group by item)  B
                        on (A.item, A.update_time)=(B.item, B.update_time)
            '''.format( table_name=table, cutoff_time=cutoff_time)
        elif item_list:
            query = '''
            SELECT  * FROM  "{table_name}"
            where item in ({item_list})
            '''.format(table_name= table, item_list = "'" + "', '".join(item_list) + "'")       
        else:
            query = ''' SELECT * FROM "{table_name}"  
            
            '''.format( table_name=table)
        print ('loading data from db....')
    
        with self.engine.connect() as conn:  
            response = conn.execute(query)
            resq = response.fetchall()
            conn.close()
                       
        data_input = {}
        for item in resq:            
            cross, data = item[0], item[1]
            data_input[cross] = json.loads(data)    
    
        return data_input    
    
if __name__ == "__main__":
    pass
    # db = CVDB(specify='production_live', password=pswrd)
    # print(db.engine.table_names())

    # db.truncate_tables(must_include='live1')
    # ts = str(int(time.time()))
    # db.add_new_row('METH', 'test7', {'last_analysis': ts, 'trade_time': ts, 'st': str(-1)})
    # db.add_new_row('METH', 'test7', {'last_analysis': ts, 'trade_time': ts, 'st': str(-1), 'missing_hours': "'INCEPTION'"})
    # db.add_new_row('VBTC', 'live1', {'last_analysis': ts, 'trade_time': ts, 'st_hlh': str(1), 'st_hhl': str(1), 'missing_hours': "'CORRECTION'"})
    # db.add_new_row('VXRP', 'live1', {'last_analysis': ts, 'trade_time': ts, 'st_hhl': str(0), 'st_hlh': str(0), 'missing_hours': "'INCEPTION'"})

