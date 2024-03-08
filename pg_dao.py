import sqlalchemy
import pandas as pd
import datetime


# エンジン取得
def get_engine(logger , host , dbname , port , user , password , connect_timeout , query_timeout):
    
    err_flg = 0    
    err_msg = ""
    engine = None

    try:
        # 接続情報URLをセット 
        url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        # エンジン作成
        engine = sqlalchemy.create_engine(url , connect_args={'connect_timeout': connect_timeout , "options": f"-c statement_timeout={query_timeout}"})
    except sqlalchemy.exc.SQLAlchemyError as ex:
        err_flg = 1
        err_msg = "エンジンオブジェクトの作成時にエラーが発生しました。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(host , dbname , port , user , password)
    except Exception as ex:
        err_flg = 1
        err_msg = "エンジンオブジェクトの作成時に想定外のエラーが発生しました。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(host , dbname , port , user , password)
    finally:
        return engine , err_flg , err_msg
    

# 前回更新日時取得
def get_before_time(logger , items , maker_id):

    err_flg = 0    
    err_msg = ""
    ret_result = None

    # エンジン取得
    engine , err_flg , err_msg = get_engine(logger , items.host , items.db , items.port , items.user , items.password , items.connect_timeout , items.s_query_timeout)
    
    # エンジンチェック
    if engine == None:
        # 処理を停止
        return ret_result , err_flg , err_msg
        
    query = items.time_sql.format(maker_id)
    
    try:
        with engine.connect() as con:
            # クエリ実行
            rows = con.execute(query)

            if rows.rowcount != 0:
                # 更新日時をセット
                for row in rows:                
                    ret_result = row.update_date
            else:
                # ２時間前の日時をセット
                current_time = datetime.datetime.now()
                ret_result =  current_time + datetime.timedelta(hours=-2)       
        # リソースの開放
        engine.dispose()
    except sqlalchemy.exc.TimeoutError:
        err_flg = 1
        err_msg = "前回更新日時のセレクト時に接続タイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.OperationalError:
        err_flg = 1
        err_msg = "前回更新日時のセレクト時に接続先が不正、もしくはクエリタイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.SQLAlchemyError as ex:
        err_flg = 1
        err_msg = "前回更新日時のセレクト時にエラーが発生しました。"
    except Exception as ex:
        err_flg= 1
        err_msg = "前回更新日時のセレクト時に想定外のエラーが発生しました。"
    finally:
        return ret_result , err_flg , err_msg
    

# Machineマスタ取得
def select_machine_master(logger , items):
    
    err_flg = 0    
    err_msg = ""
    ret_df = pd.DataFrame()

    # エンジン取得
    engine , err_flg , err_msg = get_engine(logger , items.host , items.db , items.port , items.user , items.password , items.connect_timeout , items.s_query_timeout)
    
    # エンジンチェック
    if engine == None:
        # 処理を停止
        return ret_df , err_flg , err_msg
    
    query = items.machine_sql

    try:
        with engine.connect() as con:
            # セレクトを実行してDataFrameで取得
            ret_df = pd.read_sql(sql=query , con=con)
        # リソースの開放
        engine.dispose()
    except sqlalchemy.exc.TimeoutError:
        err_flg = 1
        err_msg = "Machineのセレクト時に接続タイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.OperationalError:
        err_flg = 1
        err_msg = "Machineのセレクト時に接続先が不正、もしくはクエリタイムアウトクエリタイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)  
    except sqlalchemy.exc.SQLAlchemyError as ex:
        err_flg = 1
        err_msg = "Machineマスタのセレクト時にエラーが発生しました。"
    except Exception as ex:
        err_flg = 1
        err_msg = "Machineマスタのセレクト時に想定外のエラーが発生しました。"
    finally:
        return ret_df , err_flg , err_msg
    

# GWマスタ取得
def select_gateway_master(logger , items , area_id , maker_id):

    err_flg = 0    
    err_msg = ""
    ret_df = pd.DataFrame()

    # エンジン取得
    engine , err_flg , err_msg = get_engine(logger , items.host , items.db , items.port , items.user , items.password , items.connect_timeout , items.s_query_timeout)
    
    # エンジンチェック
    if engine == None:
        # 処理を停止
        return ret_df , err_flg , err_msg
    
    query = items.gateway_sql.format(area_id , maker_id)

    try:
        with engine.connect() as con:
            # timeout_sql = 'SET statement_timeout TO 1000'
            # con.execute(timeout_sql)
            # stop_sql_test = "SELECT pg_sleep(10)"
            # con.execute(stop_sql_test)
            # con.execute('SET statement_timeout TO 10000')
            # stop_query = "SELECT pg_sleep(15)"
            # con.execute(stop_query)
            # セレクトを実行してDataFrameで取得
            ret_df = pd.read_sql(sql=query , con=con)
        # リソースの開放
        engine.dispose()
    except sqlalchemy.exc.TimeoutError:
        err_flg = 1
        err_msg = "GWマスタのセレクト時に接続タイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.OperationalError:
        err_flg = 1
        err_msg = "GWマスタのセレクト時に接続先が不正、もしくはクエリタイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.SQLAlchemyError as ex:
        err_flg = 1
        err_msg = "GWマスタのセレクト時にエラーが発生しました。"
    except Exception as ex:
        err_flg = 1
        err_msg = "GWマスタのセレクト時に想定外のエラーが発生しました。"
    finally:
        return  ret_df , err_flg , err_msg
    

# センサー情報登録
def insert_sensor_table(logger , items , dict_df):

    err_flg = 0    
    err_msg = ""
    regist_count = 0
    
    # エンジン取得
    engine , err_flg , err_msg = get_engine(logger , items.host , items.db , items.port , items.user , items.password , items.connect_timeout , items.i_query_timeout)
    
    # エンジンチェック
    if engine == None:
        # 処理を停止
        return regist_count , err_flg , err_msg

    try:
        with engine.begin() as con:
            for df in dict_df.values():
                # 一括登録
                df.to_sql(name='sensor_manage_table' , schema='public' , con=con , if_exists='append' , index=None, method='multi' , chunksize=int(items.chunksize))
                # 処理件数をカウントアップ
                regist_count += df.shape[0]
        # リソースの開放
        engine.dispose()
    except sqlalchemy.exc.TimeoutError:
        err_flg = 1
        err_msg = "センサーテーブルへの登録時に接続タイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.OperationalError:
        err_flg = 1
        err_msg = "センサーテーブルへの登録時に接続先が不正、もしくはクエリタイムアウトが発生しました。再度実行 または、管理者に問い合わせてください。host:{0} , dbname:{1} , port:{2} , user:{3} , password:{4}".format(items.host , items.db , items.port , items.user , items.password)
    except sqlalchemy.exc.SQLAlchemyError as ex:
        err_flg = 1
        err_msg = "センサーテーブルへの登録時にエラーが発生しました。\n ロールバックが実行されました。"
    except Exception as ex:
        err_flg = 1
        err_msg = "センサーテーブルへの登録時に想定外のエラーが発生しました。\n ロールバックが実行されました。"
    finally:
        return regist_count , err_flg , err_msg
