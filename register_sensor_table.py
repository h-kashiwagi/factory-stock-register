from pathlib import Path
import pandas as pd
import sys
import concurrent.futures
import time
import json
import pg_dao
import traceback
import configparser
import items
from logging import getLogger, Formatter , DEBUG
from logging.handlers import TimedRotatingFileHandler

import clr

# .NETライブラリを参照
clr.AddReference("System")

# 各種DLLを参照
clr.AddReference("CmsLib")
clr.AddReference("CmsLibIntec")
clr.AddReference("CmsLibGpcass")

# C# 名前空間・クラス
from System import DateTime 


# logger変数
logger = None

# ログの初期化
def init_log(module , path):

    try:
        logger = getLogger(module)
        handler = TimedRotatingFileHandler(path, when='D',backupCount=10, interval=10, encoding='utf-8')
        handler.setLevel(DEBUG)
        handler.setFormatter(Formatter('%(asctime)s--%(levelname)s-%(message)s'))
        logger.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.propagate = False
    except Exception as ex:
        print("ロガーの設定中にエラーが発生しました。{0}".format(ex))
        print(traceback.format_exc())
        return None
    else:    
        return logger


def get_json_file(path):

    json_data = None

    try:
        json_open = open(path , mode='r' , encoding='utf-8')
        json_data = json.load(json_open)
    except FileNotFoundError:
        logger.error("data_config.jsonファイルが見つかりませんでした。")
        logger.error(traceback.format_exc())
    except Exception as ex:
        logger.error("data_config.jsonファイルのデータを取得する途中でエラーが発生しました。 {0}".format(ex))
        logger.error(traceback.format_exc())
    finally:
        return json_data


# 子スレッド実行関数
def child_proc(json_data, class_ins_dict , machine_df , maker_id , area_id , gw_id , f_time , t_time , con_string):

    err_flg = 0
    err_msg = ""
    df_dict = {}
    
    # クラスインポートよりも後に読み込む必要がある
    from dll_cls import DllAction

    try:
        # Python子スレッドクラスのインスタンスを生成
        ''' 引数
        :class_ins:DLLインスタンス
        :json_data:JSONデータ
        :from_time:getDataTable()の引数
        :to_time:getDataTable()の引数
        '''
        child_proc_ins = DllAction(json_data , class_ins_dict , machine_df , maker_id , area_id , gw_id , con_string , f_time , t_time)

        # 子スレッドを実行して結果を取得
        ''' 戻り値
        :df_dict:データフレームの辞書
        :err_flg:子スレッドのエラー判定フラグ
        :err_msg:子スレッドで発生したエラー内容
        '''
        df_dict , err_flg , err_msg = child_proc_ins.main()

        # エラーメッセージに情報を追記
        err_msg = err_msg.format(json_data[str(maker_id)]["maker_name"] , con_string)

    except Exception as ex:
        err_flg = 1
        err_msg = "子スレッドの実行処理中にエラーが発生しました。 maker:{0} , IP:{1}"
        err_msg = err_msg.format(json_data[str(maker_id)]["maker_name"] , con_string)
        print("子スレッドの実行処理中にエラーが発生しました。{0}".format(ex))
    finally:
        return df_dict , err_flg, err_msg


def main():


    try:
        # reading config
        config_ini = configparser.ConfigParser()
        config_ini.read(Path() / 'setting.ini', encoding='utf-8')
        db_con_conf = configparser.ConfigParser()
        db_con_conf.read(Path() / 'db_con.conf', encoding='utf-8')
        # DB 
        items.host = config_ini['postgresql_info']['host']
        items.port = config_ini['postgresql_info']['port']
        items.db = config_ini['postgresql_info']['db']
        items.user = config_ini['postgresql_info']['user']
        items.password = config_ini['postgresql_info']['password']
        items.connect_timeout = config_ini['postgresql_info']['connect_timeout']
        items.chunksize = config_ini['postgresql_info']['chunksize']
        items.i_query_timeout = config_ini['postgresql_info']['i_query_timeout']
        items.s_query_timeout = config_ini['postgresql_info']['s_query_timeout']
        # SQL
        items.time_sql = config_ini['sql_info']['time_sql']
        items.machine_sql = config_ini['sql_info']['machine_sql']
        items.gateway_sql = config_ini['sql_info']['gateway_sql']
        # file
        items.log_path = config_ini['log_info']['log_path']
        items.json_path = config_ini['json_info']['json_path']
        # thread
        items.thread_count = config_ini['thread_info']['thread_count']
        items.thread_timeout = config_ini['thread_info']['thread_timeout']
    except Exception as ex:
        print("設定ファイルの読込に失敗しました。")
        print(traceback.format_exc())
        sys.exit()


    # loggerセットアップ
    try:
        logger = init_log(__name__ , items.log_path)
    except Exception as ex:
        print("ロガーの作成時にエラーが発生しました。")
        print(traceback.format_exc())
        sys.exit()


    # loggerセットアップ完了確認
    if logger != None:      
        logger.info("起動しました。")
    else:
        print("ロガーの作成に失敗しました。")
        sys.exit()

    start = time.time()
    logger.info("プロセス開始")
    print("プロセス開始")
    
    try:
        # パラメータ取得処理
        try:
            args = sys.argv
            print("sys.argv = {}".format(sys.argv))
            # パラメータチェック実行
            if(len(args) <= 1):
                raise ValueError("パラメータが存在しません。")
            else:
                # パラメータセット
                area_id = args[1]
                maker_id = args[2]
                logger.info("入力されたパラメータ [area_id]= {0} , [maker_id]= {1}".format(args[1] , args[2]))
        except ValueError and IndexError:
            logger.error("パラメータが不正です。")
            logger.error(traceback.format_exc())
            sys.exit()
        except Exception as ex:
            logger.error("パラメータ取得時に想定外のエラーが発生しました。")
            logger.error(traceback.format_exc())
            sys.exit()


        # JSONファイルのデータを取得する
        json_data = get_json_file(items.json_path)
        if json_data == None:
            logger.error("JSONデータが取得出来なかった為、処理を終了します。")
            sys.exit()

        
        # DLLロード
        try:
            # JSONからDLLのパスを取得
            path = json_data[str(maker_id)]["DLL_path"]
            # ロード実行
            clr.AddReference(path)
        except Exception as ex:
            logger.error("DLLファイルをロードする際にエラーが発生しました。")
        
        # ※万が一DLLパスからの参照に失敗しても、上で読んだフォルダ直下のDLLを使用する
        # from 名前空間 import クラス名
        from CmsLibIntec import DayDataPlusMaster as intec_vibration , CurrentDiagDayData as intec_current


        # 前回更新日時取得
        time_result , err_flg , err_msg = pg_dao.get_before_time(logger , items , maker_id)
        if err_flg == 1:
            logger.error(err_msg)
            logger.error("DLLパラメータの日時が取得出来なかった為、処理を終了します。")
            sys.exit()


        # 前回(f_time)/現在(t_time)
        f_time = DateTime.Parse(time_result.strftime('%Y/%m/%d %H:%M:%S'))
        t_time = DateTime.Now

        import datetime
        # 仮
        kari = datetime.datetime(2021, 9, 1, 3, 30, 20)
        f_time = DateTime.Parse(kari.strftime('%Y/%m/%d %H:%M:%S'))

        # Machineマスタ取得
        machine_df , err_flg , err_msg = pg_dao.select_machine_master(logger , items)
        # データ取得チェック
        if err_flg == 1:
            logger.error(err_msg)
            logger.error("Machineマスタからデータが取得出来なかった為、処理を終了します。")
            sys.exit()


        # GWマスタ取得
        gw_df , err_flg , err_msg = pg_dao.select_gateway_master(logger , items , area_id , maker_id)
        # データ取得チェック
        if err_flg == 1:
            logger.error(err_msg)
            logger.error("GWマスタからデータが取得出来なかった為、処理を終了します。")
            sys.exit()


        # DLL各種メソッド実行別のクラスオブジェクトを辞書に格納
        class_ins_dict = {}
        try:
            for kind in json_data[str(maker_id)]["proc_list"]:
                class_ins_dict[kind] = eval(json_data[str(maker_id)]["maker_name"] + "_" + kind)()
        except Exception as ex:
            logger.error("JOSNファイルからクラスオブジェクトを作成する際にエラーが発生しました。")
            sys.exit()
        

        # マルチスレッド処理
        max_workers = int(items.thread_count)
        timeout = int(items.thread_timeout)
        futures = []
        concat_dict = {}

        logger.info("マルチスレッド開始")
        print("マルチスレッド開始")
        multi_time = time.time()

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

                for row in gw_df.itertuples():
                    # confファイルのキーとなる値 [maker_id (manufacture_code)、area_id、gw_id] を作成する
                    con_string_key = str(maker_id).zfill(2) + str(area_id).zfill(2) + str(row.gw_code).zfill(3)
                    con_string = db_con_conf.get('connection_string', str(con_string_key))
                    # 子スレッドを実行
                    futures.append(executor.submit(child_proc, json_data, class_ins_dict, machine_df, maker_id, row.area_id,row.gw_code, f_time, t_time ,con_string))
                # 実行したスレッドの結果をループで取得する
                for future in concurrent.futures.as_completed(futures , timeout=timeout):
                    # エラーフラグチェック
                    # 0 正常　1 汎用エラー 2 GetDataTableエラー
                    if future.result()[1] == 1:
                        # エラーメッセージをログ出力
                        logger.error(format(future.result()[2]))
                    else:
                        if future.result()[1] != 0:
                            logger.warning(format(future.result()[2]))
                        # 振動、温度、電流ごとに結合する対象のDFを取り出す
                        for key, df_val in future.result()[0].items():
                            if key in concat_dict.keys():
                                # 辞書の中のDataFrameを結合
                                concat_dict[key] = pd.concat([concat_dict[key], df_val], axis=0)
                            else:
                                if not df_val.empty:
                                    concat_dict[key] = df_val            
        except concurrent.futures._base.TimeoutError:
            logger.error("マルチスレッドでタイムアウトエラーが発生しました。")
            logger.error(traceback.format_exc())
            sys.exit()          
        except Exception as ex:
            logger.error("メインプロセスでマルチスレッドの実行中にエラーが発生しました。{0}".format(ex))
            logger.error(traceback.format_exc())
            sys.exit()

        logger.info("マルチスレッド終了ーThread＝{0}".format(time.time() - multi_time))
        print("マルチスレッド終了ーThread＝{0}".format(time.time() - multi_time))


        # 登録処理
        regist_count , err_flg , err_msg =  pg_dao.insert_sensor_table(logger , items , concat_dict)

        if err_flg == 0:
            logger.info("データ登録が完了しました。 登録件数：{0}件".format(regist_count))
        else:
            logger.error("データ登録に失敗しました。 {0}".format(err_msg))

    except Exception as ex:
        logger.error("メイン処理の実行中にエラーが発生しました。{0}".format(ex))
        logger.error(traceback.format_exc())
    finally:
        logger.info("プロセスが終了しました。")
        logger.info("プロセス終了ー経過時間＝{0}".format(time.time() - start))
        print("プロセス終了ー経過時間＝{0}".format(time.time() - start))



if __name__ == '__main__':
    main()



