import pandas as pd


class DllAction:

    dll_dict = None
    json = None
    machine_df = None
    maker_id = None
    area_id = None
    gw_id = None
    con_string = None
    from_time = None
    to_time = None


    # コンストラクタ
    def __init__(self , r_json , dll_dict , machine_df , maker_id , area_id , gw_id , con_string , from_t , to_t):
        # DLLクラスの辞書
        self.dll_dict = dll_dict
        self.json = r_json
        # MachineマスタのDataFrame
        self.machine_df = machine_df
        self.maker_id = maker_id
        self.area_id = area_id
        self.gw_id = gw_id
        self.con_string = con_string
        self.from_time = from_t
        self.to_time = to_t


    def main(self):

        empty_df = pd.DataFrame()
        err_flg = 0
        err_msg = ""
        ret_df_dict = {} 
        err_kind_list = []

        try:
            for kind, cls in self.dll_dict.items():

                ### getDataTable関数を実行 ###

                try:
                    dll_get_dt = cls.GetDataTable(self.con_string , self.from_time , self.to_time)
                except Exception as ex:
                    err_flg = 2
                    err_kind_list.append(kind)
                    str_con_kind = ','.join(err_kind_list)
                    err_msg = "GetDataTableの実行に失敗しました。 maker:{0} , IP:{1} , kind:".__add__(str_con_kind)
                    print(f"GetDataTableの実行に失敗しました。{ex}")
                    # 初期化して後続の種別の処理まで完了させる
                    dll_get_dt = None

                # DataTableの値がNoneまたは空の場合はスキップする
                if dll_get_dt == None or dll_get_dt.Rows.Count == 0:
                    # 空フレームをセット
                    ret_df_dict[kind] = empty_df
                    continue

                ### C#のDataTableからDataFrameへ変換 ###

                values = []
                numbers = []
                columns = []
                set_list = []
            
                try:
                    # カラム名取得
                    for dataColumn in dll_get_dt.Columns:
                        columns.append(dataColumn.ColumnName)
                    # インデックスと値を取得
                    for i in range(len(dll_get_dt.Rows)):
                        for j in range(len(dll_get_dt.Columns)):
                            set_list.append(dll_get_dt.Rows[i][j])
                        values.insert(len(values), set_list)
                        set_list = []
                        numbers.append(i)
                    # DataFrame作成
                    dll_get_df = pd.DataFrame(data=values, index=numbers, columns=columns)
                except Exception as ex:
                    err_flg = 1
                    err_msg = "DataTableからDataFrameへの変換処理中にエラーが発生しました。 maker:{0} , IP:{1}"
                    print(f"DataTableからDataFrameへの変換処理中にエラーが発生しました。{ex}")
                    return ret_df_dict , err_flg , err_msg
  

                ### 18桁のendDevicenameを生成 ###
                ### DataTableの各値 [PcNo、SensorNo、SensorId、DiagType] と gateway_masterの各値 [maker_id (manufacture_code)、area_id、gw_id] を結合する ###

                device_id_list = []
                try:
                    for pcno, sensorno, sensorid , diagtype in zip(dll_get_df['PcNo'], dll_get_df['SensorNo'], dll_get_df['SensorId'], dll_get_df['DiagType']):
                        end_dvice_id = str(self.maker_id).zfill(2) + str(self.area_id).zfill(2) + str(self.gw_id).zfill(3) + str(pcno).zfill(2)  \
                                    + str(sensorno).zfill(5) + str(sensorid).zfill(2) + str(diagtype).zfill(2)
                        device_id_list.append(end_dvice_id)
                except Exception as ex:
                    err_flg = 1
                    err_msg = "EndDviceIdの生成時にエラーが発生しました。 maker:{0} , IP:{1}"
                    print("EndDviceIdの生成時にエラーが発生しました。")
                    return ret_df_dict , err_flg , err_msg
                

                ### postgresへの登録列を生成 ###
                ### MachineマスタのDataFrameから、endDeviceNameをキーにして、[gw_id , maker_id , model_id] を取得する　###

                regist_in_df = pd.DataFrame()
                try:
                    for i , id in enumerate(device_id_list):
                        all_machine_df = self.machine_df.query('name == @id')
                        if not all_machine_df.empty:
                            # endDeviceName列を生成
                            dll_get_df["end_device_name"] = id
                            # その他id列を生成
                            dll_get_df["gw_id"] = int(all_machine_df['gw_id'])
                            dll_get_df["maker_id"] = int(all_machine_df['maker_id'])
                            dll_get_df["model_id"] = int(all_machine_df['model_id'])                     
                            # 行を追加
                            regist_in_df = regist_in_df.append(dll_get_df.iloc[[i]] , ignore_index=True)
                except Exception as ex:
                    err_flg = 1
                    err_msg = "MachineマスタのDataFrameから登録IDを取得中にエラーが発生しました。 maker:{0} , IP:{1}"
                    print(f"MachineマスタのDataFrameから登録IDを取得中にエラーが発生しました。{ex}")
                    return ret_df_dict , err_flg , err_msg             
                

                # endDviceNameに紐づくMachineマスタのデータが無い場合はスキップする
                if regist_in_df.empty:
                    # 空フレームをセット
                    ret_df_dict[kind] = empty_df
                    continue

                
                ### 欠損チェック ###
                ### DataTableの「DataMissingFlag」をチェックして、DB項目「communication_error」のフラグを立てる ###

                error_check_list = [None] * regist_in_df.shape[0]
                try:
                    for i , check_flag in enumerate(regist_in_df['DataMissingFlag']):
                        if check_flag != 0:
                            error_check_list[i] = '1'
                    error_check = pd.Series(data=error_check_list , name='communication_error')
                except Exception as ex:
                    err_flg = 1
                    err_msg = "欠損フラグのチェック処理中にエラーが発生しました。 maker:{0} , IP:{1}"
                    print("欠損フラグのチェック処理中にエラーが発生しました。")
                    return ret_df_dict , err_flg , err_msg
                

                ### payload1にセットする値を取得 ###
                ### JSONに設定されている値がDataFrameの項目上に存在しないか調べて、在ればその項目の値を使用し、無ければJSONの値をそのまま登録する ###

                payload1_list = [None] * regist_in_df.shape[0]
                try:
                    search_word = self.json[str(self.maker_id)][kind]["payload1"]
                    if search_word in regist_in_df.columns.values:
                        for i , payload_val in enumerate(regist_in_df[search_word]):
                            payload1_list[i] = payload_val
                    else:
                        for i in range(len(payload1_list)):
                            payload1_list[i] = search_word
                    payload1 = pd.Series(data=payload1_list , name='payload1')
                except Exception as ex:
                    err_flg = 1
                    err_msg = "payload1の値取得時にエラーが発生しました。 maker:{0} , IP:{1}"
                    print("payload1の値取得時にエラーが発生しました。")
                    return ret_df_dict , err_flg , err_msg              


                ### DataTableの項目名をDB用の項目名にリネーム ###
                
                try:
                    # JSONのrename_itemsに追加されている項目と値をループする
                    for key , value in self.json[str(self.maker_id)][kind]["rename_items"].items():
                        for col in regist_in_df.columns.values:
                            if col == key:
                                regist_in_df = regist_in_df.rename({col: value} , axis='columns')
                except Exception as ex:
                    err_flg = 1
                    err_msg = "DB用の項目名へリネーム処理中にエラーが発生しました。 maker:{0} , IP:{1}"
                    print("DB用の項目名へリネーム処理中にエラーが発生しました。")
                    return ret_df_dict , err_flg , err_msg
                

                ### 生成した列をDataFrameに追加し、DB項目として必要の無い列を削除する ###

                try:
                    # 登録列の追加
                    merge_df = pd.concat([regist_in_df , error_check , payload1], axis=1)
                    # 登録列のみ取得
                    ret_df = merge_df[self.json[str(self.maker_id)][kind]["regist_items"]]
                except Exception as ex:
                    err_flg = 1
                    err_msg = "登録するカラムを取捨する際にエラーが発生しました。 maker:{0} , IP:{1}"
                    print(f"登録するカラムを取捨する際にエラーが発生しました。{ex}")
                    return ret_df_dict , err_flg , err_msg
            

                # クラスの種別に分けて代入
                ret_df_dict[kind] = ret_df

        except Exception as ex:
            err_flg = 1
            err_msg = "DLLの呼び出し中に想定外のエラーが発生しました。 maker:{0} , IP:{1}"
            print("DLLの呼び出し中に想定外のエラーが発生しました。")
        finally:
            return ret_df_dict , err_flg , err_msg

        


