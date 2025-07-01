from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


import pandas as pd
import numpy as np

import warnings 

warnings.simplefilter("ignore")

import datetime 
import re
import calendar

import os

import uuid
import requests
import json

from dateutil.relativedelta import relativedelta

from functools import lru_cache

from sqlalchemy import create_engine


import os
import re

import json

import pendulum


import modules.api_info

import importlib
importlib.reload(modules.api_info) 

from modules.api_info import var_encrypt_TOKEN_yandex_users, f_decrypt, load_key_external
from modules.api_info import var_encrypt_var_login_da, var_encrypt_var_pass_da
# ____________________________________________________________________________________________

temp_var_TOKEN = f_decrypt(var_encrypt_TOKEN_yandex_users, load_key_external()).decode("utf-8")
login_da = f_decrypt(var_encrypt_var_login_da, load_key_external()).decode("utf-8")
pass_da = f_decrypt(var_encrypt_var_pass_da, load_key_external()).decode("utf-8")


var_link = "https://disk.yandex.ru/i/1hcv17sM3c_U1g"
var_link_active_base = "https://disk.yandex.ru/i/3J0JEqXQFSHF4g"
var_link_active_base_red_merge = "https://disk.yandex.ru/i/3J0JEqXQFSHF4g"
var_link_report_day = "https://disk.yandex.ru/i/cd3vCPYQ-Ym-DQ"
var_link_extension_base = "https://disk.yandex.ru/i/p5jb0dkO49y8TA"
var_link_extension_inn_base = "https://disk.yandex.ru/i/KlVaBLhgy42aAw"


date_now = datetime.datetime.now().date()

local_tz = pendulum.timezone("Europe/Moscow")

default_arguments = {
    'owner': 'evgenijgrinev',
}

with DAG(
    'Garant.Renewal_list_and_ACB',
    # schedule_interval="0 15 1-10 * 1,3,5",
    # schedule_interval="50 12 5-7 * 1",
    schedule_interval="0 15 1-10,15-17,25 * *",    
    
    # schedule_interval="@once",
    catchup=False,
    default_args=default_arguments,
    start_date=pendulum.datetime(2024,7,1, tz=local_tz),
) as dag:


    def algoritm(temp_var_TOKEN=temp_var_TOKEN):
        add_on  = [
        'ГАРАНТ-LegalTech. Малый пакет', 
        'ГАРАНТ-LegalTech. Средний',
        ]
        # _______________________________________
        ext = [
            "Продление", 
            "Восстановление"
        ]
        r_crimea = [
            "Крым", 
        ]
        # _______________________________________
        r_rf = [
            "НР", 
            "РФ",
        ]
        # _______________________________________

        ext = [
            "Продление", 
            "Продление с последующей Реанимацией",
            "Продление c последующим Отпадом",
            "Восстановление",
            "Восстановление с последующей Реанимацией",
            "Восстановление c последующим Отпадом",
        ]
        # _______________________________________
        new = [
            "Новая продажа", 
            "Новая продажа c последующим Отпадом",
            "Новая продажа с последующей Реанимацией"
            ]
        # _______________________________________
        recovery_list = [
            "Восстановление", 
            "Восстановление c последующим Отпадом",
            "Восстановление с последующей Реанимацией",
            ]
        # _______________________________________
        loss = [
            "Отпад", 
            "Реанимация",
            # "Отпад. Реанимация",
            
            "Продление c последующим Отпадом",
            "Продление с последующей Реанимацией",
            
            "Восстановление c последующим Отпадом",
            "Восстановление с последующей Реанимацией",
            
            "Новая продажа c последующим Отпадом",
            "Новая продажа с последующей Реанимацией",
        ]

        loss_acb = [
            "Отпад", 
            # "Реанимация",
            # "Отпад. Реанимация",
            
            "Продление c последующим Отпадом",
            # "Продление с последующей Реанимацией",
            
            "Восстановление c последующим Отпадом",
            # "Восстановление с последующей Реанимацией",
            
            "Новая продажа c последующим Отпадом",
            # "Новая продажа с последующей Реанимацией",
        ]


        rescue_acb = [
            "Реанимация",
            "Отпад. Реанимация",
            
            "Продление с последующей Реанимацией",
            
            "Восстановление с последующей Реанимацией",
            
            "Новая продажа с последующей Реанимацией",
        ]


        @lru_cache
        def add_months(sourcedate, months):
            month = sourcedate.month - 1 + months
            year = sourcedate.year + month // 12
            month = month % 12 + 1
            day = min(sourcedate.day, calendar.monthrange(year,month)[1])
            return datetime.date(year, month, day)

        @lru_cache
        def reduce_months(sourcedate, months):
            month = sourcedate.month - 1 - months
            year = sourcedate.year + month // 12
            month = month % 12 + 1
            day = min(sourcedate.day, calendar.monthrange(year,month)[1])
            return datetime.date(year, month, day)

        @lru_cache
        def add_days(sourcedate, n_days):
            source_date = sourcedate + datetime.timedelta(days=n_days)
            return source_date

        @lru_cache
        def reduce_days(sourcedate, n_days):
            source_date = sourcedate - datetime.timedelta(days=n_days)
            return source_date


        @lru_cache
        def select_region(x):
            if x[:2] in ["91", "92"]:
                var_x = "Крым"
            elif x[:2] in ["90", "93", "94", "95"]:
                var_x = "НР"
            else: 
                var_x = "РФ"
            return var_x



        current_date = datetime.datetime.now().date()
        set_date = current_date
        month_for_acb = reduce_months(set_date, 1)
        name_month_for_acb = month_for_acb.strftime("%B")
        print("Месяц отчета АКБ:", name_month_for_acb)

        # month_for_otpad = reduce_months(set_date, 1) - datetime.timedelta(days=1)
        month_for_otpad = reduce_months(set_date, 1)
        name_month_for_otpad = month_for_otpad.strftime("%B")
        print("Месяц отпада:", name_month_for_otpad)


        sql_query = """
            select * from intermediate_scheme.sbis_coll_sell_upd;
        """

        my_conn = create_engine("postgresql+psycopg2://da:qa123@10.82.2.30:5432/softum")
        try: 
            my_conn.connect()
            print('connect')
            my_conn = my_conn.connect()
            df = pd.read_sql(sql=sql_query, con=my_conn)
            print('success!')
            my_conn.close()
        except:
            print('failed')



        @lru_cache
        def regex_filter(val):
            if val:
                mo = re.search("гарант", val.lower())
                if mo:
                    return True
                else:
                    return False
            else:
                return False

        df_filtered = df[df['Номенклатура.ГАУ'].apply(regex_filter)]



        df_filtered["index"] = 0
        a = 0
        for i in range(len(df_filtered["doc_id"])):    
            df_filtered["index"].iloc[i] = a
            a += 1



        try:
            df_filtered["Дата реализации"] = df_filtered["doc_data_main"].apply(lambda x: datetime.datetime.strptime(x, "%d.%m.%Y").date())
        except Exception as e:
            pass

        df_filtered["Дата с"] = np.nan

        for i in range(len(df_filtered["doc_id"])):
            try:
                df_filtered["Дата с"].iloc[i] = datetime.datetime.strptime(df_filtered["inside_date_from"].iloc[i], "%d.%m.%Y").date()
            except:
                pass




# ______________
        try:
            df_filtered["doc_data_main_origin"] = df_filtered["doc_data_main"].apply(lambda x: datetime.datetime.strptime(x, "%d.%m.%Y").date())
        except Exception as e:
            pass

        df_filtered["Дата реализации"] = np.nan

        for i in range(len(df_filtered["doc_id"])):
            if (type(df_filtered["Дата с"].iloc[i]) == float):
                df_filtered["Дата реализации"].iloc[i] = df_filtered["doc_data_main_origin"].iloc[i]
            else:
                df_filtered["Дата реализации"].iloc[i] = df_filtered["Дата с"].iloc[i]


        # try:
        #     df_filtered["Количество"] = df_filtered["inside_doc_item_quantity"].apply(lambda x: int(x))
        # except Exception as e:
        #     pass
        
    
        df_filtered["Количество"] = np.nan
        for i_d in range(len(df_filtered["inside_doc_item_quantity"])):
            try:
                df_filtered["Количество"].iloc[i_d] = int(df_filtered["inside_doc_item_quantity"].iloc[i_d])
            except:
                df_filtered["Количество"].iloc[i_d] = 1
                

        df_filtered["Дата окончания подписки (по реализациям)"] = np.nan
        df_filtered["Период подписки"] = np.nan

        for i in range(len(df_filtered["doc_id"])):
            q_month = 0
            if "абонемент" in re.findall("абонемент", df_filtered["inside_doc_item_name"].iloc[i].lower()):
                try:
                    q_month = int(re.findall("\d+ мес", df_filtered["inside_doc_item_name"].iloc[i].lower())[0][:2].replace(" ", ""))

                    df_filtered["Период подписки"].iloc[i] = int(q_month)

                    if int(q_month) == 1:
                        
                        # var_date_add_month = add_months(df_filtered["Дата реализации"].iloc[i], q_month)
                        var_date_add_month = df_filtered["Дата реализации"].iloc[i]
                        last_date_of_month = datetime.date(var_date_add_month.year, var_date_add_month.month, 1) + relativedelta(months=1, days=-1)
                        var_date = datetime.datetime(var_date_add_month.year, var_date_add_month.month, last_date_of_month.day)

                        df_filtered["Дата окончания подписки (по реализациям)"].iloc[i] = var_date.date()
                        
                    else:

                        var_date_add_month = add_months(df_filtered["Дата реализации"].iloc[i], int(q_month)-1)
                        last_date_of_month = datetime.date(var_date_add_month.year, var_date_add_month.month, 1) + relativedelta(months=1, days=-1)
                        var_date = datetime.datetime(var_date_add_month.year, var_date_add_month.month, last_date_of_month.day)

                        df_filtered["Дата окончания подписки (по реализациям)"].iloc[i] = var_date.date()

                    
                except:
                    
                    q_month = q_month = df_filtered["Количество"].iloc[i]
                    
                    df_filtered["Период подписки"].iloc[i] = int(q_month)
                    
                    # var_date_add_month = add_months(df_filtered["Дата реализации"].iloc[i], int(q_month))
                    var_date_add_month = df_filtered["Дата реализации"].iloc[i]
                    last_date_of_month = datetime.date(var_date_add_month.year, var_date_add_month.month, 1) + relativedelta(months=1, days=-1)
                    var_date = datetime.datetime(var_date_add_month.year, var_date_add_month.month, last_date_of_month.day)
                    
                    df_filtered["Дата окончания подписки (по реализациям)"].iloc[i] = var_date.date()
            else:
                
                q_month = df_filtered["Количество"].iloc[i]
                df_filtered["Период подписки"].iloc[i] = int(q_month)
                
                if int(q_month) == 1:

                        
                    # var_date_add_month = add_months(df_filtered["Дата реализации"].iloc[i], int(q_month))
                    var_date_add_month = df_filtered["Дата реализации"].iloc[i]
                    last_date_of_month = datetime.date(var_date_add_month.year, var_date_add_month.month, 1) + relativedelta(months=1, days=-1)
                    var_date = datetime.datetime(var_date_add_month.year, var_date_add_month.month, last_date_of_month.day)
                    
                    df_filtered["Дата окончания подписки (по реализациям)"].iloc[i] = var_date.date()

                else:
                    
                    var_date_add_month = add_months(df_filtered["Дата реализации"].iloc[i], int(q_month)-1)
                    last_date_of_month = datetime.date(var_date_add_month.year, var_date_add_month.month, 1) + relativedelta(months=1, days=-1)
                    var_date = datetime.datetime(var_date_add_month.year, var_date_add_month.month, last_date_of_month.day)
                    
                    df_filtered["Дата окончания подписки (по реализациям)"].iloc[i] = var_date.date()


        df_filtered_sort = df_filtered.sort_values([
            "doc_counterparty_inn", 
            # "inside_doc_item_name", 
            "Дата реализации", 
            "Дата окончания подписки (по реализациям)", 
        ], ascending=True)


        df_filtered_sort = df_filtered_sort.drop(columns=["index", "index"])


        df_filtered_sort["index"] = 0
        a = 0
        for i in range(len(df_filtered_sort["doc_id"])):    
            df_filtered_sort["index"].iloc[i] = a
            a += 1



        one_month = 31

        extension = one_month
        recovery = range(one_month+1,one_month*6)
        new_subscribe = one_month*6



        # ____________________________________________________________________________________________________________________
        #  Subcribe algoritm
        # ____________________________________________________________________________________________________________________

        # ________________________________________________________________
        df_filtered_sort["doc_counterparty_inn"] = df_filtered_sort["doc_counterparty_inn"].fillna("")
        df_filtered_sort["doc_counterparty_inn"] = df_filtered_sort["doc_counterparty_inn"].apply(lambda x: str(x))
        df_filtered_sort_without_nan = df_filtered_sort[df_filtered_sort["doc_counterparty_inn"] != ""]

        df_filtered_sort_inn = list((set(df_filtered_sort_without_nan["doc_counterparty_inn"].to_list())))
        # ________________________________________________________________
        df_filtered_sort["doc_id"] = df_filtered_sort["doc_id"].fillna("")
        df_filtered_sort["doc_id"] = df_filtered_sort["doc_id"].apply(lambda x: str(x))
        df_filtered_sort_without_nan_id = df_filtered_sort[df_filtered_sort["doc_id"] != ""]

        df_filtered_sort_id = list((set(df_filtered_sort_without_nan["doc_id"].to_list())))
        # ________________________________________________________________

        df_filtered_sort["Кол-во повторений в БД"] = ""
        df_filtered_sort["Состояние"] = ""

        for i in range(len(df_filtered_sort_inn)):

                filtered_df_red = df_filtered_sort[(df_filtered_sort["doc_counterparty_inn"] == df_filtered_sort_inn[i]) & (~df_filtered_sort["inside_doc_item_name"].isin(add_on))]
                try:
                    df_filtered_sort["Кол-во повторений в БД"].iloc[filtered_df_red["index"].iloc[-1]] = len(filtered_df_red)
                    if len(filtered_df_red) == 1:
            
                        if filtered_df_red["Дата окончания подписки (по реализациям)"].iloc[0] < set_date:
                            df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[0]] = "Новая продажа c последующим Отпадом"
                        else:
                            df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[0]] = "Новая продажа"
                    
                    else:
                        if df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[filtered_df_red["index"].iloc[-1]] < set_date:

                
                            # if filtered_df_red["Дата реализации"].iloc[-1] < set_date:
                                # df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] = "Отпад"
                            # else:
                                # df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] = "Отпад. Реанимация"
                            
                            
                            
                            df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[0]] = "Новая продажа"
                    
                            filtered_df_filtered_sort = ""
                            # if len(filtered_df_red) == 2:
                            #     pass
                            # else:
                                
                            # filtered_df_filtered_sort = filtered_df_red[:-1]
                            filtered_df_filtered_sort = filtered_df_red
                            
                            index_first_record = 0 
                            index_second_record = 1
                            
                            for j in range(len(filtered_df_filtered_sort)):
                                if index_second_record == len(filtered_df_filtered_sort):
                                    pass
                                else:
            
                                    if (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days <= extension:
                                        df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j+1]] = "Продление"
                    
                                    elif (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days > extension:
                                        df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j+1]] = "Восстановление"
                                
                                    index_first_record += 1
                                    index_second_record += 1
            
                            if (set_date - filtered_df_red["Дата окончания подписки (по реализациям)"].iloc[-1]).days >= 1:
                                if  df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] == 'Продление':
                                    df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] = "Продление c последующим Отпадом"
                                elif  df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] == 'Восстановление':
                                    df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] = "Восстановление c последующим Отпадом"
                                elif  df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] == 'Новая продажа':
                                    df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] = "Новая продажа c последующим Отпадом"
                                else:
                                    df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[-1]] = "Отпад"
                                    
                    
                        else:
                            # _________
                            # print("2")
                            # _________
                            df_filtered_sort["Состояние"].iloc[filtered_df_red["index"].iloc[0]] = "Новая продажа"
                            # filtered_df_filtered_sort = filtered_df_red[:-1]
                            filtered_df_filtered_sort = filtered_df_red
                                
                            index_first_record = 0 
                            index_second_record = 1
                            
                            for j in range(len(filtered_df_filtered_sort)):
                                if index_second_record == len(filtered_df_filtered_sort):
                                    pass
                                else:
                                    if (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days <= extension:
                                        df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j+1]] = "Продление"
                                    elif (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days > extension:
                                        df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j+1]] = "Восстановление"
                                    index_first_record += 1
                                    index_second_record += 1
                except:
                    print(df_filtered_sort_inn[i])


        df_filtered_sort_inn = list(set(df_filtered_sort[df_filtered_sort["doc_counterparty_inn"] != ""]["doc_counterparty_inn"].to_list()))

        index_first_record = 0
        index_second_record = 1

        for i in range(len(df_filtered_sort_inn)):
            # print(i)
            index_first_record = 0
            index_second_record = 1
            filtered_df_filtered_sort = ""
            filtered_df_filtered_sort = df_filtered_sort[df_filtered_sort["doc_counterparty_inn"] == df_filtered_sort_inn[i]]

            for j in range(len(filtered_df_filtered_sort["doc_counterparty_inn"])):
                if len(filtered_df_filtered_sort["doc_id"])-1 == j:
                    pass
                else:
                        if (filtered_df_filtered_sort["Состояние"].iloc[index_first_record] == "Продление") and (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days >= 1:
                            df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j]] = "Продление c последующим Отпадом"
                        elif (filtered_df_filtered_sort["Состояние"].iloc[index_first_record] == "Восстановление") and (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days >= 1:
                            df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j]] = "Восстановление c последующим Отпадом"
                        elif (filtered_df_filtered_sort["Состояние"].iloc[index_first_record] == "Новая продажа") and (filtered_df_filtered_sort["Дата реализации"].iloc[index_second_record] - filtered_df_filtered_sort["Дата окончания подписки (по реализациям)"].iloc[index_first_record]).days >= 1:
                            df_filtered_sort["Состояние"].iloc[filtered_df_filtered_sort["index"].iloc[j]] = "Новая продажа c последующим Отпадом"
                        else:
                            pass
                index_first_record += 1
                index_second_record += 1


        # select_region______________________________
        df_filtered_sort["Регион"] = df_filtered_sort["doc_counterparty_inn"].apply(lambda x: select_region(x))
        df_filtered_sort["Стоимость"] = df_filtered_sort["inside_doc_item_full_item_price"].apply(lambda x: float(x))
        df_filtered_sort["Период подписки"] = df_filtered_sort["Период подписки"].apply(lambda x: float(x))
        df_filtered_sort["Цена.Месяц"] = df_filtered_sort.apply(lambda x: x["Стоимость"] / x["Период подписки"], axis=1)
        # ___________________________________________


        print("________________________________________________")
        print(df_filtered_sort["Состояние"].unique())
        print("________________________________________________")



        var_date_add_month = reduce_months(set_date, 1)
        last_date_of_month = datetime.date(var_date_add_month.year, var_date_add_month.month, 1) + relativedelta(months=1, days=-1)

        # print(last_date_of_month)
        # print(last_date_of_month.day)
        # print(reduce_months(set_date,1).year)
        # print(reduce_months(set_date,1).month)




        otpad_current_month = df_filtered_sort[(df_filtered_sort["Состояние"].isin(loss_acb)) & (df_filtered_sort["Дата окончания подписки (по реализациям)"].between(datetime.date(reduce_months(set_date,1).year, reduce_months(set_date,1).month, 1), datetime.date(reduce_months(set_date,1).year, reduce_months(set_date,1).month, last_date_of_month.day)))]
        rescue_current_month = df_filtered_sort[(df_filtered_sort["Состояние"].isin(rescue_acb)) & (df_filtered_sort["Дата окончания подписки (по реализациям)"].between(datetime.date(reduce_months(set_date,1).year, reduce_months(set_date,1).month, 1), datetime.date(reduce_months(set_date,1).year, reduce_months(set_date,1).month, last_date_of_month.day)))]
        print(f"Кол-во отпада за {name_month_for_otpad}:", len(otpad_current_month))
        print(f"Кол-во отпада за {name_month_for_otpad}:", len(rescue_current_month))

        # otpad_current_month.to_excel(f"Отпады {len(otpad_current_month)} шт. за {name_month_for_acb} от {current_date}.xlsx")
        print("________________________________________________")


        df_filtered_sort_for_lists = df_filtered_sort[[
            "doc_id",
            "doc_number",
            "doc_counterparty_inn",
            "doc_counterparty_full_name",
            "doc_assigned_manager",
            "inside_doc_author",
            "inside_doc_item_name",
            "Период подписки",
            "Дата реализации",
            "Дата с",
            "Дата окончания подписки (по реализациям)",
            "Состояние",
            "Стоимость",
            "Цена.Месяц",
        ]]
        
        df_filtered_sort_for_lists_rename = df_filtered_sort_for_lists.rename(columns={
            "doc_id": "id",
            "doc_number": "Номер документа",
            "doc_counterparty_inn": "ИНН",
            "doc_counterparty_full_name": "Клиент",
            "doc_assigned_manager": "Ответственный менеджер",
            "inside_doc_author": "Автор документа",
            "inside_doc_item_name": "Номенклатура",
        })


        full_name_base_for_ext = 'Гарант. Полная база. Для формирования списков'
        df_filtered_sort_for_lists_rename.to_excel(f"{full_name_base_for_ext}.xlsx")
        
        # curr_month = (datetime.date(current_date.year, current_date.month, 1) + relativedelta(months=1, days=-1))
        curr_month = datetime.date(current_date.year, current_date.month, 1)
        end_curr_month = (datetime.date(curr_month.year, curr_month.month, 1) + relativedelta(months=1, days=-1))
        
        next_month = (datetime.date(current_date.year, current_date.month, 1) + relativedelta(months=2, days=-1))
        
        df_filtered_sort_for_extension_name = 'Гарант. Списки на продление'
        df_filtered_sort_for_extension = df_filtered_sort_for_lists_rename[df_filtered_sort_for_lists_rename["Дата окончания подписки (по реализациям)"].between(curr_month, end_curr_month)]
        df_filtered_sort_for_extension.to_excel(f"{df_filtered_sort_for_extension_name}.xlsx")

        df_filtered_sort_for_extension_inn_name = 'Гарант. ИНН для заливки в список'
        df_filtered_sort_for_extension_inn = df_filtered_sort_for_extension.groupby(["ИНН"], as_index=False).agg({
            "ИНН": lambda x: list(set(x))[-1],
            "Клиент": lambda x: list(set(x))[-1],
            "Ответственный менеджер": lambda x: list(set(x))[-1],
            "Автор документа": lambda x: list(set(x))[-1],
            "Дата окончания подписки (по реализациям)": lambda x: list(set(x)),
        })
        df_filtered_sort_for_extension_inn.to_excel(f"{df_filtered_sort_for_extension_inn_name}.xlsx")
      

        # ______________________________________________________________
        # ______________________________________________________________
        # ______________________________________________________________
        # ALGORITM DATA_MART_GARANT_MONTH_TO_MONTH
        # _____________________________________

        range_years = list(range(datetime.datetime.now().year-1, datetime.datetime.now().year+1))
        range_months = list(range(1,13))


        # _____________________________________
        # ACTIVE & LOSS

        df_active_base = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        df_active_base["Период"] = np.nan
        df_active_base["Разбивка"] = np.nan

        df_active_base_sum = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        df_active_base_sum["Период"] = np.nan
        df_active_base_sum["Разбивка"] = np.nan

        df_active_base_nak = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        df_active_base_nak["Период"] = np.nan
        df_active_base_nak["Период.нак"] = np.nan
        df_active_base_nak["Разбивка"] = np.nan

        df_active_base_nak_sum = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        df_active_base_nak_sum["Период"] = np.nan
        df_active_base_nak_sum["Период.нак"] = np.nan
        df_active_base_nak_sum["Разбивка"] = np.nan

        df_loss = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        df_loss["Период"] = np.nan

        temp_df_loss_nak = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        temp_df_loss_nak["Период"] = np.nan
        temp_df_loss_nak["Период.нак"] = np.nan

        df_loss_nak = pd.DataFrame(columns=df_filtered_sort.columns.to_list()) 
        df_loss_nak["Период"] = np.nan
        df_loss_nak["Период.нак"] = np.nan
        df_loss_nak["Разбивка"] = np.nan

        # _____________________________________
        # LIST
        lst_period = []
        lst_values = []
        lst_nak_values = []
        lst_values_sum = []
        lst_nak_values_sum = []
        lst_types = [] 
        # LIST
        # _____________________________________

        for i_year in range_years:

            curr_acb_arr_inn_nak = []
            var_intersection_nak = []
            var_loss_curr_nak = []
            list_recovery_curr_nak = []
            list_new_curr_nak = []

            sum_acb_nak = 0
            var_ext_sum_nak = 0
            var_recovery_sum_nak = 0
            var_new_sum_nak = 0

            for i_month in range_months:
        # _________________________________________________________________________________________________________
        # _________________________________________________________________________________________________________
        # _________________________________________________________________________________________________________
        # QUANTITY_BY_ДАТА_С
                # Introductory_info___________________________________________
                # print(i_year, i_month)
                
                date_start_curr = datetime.date(i_year, i_month, 1)
                lst_day_curr = (date_start_curr + relativedelta(months=1, days=-1)).day
                date_end_curr = datetime.date(i_year, i_month, lst_day_curr)
                
                date_start_prev = datetime.date(i_year, i_month, 1) - relativedelta(months=1)
                lst_day_prev = (date_start_prev + relativedelta(months=1, days=-1)).day
                date_end_prev = datetime.date(date_start_prev.year, date_start_prev.month, lst_day_prev)
                
                # PREV_PERIOD_____________
                prev_preiod_base = df_filtered_sort[(df_filtered_sort["Дата реализации"] < date_start_curr)]
                # ________________________
                # _____________
                # _____________
                curr_acb_fact_q =  df_filtered_sort[(df_filtered_sort["Дата реализации"].between(date_start_curr, date_end_curr)) & (df_filtered_sort["Состояние"] != '')]
                # curr_acb_fact_q_data_origin =  df_filtered_sort[(df_filtered_sort["doc_data_main_origin"].between(date_start_curr, date_end_curr)) & (df_filtered_sort["Состояние"] != '')]
                curr_acb_sub_q =  df_filtered_sort[(df_filtered_sort["Дата реализации"] < date_start_curr) & (df_filtered_sort["Дата окончания подписки (по реализациям)"] >= date_end_curr) & (df_filtered_sort["Состояние"] != '')]
                
                curr_acb = pd.concat([
                    curr_acb_fact_q,
                    curr_acb_sub_q
                ]).sort_values(["doc_counterparty_inn", "Дата реализации"]).drop_duplicates(subset=["doc_counterparty_inn"], keep='last').reset_index(drop=True)
                curr_acb_arr_inn = curr_acb["doc_counterparty_inn"].unique()
                curr_acb_arr_inn_nak.extend(curr_acb_arr_inn)
                curr_acb_arr_inn_nak = list(set(curr_acb_arr_inn_nak))

                
                # sum_acb = curr_acb_fact_q_data_origin["Стоимость"].sum()


                # _____________
                # _____________
                prev_acb_fact_q =  df_filtered_sort[(df_filtered_sort["Дата реализации"].between(date_start_prev, date_end_prev)) & (df_filtered_sort["Состояние"] != '')]
                prev_acb_sub_q =  df_filtered_sort[(df_filtered_sort["Дата реализации"] < date_start_prev) & (df_filtered_sort["Дата окончания подписки (по реализациям)"] >= date_end_prev) & (df_filtered_sort["Состояние"] != '')]
                
                prev_acb = pd.concat([
                    prev_acb_fact_q,
                    prev_acb_sub_q
                ]).sort_values(["doc_counterparty_inn", "Дата реализации"]).drop_duplicates(subset=["doc_counterparty_inn"], keep='last').reset_index(drop=True)
                prev_acb_arr_inn = prev_acb["doc_counterparty_inn"].unique()
                # _____________
                # _____________
                
                var_intersection = set(curr_acb_arr_inn).intersection(set(prev_acb_arr_inn))
                temp_curr_acb = curr_acb[curr_acb["doc_counterparty_inn"].isin(var_intersection)]
                temp_curr_acb["Разбивка"] = 'Продление'
                temp_curr_acb["Период"] = date_start_curr
                df_active_base = pd.concat([
                    df_active_base,
                    temp_curr_acb
                ])
                
                
                var_difference_curr = set(curr_acb_arr_inn).symmetric_difference(set(var_intersection))
                var_loss_curr = set(prev_acb_arr_inn).symmetric_difference(set(var_intersection))

                var_loss_curr_nak.extend(var_loss_curr)
                var_loss_curr_nak = list(set(var_loss_curr_nak))

                prev_preiod_base_inn = prev_preiod_base["doc_counterparty_inn"].unique()
                
                list_recovery_curr = []
                list_new_curr = []
                
                for i_inn in var_difference_curr:
                    if (i_inn in prev_preiod_base_inn) and (i_inn not in prev_acb_arr_inn):
                        list_recovery_curr.append(i_inn)
                    else:
                        list_new_curr.append(i_inn)

                temp_curr_recovery = curr_acb[curr_acb["doc_counterparty_inn"].isin(list_recovery_curr)]
                temp_curr_recovery["Разбивка"] = 'Восстановление'
                temp_curr_recovery["Период"] = date_start_curr
                df_active_base = pd.concat([
                    df_active_base,
                    temp_curr_recovery
                ])

                temp_curr_new = curr_acb[curr_acb["doc_counterparty_inn"].isin(list_new_curr)]
                temp_curr_new["Разбивка"] = 'Новый клиент'
                temp_curr_new["Период"] = date_start_curr
                df_active_base = pd.concat([
                    df_active_base,
                    temp_curr_new
                ]).reset_index(drop=True)

                
                list_recovery_curr_nak.extend(list_recovery_curr)
                list_recovery_curr_nak = list(set(list_recovery_curr_nak))
                list_new_curr_nak.extend(list_new_curr)
                list_new_curr_nak = list(set(list_new_curr_nak))

                # _____________________________________________________
                # nak_ext
                list_ext_intersection_nak = set(curr_acb_arr_inn_nak).symmetric_difference(set(list_recovery_curr_nak + list_new_curr_nak))
                # nak_ext
                # _____________________________________________________
                # nak_loss

                # ACB_NAK______________________________________________
                temp_df_active_base = df_active_base[df_active_base["Период"].between(datetime.date(i_year, 1, 1), date_start_curr)].sort_values(["doc_counterparty_inn", "Дата реализации"]).drop_duplicates(subset=["doc_counterparty_inn"], keep='last')
                temp_df_active_ext = temp_df_active_base[temp_df_active_base["doc_counterparty_inn"].isin(list_ext_intersection_nak)]
                temp_df_active_ext["Разбивка"] = 'Продление'
                temp_df_active_ext["Период.нак"] = date_start_curr
                df_active_base_nak = pd.concat([
                    df_active_base_nak, 
                    temp_df_active_ext
                ])

                
                temp_df_active_rec = temp_df_active_base[temp_df_active_base["doc_counterparty_inn"].isin(list_recovery_curr_nak)]
                temp_df_active_rec["Разбивка"] = 'Восстановление'
                temp_df_active_rec["Период.нак"] = date_start_curr
                df_active_base_nak = pd.concat([
                    df_active_base_nak, 
                    temp_df_active_rec
                ])

                temp_df_active_new = temp_df_active_base[temp_df_active_base["doc_counterparty_inn"].isin(list_new_curr_nak)]
                temp_df_active_new["Разбивка"] = 'Новый клиент'
                temp_df_active_new["Период.нак"] = date_start_curr
                df_active_base_nak = pd.concat([
                    df_active_base_nak, 
                    temp_df_active_new
                ]).reset_index(drop=True)
                
                # ACB_NAK______________________________________________

                l_acb = curr_acb_arr_inn
                l_loss_nak = var_loss_curr_nak
                l_intersection_recovery = set(var_loss_curr_nak).intersection(set(l_acb))
                l_upd_loss_loss = set(l_loss_nak).symmetric_difference(set(l_intersection_recovery))

                
                # temp_df_loss = prev_acb[(prev_acb["doc_counterparty_inn"].isin(l_upd_loss_loss)) & (prev_acb["Состояние"].isin(lst_q_loss))]
                temp_df_loss = prev_acb[(prev_acb["doc_counterparty_inn"].isin(l_upd_loss_loss))].sort_values(["doc_counterparty_inn", "Дата реализации"]).drop_duplicates(subset=["doc_counterparty_inn"], keep='last')
                temp_df_loss["Период"] = date_start_curr
                df_loss = pd.concat([
                    df_loss,
                    temp_df_loss
                ]).reset_index(drop=True)

                sum_loss_curr = temp_df_loss["Цена.Месяц"].sum() / 1000

                temp_nak_df_loss = df_loss[(df_loss["Период"].between(datetime.date(i_year, 1, 1), date_start_curr)) & (df_loss["doc_counterparty_inn"].isin(l_upd_loss_loss))].sort_values(["doc_counterparty_inn", "Дата реализации"]).drop_duplicates(subset=["doc_counterparty_inn"], keep='last')
                temp_nak_df_loss["Период.нак"] = date_start_curr
                df_loss_nak = pd.concat([
                    df_loss_nak,
                    temp_nak_df_loss
                ]).reset_index(drop=True)


                # print('l_upd_loss_loss:', l_upd_loss_loss)
                # print('\n')
                # print('temp_nak_df_loss:', temp_nak_df_loss["doc_counterparty_inn"].to_list())
                # print('_____________')
                # print('len(l_upd_loss_loss):', len(l_upd_loss_loss))
                # print('len(temp_nak_df_loss):', len(temp_nak_df_loss))
                
                
                # for i_inn in curr_inn_temp:
                #     if i_inn in temp_lst_nak_loss:
                #         index_temp_loss = temp_lst_nak_loss.index(i_inn)
                #         temp_lst_nak_loss.pop(index_temp_loss)
                
                loss_nak = df_loss_nak[(df_loss_nak["Период.нак"] == date_start_curr)]
                sum_loss_nak = loss_nak["Цена.Месяц"].sum() / 1000
                
        # _________________________________________________________________________________________________________
        # _________________________________________________________________________________________________________
        # _________________________________________________________________________________________________________
        # SUM_BY_MAIN_DATE_________________________________________________________________________________________

                    
                # PREV_PERIOD_____________
                prev_preiod_base_sum = df_filtered_sort[(df_filtered_sort["doc_data_main_origin"] < date_start_curr)]
                # ________________________
                # curr_acb_fact_sum =  df_filtered_sort[(df_filtered_sort["doc_data_main_origin"].between(date_start_curr, date_end_curr)) & (df_filtered_sort["Состояние"] != '')]
                curr_acb_fact_sum =  df_filtered_sort[(df_filtered_sort["doc_data_main_origin"].between(date_start_curr, date_end_curr))]
                curr_acb_fact_sum_inn = curr_acb_fact_sum["doc_counterparty_inn"].unique()
                sum_acb_curr = curr_acb_fact_sum["Стоимость"].sum() / 1000
                sum_acb_nak += sum_acb_curr
                # _____________
                # prev_acb_fact_sum =  df_filtered_sort[(df_filtered_sort["doc_data_main_origin"].between(date_start_prev, date_end_prev)) & (df_filtered_sort["Состояние"] != '')]
                prev_acb_fact_sum =  df_filtered_sort[(df_filtered_sort["doc_data_main_origin"].between(date_start_prev, date_end_prev))]
                prev_acb_fact_sum_inn = prev_acb_fact_sum["doc_counterparty_inn"].unique()
                # _____________
                
                var_intersection_sum = set(curr_acb_fact_sum_inn).intersection(set(prev_acb_fact_sum_inn))

                temp_curr_acb_sum = curr_acb_fact_sum[curr_acb_fact_sum["doc_counterparty_inn"].isin(var_intersection_sum)]
                temp_curr_acb_sum["Разбивка"] = 'Продление'
                temp_curr_acb_sum["Период"] = date_start_curr
                df_active_base_sum = pd.concat([
                    df_active_base_sum,
                    temp_curr_acb_sum
                ])

                var_ext_sum = temp_curr_acb_sum["Стоимость"].sum() / 1000
                var_ext_sum_nak += var_ext_sum
                
                var_difference_curr_sum = set(curr_acb_fact_sum_inn).symmetric_difference(set(var_intersection_sum))
                var_loss_curr_sum = set(prev_acb_fact_sum_inn).symmetric_difference(set(var_intersection_sum))

                # var_loss_curr_nak_sum += var_loss_curr_sum
                # var_loss_curr_nak = list(set(var_loss_curr_nak))

                prev_preiod_base_inn_sum = prev_preiod_base_sum["doc_counterparty_inn"].unique()
                
                list_recovery_curr_sum = []
                list_new_curr_sum = []
                
                for i_inn_sum in var_difference_curr_sum:
                    if (i_inn_sum in prev_preiod_base_inn_sum) and (i_inn_sum not in prev_acb_fact_sum_inn):
                        list_recovery_curr_sum.append(i_inn_sum)
                    else:
                        list_new_curr_sum.append(i_inn_sum)

                temp_curr_recovery_sum = curr_acb_fact_sum[curr_acb_fact_sum["doc_counterparty_inn"].isin(list_recovery_curr_sum)]
                temp_curr_recovery_sum["Разбивка"] = 'Восстановление'
                temp_curr_recovery_sum["Период"] = date_start_curr
                df_active_base_sum = pd.concat([
                    df_active_base_sum,
                    temp_curr_recovery_sum
                ])

                var_recovery_sum = temp_curr_recovery_sum["Стоимость"].sum() / 1000
                var_recovery_sum_nak += var_recovery_sum

                temp_curr_new_sum = curr_acb_fact_sum[curr_acb_fact_sum["doc_counterparty_inn"].isin(list_new_curr_sum)]
                temp_curr_new_sum["Разбивка"] = 'Новый клиент'
                temp_curr_new_sum["Период"] = date_start_curr
                df_active_base_sum = pd.concat([
                    df_active_base_sum,
                    temp_curr_new_sum
                ]).reset_index(drop=True)

                var_new_sum = temp_curr_new_sum["Стоимость"].sum() / 1000
                var_new_sum_nak += var_new_sum

                
                # list_recovery_curr_nak.extend(list_recovery_curr)
                # list_recovery_curr_nak = list(set(list_recovery_curr_nak))
                # list_new_curr_nak.extend(list_new_curr)
                # list_new_curr_nak = list(set(list_new_curr_nak))

                

                # print('____________________________________________')
                # _________________________________________________
                # APPEND
                lst_period.append(datetime.date(i_year, i_month, 1))
                lst_values.append(len(curr_acb_arr_inn))
                lst_nak_values.append(len(curr_acb_arr_inn_nak))
                lst_values_sum.append(sum_acb_curr)
                lst_nak_values_sum.append(sum_acb_nak)       
                lst_types.append('АКБ')    
                
                lst_period.append(datetime.date(i_year, i_month, 1))
                lst_values.append(len(var_intersection))
                lst_nak_values.append(len(list_ext_intersection_nak))
                lst_values_sum.append(var_ext_sum)
                lst_nak_values_sum.append(var_ext_sum_nak)
                lst_types.append('Продление')    
                
                lst_period.append(datetime.date(i_year, i_month, 1))
                lst_values.append(len(list_new_curr))
                lst_nak_values.append(len(list_new_curr_nak))
                lst_values_sum.append(var_new_sum)
                lst_nak_values_sum.append(var_new_sum_nak)
                lst_types.append('Новый клиент')    
                
                lst_period.append(datetime.date(i_year, i_month, 1))
                lst_values.append(len(list_recovery_curr))
                lst_nak_values.append(len(list_recovery_curr_nak))
                lst_values_sum.append(var_recovery_sum)
                lst_nak_values_sum.append(var_recovery_sum_nak)
                lst_types.append('Восстановление')    
                
                lst_period.append(datetime.date(i_year, i_month, 1))
                lst_values.append(-len(var_loss_curr))
                lst_nak_values.append(-len(l_upd_loss_loss))
                lst_values_sum.append(-sum_loss_curr)
                lst_nak_values_sum.append(-sum_loss_nak)
                lst_types.append('Отпад')     
                # APPEND
                # _________________________________________________



        df_data_mart_acb = pd.DataFrame(columns=[
            "Период",
            "Показатели",
            "Накопительно",
            "Показатели.sum",
            "Накопительно.sum",
            "Тип",
        ], data=zip(
            lst_period,
            lst_values,
            lst_nak_values,
            lst_values_sum,
            lst_nak_values_sum,
            lst_types,
        ))

        # df_data_mart_acb["sort_index"] = df_data_mart_acb["Тип"].apply(lambda x: select_index(x))
        # df_data_mart_acb


        # _____________________________________________________
        # _____________________________________________________
        # _____________________________________________________
        df_loss["Разбивка"] = 'Отпад'
        df_curr_prev_base = pd.concat([df_active_base, df_loss])
        col = [
            "doc_id",
            "doc_number",
            "doc_data_main_origin",
            "doc_counterparty_inn",
            "doc_counterparty_full_name",
            "doc_department",
            "doc_assigned_manager",
            "inside_doc_author",
            "inside_doc_item_name",
            "inside_doc_item_note",
            "Дата реализации",
            "Период подписки",
            "Дата окончания подписки (по реализациям)",
            "Период",
            "Разбивка",
            "Регион",
            "Стоимость",
            "Цена.Месяц",
            "Состояние",
            "Кол-во повторений в БД",
        ]

        df_active_base_red = df_curr_prev_base[col]
        
        df_active_base_red = df_active_base_red.rename(columns={
        "doc_number": "Номер реализации",
        "doc_data_main_origin": "дата документа (реализации)",
        "doc_counterparty_inn": "ИНН",
        "doc_counterparty_full_name": "Клиент",
        "doc_department": "Подразделение",
        "doc_assigned_manager": "Ответственный менеджер",
        "inside_doc_author": "Автор документа",
        "inside_doc_item_name": "Номенклатура",
        "inside_doc_item_note": "Комментарий автора документа",
        "Состояние": "Состояние (инф. для аналитика)",
        })
            
        # ______________________________________________________________
        # ______________________________________________________________
        # ______________________________________________________________
        
        
        # ______________________________________________________________
        # ______________________________________________________________
        # ______________________________________________________________
               
                
        # report_month = datetime.datetime.now().date() + relativedelta(months=-1)
        report_month = datetime.datetime.now().date()
        print('\n')
        print("report_month:", report_month)
        print('\n')

        arr_acb_curr_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month.year,report_month.month,1)) & (df_curr_prev_base["Разбивка"] != 'Отпад')]
        arr_new_curr_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month.year,report_month.month,1)) & (df_curr_prev_base["Разбивка"] == 'Новый клиент')]
        arr_rec_curr_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month.year,report_month.month,1)) & (df_curr_prev_base["Разбивка"] == 'Восстановление')]

        report_month_prev = datetime.datetime.now().date() - relativedelta(months=1)
        print('\n')
        print("report_month_prev:", report_month_prev)
        print('\n')
        
        arr_acb_prev_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)) & (df_curr_prev_base["Разбивка"] != 'Отпад')]
        arr_new_prev_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)) & (df_curr_prev_base["Разбивка"] == 'Новый клиент')]
        arr_rec_prev_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)) & (df_curr_prev_base["Разбивка"] == 'Восстановление')]

        arr_extension_curr_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month.year,report_month.month,1)) & (df_curr_prev_base["Разбивка"] == 'Продление')]
        arr_loss_curr_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month.year,report_month.month,1)) & (df_curr_prev_base["Разбивка"] == 'Отпад')]
        arr_loss_curr_month_without_1 = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month.year,report_month.month,1)) & (df_curr_prev_base["Разбивка"] == 'Отпад') & (df_curr_prev_base["Период подписки"] != 1)]

        arr_extension_prev_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)) & (df_curr_prev_base["Разбивка"] == 'Продление')]
        arr_loss_prev_month = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)) & (df_curr_prev_base["Разбивка"] == 'Отпад')]
        arr_loss_prev_month_without_1 = df_curr_prev_base[(df_curr_prev_base["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)) & (df_curr_prev_base["Разбивка"] == 'Отпад') & (df_curr_prev_base["Период подписки"] != 1)]

        # arr_extension_curr_month
        # arr_loss_curr_month
        percent_of_compl = len(arr_loss_curr_month) / (len(arr_extension_curr_month) + len(arr_loss_curr_month))
        percent_of_compl_without_1 = len(arr_loss_curr_month_without_1) / (len(arr_extension_curr_month) + len(arr_loss_curr_month_without_1))


        percent_of_compl_prev = len(arr_loss_prev_month) / (len(arr_extension_prev_month) + len(arr_loss_prev_month))
        percent_of_compl_without_1_prev = len(arr_loss_prev_month_without_1) / (len(arr_extension_prev_month) + len(arr_loss_prev_month_without_1))

        # print(f"Процент отпада: -{percent_of_compl:.0%}")
        
        
        q_arr_acb_curr_month = len(arr_acb_curr_month)
        q_arr_new_curr_month = len(arr_new_curr_month)
        q_arr_rec_curr_month = len(arr_rec_curr_month)
        
        q_arr_extension_curr_month = len(arr_extension_curr_month)
        q_arr_loss_curr_month = len(arr_loss_curr_month)
        q_arr_loss_curr_month_without_1 = len(arr_loss_curr_month_without_1)
        q_percent_of_compl = percent_of_compl
        q_percent_of_compl_without_1 = percent_of_compl_without_1
        
        # q_arr_extension_curr_month
        # q_arr_loss_curr_month
        # q_percent_of_compl        
        
        q_arr_acb_prev_month = len(arr_acb_prev_month)
        q_arr_new_prev_month = len(arr_new_prev_month)
        q_arr_rec_prev_month = len(arr_rec_prev_month)
        
        q_arr_extension_prev_month = len(arr_extension_prev_month)
        q_arr_loss_prev_month = len(arr_loss_prev_month)
        q_arr_loss_prev_month_without_1 = len(arr_loss_prev_month_without_1)
        q_percent_of_compl_prev = percent_of_compl_prev
        q_percent_of_compl_without_1_prev = percent_of_compl_without_1_prev
        
        def select_name_month(x):
            if x == 1:
                n = 'Январь'
            elif x == 2:
                n = 'Февраль'
            elif x == 3:
                n = 'Март'
            elif x == 4:
                n = 'Апрель'
            elif x == 5:
                n = 'Май'
            elif x == 6:
                n = 'Июнь'
            elif x == 7:
                n = 'Июль'
            elif x == 8:
                n = 'Август'
            elif x == 9:
                n = 'Сентябрь'
            elif x == 10:
                n = 'Октябрь'
            elif x == 11:
                n = 'Ноябрь'
            elif x == 12:
                n = 'Декабрь'
            return n
        
        
        df_active_base_red_fil = df_active_base_red[df_active_base_red["Период"] == datetime.date(report_month_prev.year,report_month_prev.month,1)]
        
        full_name_base_for_active_base_report_month = f'Гарант. Активная база за отчетный месяц'
        df_active_base_red_fil.to_excel(f"{full_name_base_for_active_base_report_month}.xlsx")

        full_name_base_for_active_base = f'Гарант. Активная база (за весь период)'
        df_active_base_red.to_excel(f"{full_name_base_for_active_base}.xlsx")

# _________________________________________________________________________________________________________________________________

        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        temp_report_date_now = datetime.datetime.now().date()
        temp_report_date_for_kpi = datetime.datetime.now().date() - relativedelta(months=1)
        
        if len(str(temp_report_date_now.month)) == 1:
            var_month = '0' + str(temp_report_date_now.month)
        else:
            var_month = str(temp_report_date_now.month)
            
        report_date_for_kpi = datetime.date(temp_report_date_for_kpi.year, temp_report_date_for_kpi.month, 1)
        last_date_report_date_for_kpi = report_date_for_kpi + relativedelta(months=1,days=-1)
        print('report_date_for_kpi last_date_report_date_for_kpi:',report_date_for_kpi, last_date_report_date_for_kpi)
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        
        try:
            df_active_base_red["Период"] = df_active_base_red["Период"].apply(lambda x: x.date())
        except:
            pass
        try:
            df_active_base_red["ИНН"] = df_active_base_red["ИНН"].apply(lambda x: str(int(x)))\
        except:
            pass
        try: 
            df_active_base_red["Дата реализации"] = df_active_base_red["Дата реализации"].apply(lambda x: x.date())
        except:
            pass
        try:
            df_active_base_red["Дата окончания подписки (по реализациям)"] = df_active_base_red["Дата окончания подписки (по реализациям)"].apply(lambda x: x.date())
        except:
            pass
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        
        df_active_base_red_for_plan = df_active_base_red[(df_active_base_red["Период"] == report_date_for_kpi) & (df_active_base_red["Дата окончания подписки (по реализациям)"] == last_date_report_date_for_kpi)]
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        # dir_path = f'Z:/garant/kpi_by_list/{temp_report_date_for_kpi.year}-{var_month}/'
        dir_path = f'Z:/garant/kpi_by_list/{temp_report_date_for_kpi.year}-{var_month}/'
        
        list_files = os.listdir(dir_path)
        
        df_active_base_red_template_concat = pd.read_excel(dir_path + list_files[0])
        df_active_base_red_concat = pd.DataFrame(columns=df_active_base_red_template_concat.columns.to_list())
        df_active_base_red_concat["МС (по спискам)"] = np.nan
        
        for i_file in list_files:
            print(i_file)
            
            temp_file = pd.read_excel(dir_path + i_file)
            var_ms_name = i_file.replace('.xlsx', '').split(' ')[-1]
            temp_file["МС (по спискам)"] = var_ms_name
            
            df_active_base_red_concat = pd.concat([
                df_active_base_red_concat,
                temp_file
            ]).reset_index(drop=True)
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        
        df_active_base_red_concat_red = df_active_base_red_concat[[
         'Название (ФИО)',
         'ИНН',
         'Ответственный',
         'Подразделение(ответственного)',
         'МС (по спискам)',
        ]]
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        df_active_base_red_concat_red["ИНН"] = df_active_base_red_concat_red["ИНН"].apply(lambda x: str(int(x)))
        df_active_base_red_for_plan["ИНН"] = df_active_base_red_for_plan["ИНН"].apply(lambda x: str(int(x)))
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        df_active_base_red_for_plan_red = df_active_base_red_for_plan[[
         'Номер реализации',
         'дата документа (реализации)',
         'ИНН',
         'Клиент',
         'Подразделение',
         'Ответственный менеджер',
         'Автор документа',
         'Дата реализации',
         'Период подписки',
         'Дата окончания подписки (по реализациям)',
         'Период',
         # 'Разбивка',
         'Стоимость',
         'Цена.Месяц',
        ]].reset_index(drop=True)
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        var_plan_ext_price = round(df_active_base_red_for_plan_red["Цена.Месяц"].sum(),2)
        var_plan_ext_cost = round(df_active_base_red_for_plan_red["Стоимость"].sum(),2)
        print('var_plan_ext_price:', var_plan_ext_price)
        print('var_plan_ext_cost:', var_plan_ext_cost)
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        df_active_base_red_merge = df_active_base_red_for_plan_red.merge(df_active_base_red_concat_red, how='left', left_on=['ИНН'], right_on=['ИНН'])
        df_active_base_red_merge["Статус"] = np.nan
        df_active_base_red_merge["Детализация по статусу"] = np.nan
        df_active_base_red_merge["Продлено.Цена.Месяц"] = np.nan
        df_active_base_red_merge["Продлено.Стоимость"] = np.nan
        
        df_active_base_red_red = df_active_base_red[(df_active_base_red["Период"] > report_date_for_kpi)][[
         'Номер реализации',
         'ИНН',
         'Дата реализации',
         'Период подписки',
         'Дата окончания подписки (по реализациям)',
         'Период',
         'Разбивка',
         'Стоимость',
         'Цена.Месяц',
        ]].sort_values(["ИНН"]).reset_index(drop=True)
        
        for i_index_inn in range(len(df_active_base_red_merge["ИНН"])):
            var_temp_inn = df_active_base_red_merge["ИНН"].iloc[i_index_inn]
            temp_arr_status = df_active_base_red_red[df_active_base_red_red["ИНН"] == var_temp_inn]["Разбивка"].to_list()
            temp_arr = df_active_base_red_red[df_active_base_red_red["ИНН"] == var_temp_inn]
        
        
            temp_list_index = []
            
            if 'Продление' in temp_arr_status:
                # df_active_base_red_merge["Статус"].iloc[i_index_inn] = 'Продление'
                temp_list_index.append(temp_arr_status.index('Продление'))
            elif 'Восстановление' in temp_arr_status:
                # df_active_base_red_merge["Статус"].iloc[i_index_inn] = 'Восстановление'
                temp_list_index.append(temp_arr_status.index('Продление'))
            elif 'Новый клиент' in temp_arr_status:
                # df_active_base_red_merge["Статус"].iloc[i_index_inn] = 'Новый клиент'
                temp_list_index.append(temp_arr_status.index('Продление'))
        
            if len(temp_list_index) != 0:
                min_index = min(temp_list_index)
                df_active_base_red_merge["Статус"].iloc[i_index_inn] = temp_arr_status[int(min_index)]
        
                temp_dict = {}
                temp_dict["Номер реализации"] = temp_arr["Номер реализации"].iloc[int(min_index)]
                temp_dict["Период"] = temp_arr["Период"].iloc[int(min_index)]
                temp_dict["Дата реализации"] = temp_arr["Дата реализации"].iloc[int(min_index)]
                temp_dict["Период подписки"] = temp_arr["Период подписки"].iloc[int(min_index)]
                temp_dict["Дата окончания подписки (по реализациям)"] = temp_arr["Дата окончания подписки (по реализациям)"].iloc[int(min_index)]
                temp_dict["Стоимость"] = temp_arr["Стоимость"].iloc[int(min_index)]
                temp_dict["Цена.Месяц"] = temp_arr["Цена.Месяц"].iloc[int(min_index)]
                
                df_active_base_red_merge["Детализация по статусу"].iloc[i_index_inn] = str(temp_dict)
                df_active_base_red_merge["Продлено.Цена.Месяц"].iloc[i_index_inn] = temp_arr["Цена.Месяц"].iloc[int(min_index)]
                df_active_base_red_merge["Продлено.Стоимость"].iloc[i_index_inn] = temp_arr["Стоимость"].iloc[int(min_index)]
            else:
                df_active_base_red_merge["Статус"].iloc[i_index_inn] = 'Отпад'
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________
        var_ext_success_price = round(df_active_base_red_merge[~df_active_base_red_merge["Детализация по статусу"].isna()]["Продлено.Цена.Месяц"].sum(),2)
        var_ext_success_cost = round(df_active_base_red_merge[~df_active_base_red_merge["Детализация по статусу"].isna()]["Продлено.Стоимость"].sum(),2)
        
        print('var_ext_success_price:', var_ext_success_price)
        print('var_ext_success_cost:', var_ext_success_cost)
        # ___________________________________________________
        # ___________________________________________________
        # ___________________________________________________

        full_name_base_df_active_base_red = f'Гарант. База рассчетов плановых показателей (на отчетный период)'
        df_active_base_red_merge.to_excel(f"{full_name_base_df_active_base_red}.xlsx")

        # _________________________________________________________________________________________________________________________________
        var_TOKEN = temp_var_TOKEN

        date_now = datetime.datetime.now().date()

        local_tz = pendulum.timezone("Europe/Moscow")



        URL = 'https://cloud-api.yandex.net/v1/disk/resources'
        TOKEN = str(var_TOKEN)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'}

        path_for_upl_file = 'Гарант/Продление лицензий'

        def create_folder(path):
            """Создание папки. \n path: Путь к создаваемой папке."""
            a = requests.put(f'{URL}?path=%2F{path}', headers=headers)
            print("mkdir:", a.status_code)


        # create_folder('UBSERVER/Гарант')
        # create_folder('UBSERVER/Гарант/Продление лицензий')

        def upload_file(loadfile, savefile, replace=False):
            """Загрузка файла.
            savefile: Путь к файлу на Диске
            loadfile: Путь к загружаемому файлу
            replace: true or false Замена файла на Диске"""
            res = requests.get(f'{URL}/upload?path={savefile}&overwrite={replace}', headers=headers).json()
            with open(loadfile, 'rb') as f:
                try:
                    a = requests.put(res['href'], files={'file':f})
                    print("upload:", a.status_code)
                except KeyError:
                    print(res)
        try:
            upload_file(f'./{full_name_base_for_ext}.xlsx', f'{path_for_upl_file}/{full_name_base_for_ext}.xlsx', True)
        except:
            print(f'{full_name_base_for_ext} uploading failed.')
        try:
            upload_file(f'./{full_name_base_for_active_base}.xlsx', f'{path_for_upl_file}/{full_name_base_for_active_base}.xlsx', True)
        except:
            print(f'{full_name_base_for_active_base} uploading failed.')
        try:
            upload_file(f'./{df_filtered_sort_for_extension_name}.xlsx', f'{path_for_upl_file}/{df_filtered_sort_for_extension_name}.xlsx', True)
        except:
            print(f'{df_filtered_sort_for_extension_name} uploading failed.')
        try:
            upload_file(f'./{df_filtered_sort_for_extension_inn_name}.xlsx', f'{path_for_upl_file}/{df_filtered_sort_for_extension_inn_name}.xlsx', True)
        except:
            print(f'{df_filtered_sort_for_extension_inn_name} uploading failed.')
        try:
            upload_file(f'./{full_name_base_df_active_base_red}.xlsx', f'{path_for_upl_file}/{full_name_base_df_active_base_red}.xlsx', True)
        except:
            print(f'{full_name_base_df_active_base_red} uploading failed.')
        try:
            upload_file(f'./{full_name_base_for_active_base_report_month}.xlsx', f'{path_for_upl_file}/{full_name_base_for_active_base_report_month}.xlsx', True)
        except:
            print(f'{full_name_base_for_active_base_report_month} uploading failed.')            

        print(q_arr_extension_curr_month, q_arr_loss_curr_month, q_percent_of_compl)
        print('CURR_______________________________________')
        print(q_arr_extension_curr_month, q_arr_loss_curr_month, q_percent_of_compl, q_arr_loss_curr_month_without_1, q_percent_of_compl_without_1, q_arr_acb_curr_month, q_arr_new_curr_month, q_arr_rec_curr_month,)
        print('PREV_______________________________________')
        print(q_arr_extension_prev_month, q_arr_loss_prev_month, q_percent_of_compl_prev, q_arr_loss_prev_month_without_1, q_percent_of_compl_without_1_prev, q_arr_acb_prev_month, q_arr_new_prev_month, q_arr_rec_prev_month,)
        print('___________________________________________')

        return q_arr_extension_curr_month, q_arr_loss_curr_month, q_percent_of_compl, q_arr_loss_curr_month_without_1, q_percent_of_compl_without_1, q_arr_acb_curr_month, q_arr_new_curr_month, q_arr_rec_curr_month, q_arr_extension_prev_month, q_arr_loss_prev_month, q_percent_of_compl_prev, q_arr_loss_prev_month_without_1, q_percent_of_compl_without_1_prev, q_arr_acb_prev_month, q_arr_new_prev_month, q_arr_rec_prev_month, var_plan_ext_price, var_plan_ext_cost, var_ext_success_price, var_ext_success_cost

    def send_message():
        
        
        global q_arr_extension_curr_month, q_arr_loss_curr_month, q_percent_of_compl, q_arr_loss_curr_month_without_1, q_percent_of_compl_without_1, q_arr_acb_curr_month, q_arr_new_curr_month, q_arr_rec_curr_month, q_arr_extension_prev_month, q_arr_loss_prev_month, q_percent_of_compl_prev, q_arr_loss_prev_month_without_1, q_percent_of_compl_without_1_prev, q_arr_acb_prev_month, q_arr_new_prev_month, q_arr_rec_prev_month, var_plan_ext_price, var_plan_ext_cost, var_ext_success_price, var_ext_success_cost
        q_arr_extension_curr_month, q_arr_loss_curr_month, q_percent_of_compl, q_arr_loss_curr_month_without_1, q_percent_of_compl_without_1, q_arr_acb_curr_month, q_arr_new_curr_month, q_arr_rec_curr_month, q_arr_extension_prev_month, q_arr_loss_prev_month, q_percent_of_compl_prev, q_arr_loss_prev_month_without_1, q_percent_of_compl_without_1_prev, q_arr_acb_prev_month, q_arr_new_prev_month, q_arr_rec_prev_month, var_plan_ext_price, var_plan_ext_cost, var_ext_success_price, var_ext_success_cost = algoritm()

        
        var_login_da = str(login_da)
        var_pass_da = str(pass_da)
        

        # myuuid_garant = str(uuid.uuid4())
        myuuid_garant = "1e0dd0db-f38b-4fc2-9145-0f2951c86396"
        print('Your UUID is: ' + str(myuuid_garant))
        # _________________________________

        url = "https://online.sbis.ru/auth/service/" 


        method = "СБИС.Аутентифицировать"

        params = {
            "Параметр": {
                "Логин": f"{var_login_da}",
                "Пароль": f"{var_pass_da}",
            }

        }
        parameters = {
        "jsonrpc": "2.0",
        "method": method,
        "params":params,
        "id": 0
        }
            
        response = requests.post(url, json=parameters)
        response.encoding = 'utf-8'


        str_to_dict = json.loads(response.text)
        print(str_to_dict)
        access_token = str_to_dict["result"]
        # print("access_token:", access_token)

        headers = {
        "X-SBISSessionID": access_token,
        "Content-Type": "application/json",
        }  

        # _____________________________________________________________


        
        def select_name_month(x):
            if x == 1:
                n = 'Январь'
            elif x == 2:
                n = 'Февраль'
            elif x == 3:
                n = 'Март'
            elif x == 4:
                n = 'Апрель'
            elif x == 5:
                n = 'Май'
            elif x == 6:
                n = 'Июнь'
            elif x == 7:
                n = 'Июль'
            elif x == 8:
                n = 'Август'
            elif x == 9:
                n = 'Сентябрь'
            elif x == 10:
                n = 'Октябрь'
            elif x == 11:
                n = 'Ноябрь'
            elif x == 12:
                n = 'Декабрь'
            return n
        
        var_month = (datetime.datetime.now().date() + relativedelta(months=1)).month
        var_name_month = select_name_month(var_month)
        
        var_report_month = datetime.datetime.now().date().month
        var_name_report_month = select_name_month(var_report_month)
        
        var_report_month_prev = (datetime.datetime.now().date() - relativedelta(months=1)).month
        var_name_report_month_prev = select_name_month(var_report_month_prev)




        if date_now.day in list(range(1,6)):
            
            parameters_real = {

            "jsonrpc": "2.0",
            "protocol": 6,
            "method": "PublicMsgApi.MessageSend",
            "params": {
                "dialogID": myuuid_garant,
                "messageID": None,
                "answer": None,
                "text": f"""
                Гарант. За {var_name_report_month_prev} АКБ составил {q_arr_acb_prev_month}, продлено клиентов {q_arr_extension_prev_month}, восстановленных клиентов {q_arr_rec_prev_month}, новых клиентов {q_arr_new_prev_month} и отпало {q_arr_loss_prev_month} клиентов, процент отпада составил {q_percent_of_compl_prev:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
                Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_prev_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1_prev:.0%}. \n
                
                Гарант. Активная база за отчетный месяц {var_name_report_month_prev}
                {var_link_report_day} \n
                Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n

                План по продлению составил: {var_plan_ext_price} (по Цена.Месяц), {var_plan_ext_cost} (по Стоимость), 
                Уже продлено: {var_ext_success_price} (по Цена.Месяц), {var_ext_success_cost} (по Стоимость). \n
                
                Детализация по плану продления от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n
                
                """, 
                
                "document": None,
                "files": None,
                "recipients": [
                # link url sbis
                # DA
                "c943a420-f494-4a38-8975-d9db61c3dba7",
                # rop garant
                "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
                # om
                # "88619719-b479-46aa-84c4-80ca85d019c1", 
                # "300aa502-0455-4684-bfed-0c337f831fd6",
                # ms
                # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
                # "656712d1-afce-4296-af20-71076b5337fe", 
                # "01c7b684-a807-488e-bde4-ea40e7ef54ae",
                # hr
                # 'edeefe05-a0da-4f43-b978-56b79b78be59',
                # rop back office
                # "dde503d0-53c2-4afd-a3ed-62030a31981d",
                ],
                "options": {
                    "d": [
                        "Гарант. Показатели АКБ и списки продления на следующий месяц.",
                        0,
                        {}
                    ],
                    "s": [
                        {
                        "t": "Строка",
                        "n": "Title"
                        },
                        {
                        "t": "Число целое",
                        "n": "TextFormat"
                        },
                        {
                        "t": "JSON-объект",
                        "n": "ServiceObject"
                        }
                    ],
                    "_type": "record"
                    }
                },
                "id": 1
                }

            url_real = "https://online.sbis.ru/msg/service/"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points = json.loads(response_points.text)
            # str_to_dict_points["result"]["d"][3]["chat_name"]
            

        elif date_now.day in list(range(5,10)):
            
            parameters_real = {

            "jsonrpc": "2.0",
            "protocol": 6,
            "method": "PublicMsgApi.MessageSend",
            "params": {
                "dialogID": myuuid_garant,
                "messageID": None,
                "answer": None,
                "text": f"""
                Гарант. За {var_name_report_month_prev} АКБ составил {q_arr_acb_prev_month}, продлено клиентов {q_arr_extension_prev_month}, восстановленных клиентов {q_arr_rec_prev_month}, новых клиентов {q_arr_new_prev_month} и отпало {q_arr_loss_prev_month} клиентов, процент отпада составил {q_percent_of_compl_prev:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
                Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_prev_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1_prev:.0%}. \n
                
                Гарант. Активная база за отчетный месяц {var_name_report_month_prev}
                {var_link_report_day} \n
                Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n

                """, 
                
                "document": None,
                "files": None,
                "recipients": [
                # link url sbis
                # DA
                "c943a420-f494-4a38-8975-d9db61c3dba7",
                # rop garant
                "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
                # om
                # "88619719-b479-46aa-84c4-80ca85d019c1", 
                # "300aa502-0455-4684-bfed-0c337f831fd6",
                # ms
                # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
                # "656712d1-afce-4296-af20-71076b5337fe", 
                # "01c7b684-a807-488e-bde4-ea40e7ef54ae",
                # hr
                'edeefe05-a0da-4f43-b978-56b79b78be59',
                # rop back office
                "dde503d0-53c2-4afd-a3ed-62030a31981d",
                ],
                "options": {
                    "d": [
                        "Гарант. Показатели АКБ и списки продления на следующий месяц.",
                        0,
                        {}
                    ],
                    "s": [
                        {
                        "t": "Строка",
                        "n": "Title"
                        },
                        {
                        "t": "Число целое",
                        "n": "TextFormat"
                        },
                        {
                        "t": "JSON-объект",
                        "n": "ServiceObject"
                        }
                    ],
                    "_type": "record"
                    }
                },
                "id": 1
                }

            url_real = "https://online.sbis.ru/msg/service/"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points = json.loads(response_points.text)
            # str_to_dict_points["result"]["d"][3]["chat_name"]
            
           
           
        elif date_now.day == 15:

            parameters_real = {

            "jsonrpc": "2.0",
            "protocol": 6,
            "method": "PublicMsgApi.MessageSend",
            "params": {
                "dialogID": myuuid_garant,
                "messageID": None,
                "answer": None,
                "text": f"""

                Гарант. За {var_name_report_month} АКБ составил {q_arr_acb_curr_month}, продлено клиентов {q_arr_extension_curr_month}, восстановленных клиентов {q_arr_rec_curr_month}, новых клиентов {q_arr_new_curr_month} и отпало {q_arr_loss_curr_month} клиентов, процент отпада составил {q_percent_of_compl:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
                Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_curr_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1:.0%}. \n
                
                Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n
                _________________________________________
                _________________________________________
                _________________________________________ \n
                Гарант. Списки на продление на {var_name_month} от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_extension_base} \n
                Гарант. ИНН для заливки в список на {var_name_month} от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_extension_inn_base}
                """, 
                
                "document": None,
                "files": None,
                "recipients": [
                # link url sbis
                # DA
                "c943a420-f494-4a38-8975-d9db61c3dba7",
                # rop garant
                "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
                # om
                # "88619719-b479-46aa-84c4-80ca85d019c1", 
                # "300aa502-0455-4684-bfed-0c337f831fd6",
                # ms
                # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
                # "656712d1-afce-4296-af20-71076b5337fe", 
                "300aa502-0455-4684-bfed-0c337f831fd6", 
                "01c7b684-a807-488e-bde4-ea40e7ef54ae",
                "f00ee0a8-f5dc-4272-b095-5777f3aa0ba5",
                # "", 
                # rop back office
                "dde503d0-53c2-4afd-a3ed-62030a31981d",
                ],
                "options": {
                    "d": [
                        "Гарант. Списки на продление",
                        0,
                        {}
                    ],
                    "s": [
                        {
                        "t": "Строка",
                        "n": "Title"
                        },
                        {
                        "t": "Число целое",
                        "n": "TextFormat"
                        },
                        {
                        "t": "JSON-объект",
                        "n": "ServiceObject"
                        }
                    ],
                    "_type": "record"
                    }
                },
                "id": 1
                }



            url_real = "https://online.sbis.ru/msg/service/"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points = json.loads(response_points.text)
            # str_to_dict_points["result"]["d"][3]["chat_name"]

        elif date_now.day in list(range(16,18)):

            parameters_real = {

            "jsonrpc": "2.0",
            "protocol": 6,
            "method": "PublicMsgApi.MessageSend",
            "params": {
                "dialogID": myuuid_garant,
                "messageID": None,
                "answer": None,
                "text": f"""

                Гарант. За {var_name_report_month} АКБ составил {q_arr_acb_curr_month}, продлено клиентов {q_arr_extension_curr_month}, восстановленных клиентов {q_arr_rec_curr_month}, новых клиентов {q_arr_new_curr_month} и отпало {q_arr_loss_curr_month} клиентов, процент отпада составил {q_percent_of_compl:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
                Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_curr_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1:.0%}. \n
                
                Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n
                """, 
                
                "document": None,
                "files": None,
                "recipients": [
                # link url sbis
                # DA
                "c943a420-f494-4a38-8975-d9db61c3dba7",
                # rop garant
                "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
                # om
                # "88619719-b479-46aa-84c4-80ca85d019c1", 
                # "300aa502-0455-4684-bfed-0c337f831fd6",
                # ms
                # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
                # "656712d1-afce-4296-af20-71076b5337fe", 
                
                "300aa502-0455-4684-bfed-0c337f831fd6", 
                # "", 
                # rop back office
                "dde503d0-53c2-4afd-a3ed-62030a31981d",
                ],
                "options": {
                    "d": [
                        "Гарант. Списки на продление. Отпады по текущему месяцу",
                        0,
                        {}
                    ],
                    "s": [
                        {
                        "t": "Строка",
                        "n": "Title"
                        },
                        {
                        "t": "Число целое",
                        "n": "TextFormat"
                        },
                        {
                        "t": "JSON-объект",
                        "n": "ServiceObject"
                        }
                    ],
                    "_type": "record"
                    }
                },
                "id": 1
                }



            url_real = "https://online.sbis.ru/msg/service/"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points = json.loads(response_points.text)
            # str_to_dict_points["result"]["d"][3]["chat_name"]


        elif date_now.day in list(range(25,26)):
            parameters_real = {

            "jsonrpc": "2.0",
            "protocol": 6,
            "method": "PublicMsgApi.MessageSend",
            "params": {
                "dialogID": myuuid_garant,
                "messageID": None,
                "answer": None,
                "text": f"""

                Гарант. За {var_name_report_month} АКБ составил {q_arr_acb_curr_month}, продлено клиентов {q_arr_extension_curr_month}, восстановленных клиентов {q_arr_rec_curr_month}, новых клиентов {q_arr_new_curr_month} и отпало {q_arr_loss_curr_month} клиентов, процент отпада составил {q_percent_of_compl:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
                Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_curr_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1:.0%}. \n
                
                Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n               
                """, 
                
                "document": None,
                "files": None,
                "recipients": [
                # link url sbis
                # DA
                "c943a420-f494-4a38-8975-d9db61c3dba7",
                # rop garant
                "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
                # om
                # "88619719-b479-46aa-84c4-80ca85d019c1", 
                # "300aa502-0455-4684-bfed-0c337f831fd6",
                # ms
                # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
                # "656712d1-afce-4296-af20-71076b5337fe", 
                # "300aa502-0455-4684-bfed-0c337f831fd6", 
                # "", 
                # rop back office
                # "dde503d0-53c2-4afd-a3ed-62030a31981d",
                ],
                "options": {
                    "d": [
                        "Гарант. Списки на продление. Отпады по текущему месяцу",
                        0,
                        {}
                    ],
                    "s": [
                        {
                        "t": "Строка",
                        "n": "Title"
                        },
                        {
                        "t": "Число целое",
                        "n": "TextFormat"
                        },
                        {
                        "t": "JSON-объект",
                        "n": "ServiceObject"
                        }
                    ],
                    "_type": "record"
                    }
                },
                "id": 1
                }



            url_real = "https://online.sbis.ru/msg/service/"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points = json.loads(response_points.text)
            # str_to_dict_points["result"]["d"][3]["chat_name"]


        else:

            parameters_real = {

            "jsonrpc": "2.0",
            "protocol": 6,
            "method": "PublicMsgApi.MessageSend",
            "params": {
                "dialogID": myuuid_garant,
                "messageID": None,
                "answer": None,
                "text": f"""

                Гарант. За {var_name_report_month} АКБ составил {q_arr_acb_curr_month}, продлено клиентов {q_arr_extension_curr_month}, восстановленных клиентов {q_arr_rec_curr_month}, новых клиентов {q_arr_new_curr_month} и отпало {q_arr_loss_curr_month} клиентов, процент отпада составил {q_percent_of_compl:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
                Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_curr_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1:.0%}. \n
                
                Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
                {var_link_active_base} \n               
                """, 
                
                "document": None,
                "files": None,
                "recipients": [
                # link url sbis
                # DA
                "c943a420-f494-4a38-8975-d9db61c3dba7",
                # rop garant
                "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
                # om
                # "88619719-b479-46aa-84c4-80ca85d019c1", 
                # "300aa502-0455-4684-bfed-0c337f831fd6",
                # ms
                # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
                # "656712d1-afce-4296-af20-71076b5337fe", 
                # "300aa502-0455-4684-bfed-0c337f831fd6", 
                # "", 
                # rop back office
                # "dde503d0-53c2-4afd-a3ed-62030a31981d",
                ],
                "options": {
                    "d": [
                        "Гарант. Списки на продление. Отпады по текущему месяцу",
                        0,
                        {}
                    ],
                    "s": [
                        {
                        "t": "Строка",
                        "n": "Title"
                        },
                        {
                        "t": "Число целое",
                        "n": "TextFormat"
                        },
                        {
                        "t": "JSON-объект",
                        "n": "ServiceObject"
                        }
                    ],
                    "_type": "record"
                    }
                },
                "id": 1
                }



            url_real = "https://online.sbis.ru/msg/service/"

            response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # str_to_dict_points = json.loads(response_points.text)
            # str_to_dict_points["result"]["d"][3]["chat_name"]

         
         
            
            
            
            # parameters_real = {

            # "jsonrpc": "2.0",
            # "protocol": 6,
            # "method": "PublicMsgApi.MessageSend",
            # "params": {
            #     "dialogID": myuuid_garant,
            #     "messageID": None,
            #     "answer": None,
            #     "text": f"""

            #     Гарант. За {var_name_report_month} АКБ составил {q_arr_acb_curr_month}, продлено клиентов {q_arr_extension_curr_month}, восстановленных клиентов {q_arr_rec_curr_month}, новых клиентов {q_arr_new_curr_month} и отпало {q_arr_loss_curr_month} клиентов, процент отпада составил {q_percent_of_compl:.0%} от {datetime.datetime.now().date() + relativedelta(days=-1)}. \n
            #     Отпад, без учета ежемесячных лицензий составляет {q_arr_loss_curr_month_without_1}, тогда процент отпада составляет {q_percent_of_compl_without_1:.0%}. \n
                
            #     Гарант. Активная база от {datetime.datetime.now().date() + relativedelta(days=-1)}
            #     {var_link_active_base} \n
            #     # _________________________________________
            #     # _________________________________________
            #     # _________________________________________ \n
            #     # Гарант. Списки на продление на {var_name_month} от {datetime.datetime.now().date() + relativedelta(days=-1)}
            #     # {var_link_extension_base} \n
            #     # Гарант. ИНН для заливки в список на {var_name_month} от {datetime.datetime.now().date() + relativedelta(days=-1)}
            #     # {var_link_extension_inn_base}
            #     """, 
                
            #     "document": None,
            #     "files": None,
            #     "recipients": [
            #     # link url sbis
            #     # DA
            #     "c943a420-f494-4a38-8975-d9db61c3dba7",
            #     # rop garant
            #     # "cda9b72d-0ecb-4eb9-b31f-4918d6588555", 
            #     # om
            #     # "88619719-b479-46aa-84c4-80ca85d019c1", 
            #     # "300aa502-0455-4684-bfed-0c337f831fd6",
            #     # ms
            #     # "53b10771-8c6d-4900-ba82-b1ce64a8194d", 
            #     # "656712d1-afce-4296-af20-71076b5337fe", 
            #     # "300aa502-0455-4684-bfed-0c337f831fd6", 
            #     # "", 
            #     # rop back office
            #     # "dde503d0-53c2-4afd-a3ed-62030a31981d",
            #     ],
            #     "options": {
            #         "d": [
            #             "Гарант. Списки на продление. Отпады по текущему месяцу",
            #             0,
            #             {}
            #         ],
            #         "s": [
            #             {
            #             "t": "Строка",
            #             "n": "Title"
            #             },
            #             {
            #             "t": "Число целое",
            #             "n": "TextFormat"
            #             },
            #             {
            #             "t": "JSON-объект",
            #             "n": "ServiceObject"
            #             }
            #         ],
            #         "_type": "record"
            #         }
            #     },
            #     "id": 1
            #     }



            # url_real = "https://online.sbis.ru/msg/service/"

            # response_points = requests.post(url_real, json=parameters_real, headers=headers)
            # # str_to_dict_points = json.loads(response_points.text)
            # # str_to_dict_points["result"]["d"][3]["chat_name"]

    op1 = PythonOperator(
        task_id='algoritm',
        python_callable=algoritm
    )
    
    op2 = PythonOperator(
        task_id='mess',
        python_callable=send_message
    )

op1 >> op2
