import requests
import json
import sys

from PyQt5.QtCore import QThread
from PyQt5 import QtCore
from fake_useragent import UserAgent
from decimal import *

from db_connection import *


useragent = UserAgent()


class TradeCSMSale(QThread):
    updated_csm = QtCore.pyqtSignal(int)
    Update_TradeCSM = QtCore.pyqtSignal(int, int, QtCore.QVariant)

    def __init__(self, text):
        QThread.__init__(self)
        self.value = 0
        self.text = text

    def run(self):
        self.table_csm()

    def stop(self):
        self.terminate()
        self.wait()

    def table_csm(self):

        try:
            connection.autocommit = True
            cursor = connection.cursor()

            print('Connected to a table, CSM Sale')

            # response = requests.get("https://csm.auction/api/skins_base")
            response = requests.get("https://old.cs.money/js/database-skins/library-en-730.js")

            h = 0
            names = []

            if response.status_code == 200:
                # items = json.loads(response.content.decode())
                items = json.loads(response.content.decode().split("skinsBaseList[730] = ")[1])

                for item in items:
                    names.append(items[item])
                len_items = len(names)
                value_total_count = float(33 / len_items)

                cs_money_prices = {}
                for item in items:
                    item = items.get(item)
                    name = item.get('m').replace("/", '-')
                    price = item.get('a')
                    # name_rus = item.get('j')

                    if "Doppler" in name:
                        phase = name.split("Doppler ")[1].split(" (")[0]
                        name = name.replace(phase + " ", "")
                        try:
                            cs_money_prices[name]['doppler'][phase] = price
                        except KeyError:
                            cs_money_prices[name] = {
                                'price': price,
                                'doppler': {
                                    phase: price
                                }
                            }
                        if phase == "Phase 3":
                            cs_money_prices[name]['price'] = price
                    else:
                        cs_money_prices[name] = {'price': price}
            else:
                error = "Could not get items from cs.money"
                print(error, " status code: ", response.status_code)
                return {
                    'statusCode': response.status_code,
                    'body': json.dumps(error)
                }

            headers_list_overstock = {'User-Agent': f'{useragent.random}'}
            response_list_overstock = requests.get("https://old.cs.money/list_overstock?appid=730",
                                                   headers_list_overstock)

            h = 0

            query_table_status = (
                f"UPDATE items SET status = 'Tradable'"
            )
            cursor.execute(query_table_status)
            print("Столбец статус обнулен!")

            if response_list_overstock.status_code == 200:
                items_list_overstock = response_list_overstock.json()

                len_overstock = len(items_list_overstock)
                vtc_overstock = float(33 / len_overstock)

                for i in items_list_overstock:
                    cs_money_name = i['market_hash_name']
                    cs_money_name = str(cs_money_name).replace("'", "''")
                    status = "Over"
                    query_table_basic = (
                        f"UPDATE items SET status = '{status}' where market_hash_name = '{cs_money_name}'"
                    )
                    cursor.execute(query_table_basic)

                    h += vtc_overstock
                    self.value = round(h)
                    self.updated_csm.emit(self.value)

            else:
                error = "Could not get overstock from cs.money"
                print(error, " status code: ", response_list_overstock.status_code)
                return {
                    'statusCode': response_list_overstock.status_code,
                    'body': json.dumps(error)
                }

            headers_list_unavailable = {'User-Agent': f'{useragent.random}'}
            response_list_unavailable = requests.get("https://old.cs.money/list_unavailable?appid=730",
                                                     headers_list_unavailable)

            if response_list_unavailable.status_code == 200:
                items_list_unavailable = response_list_unavailable.json()
                len_unavailable = len(items_list_unavailable)
                vtc_unavailable = float(33 / len_unavailable)
                for i in items_list_unavailable:
                    cs_money_name = i['market_hash_name']
                    cs_money_name = str(cs_money_name).replace("'", "''")
                    status = "Unav"
                    query_table_basic = (
                        f"UPDATE items SET status = '{status}' where market_hash_name = '{cs_money_name}'"
                    )
                    cursor.execute(query_table_basic)

                    h += vtc_unavailable
                    self.value = round(h)
                    self.updated_csm.emit(self.value)
            else:
                error = "Could not get unavailable from cs.money"
                print(error, " status code: ", response_list_unavailable.status_code)
                return {
                    'statusCode': response_list_unavailable.status_code,
                    'body': json.dumps(error)
                }

            for name_item, price_item in cs_money_prices.items():
                name_item = str(name_item).replace("'", "''")
                price_csm = Decimal(price_item['price'])
                price_csm_item = str("{:.2f}".format(price_csm))
                price_csm_rub = Decimal(price_item['price']) * Decimal(self.text)
                price_csm_item_rub = str("{:.2f}".format(price_csm_rub))

                if price_csm > 0:
                    query_table_basic = (
                        f"UPDATE items SET csm_sale_en = {price_csm_item}, "
                        f"csm_sale_ru = {price_csm_item_rub} where market_hash_name = '{name_item}'"
                    )
                    cursor.execute(query_table_basic)
                h += value_total_count
                self.value = round(h)
                self.updated_csm.emit(self.value)
            self.updated_csm.emit(100)

        except Exception as ex:
            print("Ошибка CSM", ex)
            self.updated_csm.emit(0)
