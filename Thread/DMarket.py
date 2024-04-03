import json
import random
import time

import requests

from PyQt5.QtCore import QThread
from PyQt5 import QtCore
from fake_useragent import UserAgent
from decimal import *
from db_connection import *
from datetime import datetime
from nacl.bindings import crypto_sign

useragent = UserAgent()


class TradeDMarket(QThread):
    updated_dm = QtCore.pyqtSignal(int)

    def __init__(self, text):
        QThread.__init__(self)
        self.value = 0
        self.text = text
        self.API_URL = "https://api.dmarket.com"

    def run(self):
        while True:
            self.DMarket()

    def stop(self):
        self.terminate()
        self.wait()

    def DMarket(self):
        try:
            connection.autocommit = True
            cursor = connection.cursor()

            print('Connected to a table, DMarket')
            cursor.execute(
                "SELECT i.market_hash_name FROM items i "
                "WHERE i.market_hash_name not SIMILAR TO "
                "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
            )

            item_value = cursor.fetchall()
            # (all) fetchall  (1) fetchone

            total_count = float(100 / int(len(item_value)))

            public_key = "d89ae4eefdb77797da976a66c4c47b3867bab4e5428f8b56aa5d845f8428b5db"
            secret_key = "18ab2ee81c42620558394228ed3e434e3fe5ef515b072b38d69b3d564e682147d89ae4eefdb77797da976a66c4c47b3867bab4e5428f8b56aa5d845f8428b5db"

            h = 0
            for name in item_value:
                url_price = "https://api.dmarket.com/" \
                            "marketplace-api/v1/" \
                            "cumulative-price-levels?" \
                            f"Title={name[0]}" \
                            "&GameID=a8db"

                api_path = "marketplace-api/v1/cumulative-price-levels?"
                # headers = {'User-Agent': f'{useragent.random}'}
                nonce = str(round(datetime.now().timestamp()))
                string_to_sign = "GET" + api_path + nonce
                signature_prefix = "dmar ed25519 "
                encoded = string_to_sign.encode('utf-8')
                secret_bytes = bytes.fromhex(secret_key)
                signature_bytes = crypto_sign(encoded, secret_bytes)
                signature = signature_bytes[:64].hex()
                headers = {
                    "X-Api-Key": public_key,
                    "X-Request-Sign": signature_prefix + signature,
                    "X-Sign-Date": nonce
                }
                response = requests.get(url_price, headers)

                try:
                    Offer_price = list(response.json()["Offers"][0].values())
                    Offer_price = Decimal(Offer_price[0])
                    Offer_price_en = "{:.2f}".format(Offer_price)
                    Offer_price_ru = Offer_price * Decimal(self.text)
                    Offer_price_ru = "{:.2f}".format(Offer_price_ru)
                    # print(Offer_price)
                except:
                    Offer_price_en = Decimal()
                    Offer_price_ru = Decimal()
                    # print("Заказов нет")

                try:
                    Target_price = list(response.json()["Targets"][0].values())
                    Target_price = Decimal(Target_price[0])
                    Target_price_en = "{:.2f}".format(Target_price)
                    Target_price_ru = Target_price * Decimal(self.text)
                    Target_price_ru = "{:.2f}".format(Target_price_ru)
                    # print(Target_price)
                except:
                    Target_price_en = Decimal()
                    Target_price_ru = Decimal()
                    # print("Продаж нет")

                market_hash_name = name[0].replace("'", "''")

                query_table = (
                    f"UPDATE items SET dm_offers_en = {Offer_price_en}, "
                    f"dm_offers_ru = {Offer_price_ru}, "
                    f"dm_targets_en = {Target_price_en}, "
                    f"dm_targets_ru = {Target_price_ru} "
                    f"where market_hash_name = '{market_hash_name}'"
                )
                cursor.execute(query_table)

                h += total_count
                self.value = round(h)
                self.updated_dm.emit(self.value)

            print("--- DMarket отлючается 1-1.5 часа  ---")
            time.sleep(random.randrange(3600, 5400))

        except Exception as ex:
            print("Ошибка DMarket: ", ex)
            self.updated_dm.emit(0)
