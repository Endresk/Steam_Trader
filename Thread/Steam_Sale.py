import random
import time
import re
import json
import os
from PyQt5.QtCore import QThread
from PyQt5 import QtCore
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from decimal import *

from Cookies.cookies import STEAM
from db_connection import *

useragent = UserAgent()


class TradeSteam(QThread):
    updated_steam = QtCore.pyqtSignal(int)

    def __init__(self, text):
        QThread.__init__(self)
        self.value = 0
        self.text = text

        options = webdriver.ChromeOptions()
        # options.headless = True
        options.add_argument("start-maximized")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument('--disable-gpu')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument(f"user-agent=f'{useragent.random}'")
        options.add_argument('--allow-profiles-outside-user-dir')
        options.add_argument('--enable-profile-shortcut-manager')

        options.add_argument(f'--user-data-dir={os.getcwd()}\\User_steam')
        options.add_argument('--profile-directory=Profile 1')
        options.add_argument('--profiling-flush=n')
        options.add_argument('--allow-profiles-outside-user-dir')

        self.driver = webdriver.Chrome(executable_path="chromedriver", options=options)
        self.driver.set_window_size(1920, 1080)

    def run(self):
        self.login()
        self.table_steam()

    def stop(self):
        self.terminate()
        self.wait()

    def stop_driver(self):
        self.stop()
        main_page = self.driver.window_handles[0]
        self.driver.switch_to.window(main_page)
        self.driver.quit()

    def login(self):

        driver = self.driver
        STEAM(driver)

    def table_steam(self):

        try:
            connection.autocommit = True
            cursor = connection.cursor()

            while True:

                driver = self.driver
                print('Connected to a table, STEAM')
                self.updated_steam.emit(0)

                user_page = 'https://steamcommunity.com/market/search/render/?query=&start=0' \
                            '&count=100&search_descriptions=0&sort_column=popular' \
                            '&sort_dir=asc&appid=730&norender=1'
                driver.get(user_page)

                one_items = json.loads(driver.find_element(By.TAG_NAME, 'body').text)
                total_count = one_items['total_count']

                h = 0
                total_count_res = ((total_count // 100) + 1)
                value_total_count = float(100 / total_count_res)

                for i in range(0, total_count, 100):

                    h += value_total_count
                    self.value = round(h)
                    self.updated_steam.emit(self.value)

                    if i == 0:
                        items = one_items
                    else:
                        user_page_100 = f'https://steamcommunity.com/market/search/render/?query=&start={i}' \
                                        f'&count={i + 100}&search_descriptions=0&sort_column=popular' \
                                        f'&sort_dir=asc&appid=730&norender=1 '
                        driver.get(user_page_100)

                        body = str(driver.find_element(By.TAG_NAME, 'body').text)

                        if len(body) > 0:
                            pass
                        elif body == 'null':
                            print("STEAM блок отключается на 25 - 30 сккунд...")
                            time.sleep(random.randrange(25, 30))
                        else:
                            continue

                        items = json.loads(driver.find_element(By.TAG_NAME, 'body').text)

                    for j in items['results']:
                        item_hash_name = j['asset_description']['market_hash_name'].replace("'", "''")

                        if len(item_hash_name) > 1:
                            item_price = Decimal(j['sell_price_text'].replace(" pуб.", "").replace(",", "."))

                            item_price_ru = "{:.2f}".format(item_price)
                            item_price_en = "{:.2f}".format(item_price / Decimal(self.text))

                            cursor.execute(
                                f"SELECT market_hash_name FROM items WHERE market_hash_name = '{item_hash_name}'")
                            item_value = cursor.fetchone()

                            if item_value:
                                query_table = (
                                    f"UPDATE items SET steam_en = {item_price_en}, steam_ru = {item_price_ru} "
                                    f"where market_hash_name = '{item_hash_name}'"
                                )
                                cursor.execute(query_table)

                            elif item_value is None:
                                query_table = (
                                    f"INSERT INTO items (market_hash_name, steam_en, steam_ru) VALUES "
                                    f"('{item_hash_name}', {item_price_en}, {item_price_ru})"
                                )
                                cursor.execute(query_table)
                        else:
                            print("\n\n\n-----------------", item_hash_name, "----------------\n\n\n")
                print("--- STEAM отлючается на 1-1.5 часа ---")
                time.sleep(random.randrange(3600, 5400))

        except Exception as ex:
            print("Ошибка Steam: ", ex)
            time.sleep(random.randrange(12, 18))
            self.updated_steam.emit(0)
