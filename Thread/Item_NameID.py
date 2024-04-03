import random
import re
import time

import numpy as np
import os

from PyQt5.QtCore import QThread
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By

from db_connection import *

from Cookies.cookies import STEAM

useragent = UserAgent()


class Item_NameID(QThread):

    def __init__(self):
        QThread.__init__(self)
        #
        # torexe = os.popen(r'D:\\Trade\\Tor Browser\\Browser\\TorBrowser\\Tor\\tor.exe')
        # PROXY = "socks5://127.0.0.1:9050"

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

        options.add_argument(f'--user-data-dir={os.getcwd()}\\User_steam_nameid')
        options.add_argument('--profile-directory=Profile 1')
        options.add_argument('--profiling-flush=n')
        options.add_argument('--allow-profiles-outside-user-dir')

        self.driver = webdriver.Chrome(executable_path="chromedriver", options=options)
        self.driver.set_window_size(1920, 1080)

    def run(self):
        self.login()
        self.item_nameid()

    def stop(self):
        self.terminate()
        self.wait()

    def stop_driver(self):
        main_page = self.driver.window_handles[0]
        self.driver.switch_to.window(main_page)
        self.driver.quit()

    def login(self):

        driver = self.driver
        STEAM(driver)

    def item_nameid(self):

        try:
            while True:
                connection.autocommit = True
                cursor = connection.cursor()

                driver = self.driver
                print('Connected to a table, item_nameid')

                cursor.execute(
                    "SELECT i.market_hash_name FROM items i "
                    "WHERE i.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' and "
                    "item_nameid is null"
                )

                item_name = cursor.fetchall()

                print(item_name)

                if len(item_name) > 0:

                    for name in item_name:
                        steam_link = (
                            f'https://steamcommunity.com/market/listings/730/{name[0]}')

                        driver.get(steam_link)

                        full_page = driver.find_element(By.TAG_NAME, 'body')

                        item_nameid = re.findall(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)',
                                                 str(full_page.get_attribute('innerHTML')))

                        print(item_nameid)
                        item_nameid = np.asarray(item_nameid[0])

                        market_hash_name = name[0].replace("'", "''")
                        query_table = (
                            f"UPDATE items SET item_nameid = {item_nameid} "
                            f"where market_hash_name = '{market_hash_name}'"
                        )
                        cursor.execute(query_table)
                else:
                    print("--- Все итемы заполнены Item_NameID, поток остановлен! ---")
                    break

        except Exception as ex:
            print("Ошибка Item_NameID: ", ex)
            time.sleep(random.randrange(12, 18))
