import datetime
import os
import time
import pickle
import sys
import random
from decimal import *

from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from PyQt5.QtCore import QThread

from Cookies.cookies import CSM_LIVE
from db_connection import connection


class TradeCSMLive(QThread):

    def __init__(self, text):
        QThread.__init__(self)
        self.value = 0
        self.text = text

        options = webdriver.ChromeOptions()
        # options.add_argument('--headless')
        # options.headless = True
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--no-sandbox")
        options.add_argument('--disable-gpu')

        prefs = {'profile.default_content_setting_values': { 'javascript': 0,
            'images': 2,
            'notifications': 2,
            'plugins': 2, 'popups': 2, 'geolocation': 2,
            'auto_select_certificate': 2,
            'fullscreen': 2,
            'mouselock': 2, 'mixed_script': 2, 'media_stream': 2,
            'media_stream_mic': 2, 'media_stream_camera': 2,
            'protocol_handlers': 2,
            'ppapi_broker': 2, 'automatic_downloads': 2,
            'midi_sysex': 2,
            'push_messaging': 2, 'ssl_cert_decisions': 2,
            'metro_switch_to_desktop': 2,
            'protected_media_identifier': 2, 'app_banner': 2,
            'site_engagement': 2,
            'durable_storage': 2
        }}
        options.add_experimental_option('prefs', prefs)
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36")

        options.add_argument("--disable-blink-features-AutomationControlled")
        options.add_argument(f"user-data-dir={os.getcwd()}\\User_csmlive")
        options.add_argument('--enable-aggressive-domstorage-flushing')
        options.add_argument('--enable-profile-shortcut-manager')
        options.add_argument('--profile-directory=Profile 1')
        options.add_argument('--profiling-flush=n')
        options.add_argument('--allow-profiles-outside-user-dir')

        self.driver = webdriver.Chrome(executable_path="chromedriver", options=options)
        self.driver.set_window_size(1920, 1080)

    def run(self):
        CSM_LIVE(self.driver)
        self.start_table_csm_live()
        self.table_csm_live()

    def stop(self):
        main_page = self.driver.window_handles[0]
        self.driver.switch_to.window(main_page)
        self.driver.quit()
        self.terminate()
        self.wait()

    def selector_exists(self, url):
        browser = self.driver
        try:
            browser.find_element(By.CSS_SELECTOR, url)
        except NoSuchElementException:
            return False
        return True

    def start_table_csm_live(self):
        try:
            driver = self.driver
            time.sleep(6)

            selector_li_lm = '#block_items_bot > div.block_header.superclass_space.hint_aim_6 > ' \
                             'div.block_header_wrap.superclass_space > div.bot_sort > div > div > ' \
                             'div > ul > li:nth-child(4)'

            click_pro = "div.sidebar_switcher_title:nth-child(1) > label:nth-child(1) > input:nth-child(1)"
            click_inactive = ".trade_lock_list_container.pro_version_off.slider.superclass_space.inactive"

            options_filters = 'body > div.body_scroll > div.main > div.content.login_test_3.auth > ' \
                              'div.trade_container.wrapper > div.row > div.column_2 > div > div.filter_mobile > ' \
                              'div > div.filter > div.filter_more_container.pro_version_off > div > a'

            input_inactive = "#tradeLockSwitcher"

            driver.find_element(By.CSS_SELECTOR, click_pro).click()

            try:
                time.sleep(1)
                if self.selector_exists(click_inactive):
                    time.sleep(1)
                    driver.find_element(By.CSS_SELECTOR, input_inactive).click()
                driver.find_element(By.CSS_SELECTOR, options_filters).click()
            except:
                driver.find_element(By.CSS_SELECTOR, click_pro).click()
                time.sleep(1)
                if self.selector_exists(click_inactive):
                    time.sleep(1)
                    driver.find_element(By.CSS_SELECTOR, input_inactive).click()
                driver.find_element(By.CSS_SELECTOR, options_filters).click()

            float_click = '#m_filter > div.modal_content > ' \
                          'div.m_filter_row.filter_mobile_hidden.m_filter_rare_skins > div:nth-child(1) > ' \
                          'div > div.filter_checkbox_dot > div.filter_checkbox_point.pro_version_off'
            driver.find_element(By.CSS_SELECTOR, float_click).click()
            stickers_click = '#m_filter > div.modal_content > ' \
                             'div.m_filter_row.filter_mobile_hidden.m_filter_rare_skins > div:nth-child(2) > ' \
                             'div > div.filter_checkbox_dot > div.filter_checkbox_point.pro_version_off'
            driver.find_element(By.CSS_SELECTOR, stickers_click).click()

            button_close = '#m_filter_close'
            WebDriverWait(driver, 0).until(EC.element_to_be_clickable((By.CSS_SELECTOR, button_close))).click()

            time.sleep(1)

            container = driver.find_element(By.CSS_SELECTOR, selector_li_lm)
            driver.execute_script("arguments[0].style.display = 'block';", container)

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            driver.execute_script("return [document.body.clientWidth, document.body.clientHeight];")

            self.live_mode()

            print("Жду предметы...")

        except:
            pass

    def table_csm_live(self):
        try:
            while True:
                connection.autocommit = True
                cursor = connection.cursor()

                driver = self.driver

                items_urls = "#main_container_bot > div.items"
                stop_search = '.control_skins_ticker_stop'
                start_search = '.control_skins_ticker_play'
                item_price_css = "div[class='p']"
                item_wear_css = "div[class='s_c'] > div[class='r']"

                list_wear = {"FN": "Factory New",
                             "MW": "Minimal Wear",
                             "FT": "Field-Tested",
                             "WW": "Well-Worn",
                             "BS": "Battle-Scarred"}

                items = driver.find_element(By.CSS_SELECTOR, items_urls)
                items_list_app = items.find_elements(By.CSS_SELECTOR, "div[cc='item']")

                if items_list_app:

                    WebDriverWait(driver, 0).until(EC.element_to_be_clickable((By.CSS_SELECTOR, stop_search))).click()
                    start_time = time.monotonic()

                    # items_list = items.find_elements_by_css_selector("div[cc='item']")

                    count_pos = 0

                    for pos in items_list_app:

                        price_item_csm = ""

                        # print(pos.get_attribute("innerHTML"))

                        item_name = pos.find_element(By.XPATH, "div[last()]")
                        item_name_text = item_name.text

                        try:
                            item_wear = pos.find_element(By.CSS_SELECTOR, item_wear_css)
                            item_wear_text = item_wear.text.replace(" ", "")

                            if item_wear_text == "":
                                name_csm_item = f"{item_name_text}"
                            else:
                                item_wear_text = list_wear[f'{item_wear_text}']
                                name_csm_item = f"{item_name_text} ({item_wear_text})"
                        except NoSuchElementException:
                            name_csm_item = f"{item_name_text}"

                        name_csm_item = str(name_csm_item).replace("'", "''")

                        cursor.execute("select exists (select csm_live_en "
                                       f"from items where market_hash_name = '{name_csm_item}')")
                        exists_name = cursor.fetchone()[0]

                        if exists_name and name_csm_item != "":
                            count_pos += 1
                            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            cursor.execute("select csm_live_en "
                                           f"from items where market_hash_name = '{name_csm_item}'")
                            bd_price_csm_live = cursor.fetchone()[0]

                            try:
                                item_price = pos.find_element(By.CSS_SELECTOR, item_price_css)
                            except NoSuchElementException:
                                continue

                            item_price_text = item_price.text.replace("$ ", '')
                            price_item_csm = Decimal(item_price_text)
                            price_csm_item = str("{:.2f}".format(price_item_csm))
                            price_item_csm_rub = Decimal(item_price_text) * Decimal(self.text)
                            price_csm_item_rub = str("{:.2f}".format(price_item_csm_rub))

                            if bd_price_csm_live is None:
                                query_table_basic = (
                                    f"UPDATE items SET csm_live_en = {price_csm_item}, "
                                    f"csm_live_count = csm_live_count + 1, csm_live_ru = {price_csm_item_rub}, "
                                    f"csm_live_datetime = '{now}' "
                                    f"where market_hash_name = '{name_csm_item}'"
                                )
                                cursor.execute(query_table_basic)
                                # print(bd_price_csm_live, price_item_csm, name_csm_item)
                            else:
                                price_i = Decimal(bd_price_csm_live)
                                if price_i == price_item_csm:
                                    query_table_basic = (
                                        f"UPDATE items SET csm_live_count = csm_live_count + 1, "
                                        f"csm_live_datetime = '{now}' "
                                        f"where market_hash_name = '{name_csm_item}'"
                                    )
                                    cursor.execute(query_table_basic)
                                    # print("price_i == price_item_csm", bd_price_csm_live, price_item_csm, name_csm_item)
                                else:
                                    price_i -= price_item_csm
                                    if price_i <= 0.02:
                                        query_table_basic = (
                                            f"UPDATE items SET csm_live_en = {price_csm_item}, "
                                            f"csm_live_count = csm_live_count + 1, csm_live_ru = {price_csm_item_rub}, "
                                            f"csm_live_datetime = '{now}' "
                                            f"where market_hash_name = '{name_csm_item}'"
                                        )
                                        cursor.execute(query_table_basic)
                                        # print("if price_i <= 0.02", bd_price_csm_live, price_item_csm, name_csm_item)
                                    else:
                                        continue
                                        # print("else price_i > 0.02", bd_price_csm_live, price_item_csm, name_csm_item)
                        else:
                            print("Name False",  price_item_csm, name_csm_item)
                            # print(pos.get_attribute("innerHTML"))
                            continue

                    WebDriverWait(driver, 0).until(EC.element_to_be_clickable((By.CSS_SELECTOR, start_search))).click()
                    print("Предметы появились в количестве:", count_pos,
                          "--- %s seconds ---" % "{0:.2f}".format(time.monotonic() - start_time))
                    self.live_mode()
                else:
                    time.sleep(0.44)
        except:
              pass

    def live_mode(self):
        try:
            driver = self.driver

            more_detailed = "#block_items_bot > div.block_header.superclass_space.hint_aim_6 > " \
                            "div.block_header_wrap.superclass_space > div.bot_sort > div > a > span"

            element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, more_detailed)))
            ActionChains(driver).move_to_element(element).perform()

            live_mode = "//a[@data-option='liveMode']"
            WebDriverWait(driver, 0).until(EC.element_to_be_clickable((By.XPATH, live_mode))).click()

        except:
            pass
