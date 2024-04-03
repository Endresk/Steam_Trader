import datetime
import random
import sys
import time

import numpy as np
import requests
import pandas as pd
import pandas.io.clipboard as pyperclip

from PyQt5 import QtCore, QtWidgets, QtGui
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtCore import Qt, pyqtSlot, QModelIndex, QThread
from PyQt5.QtGui import QDoubleValidator
from PyQt5.QtWidgets import QHeaderView, QAbstractScrollArea
from fake_useragent import UserAgent
from datetime import datetime, timedelta

from Thread.CSM_Live import TradeCSMLive
from Thread.CSM_Sale import TradeCSMSale
from Thread.DMarket import TradeDMarket
from Thread.Item_NameID import Item_NameID
from Thread.Steam_Sale import TradeSteam
from trade import Ui_MainWindow

from db_connection import *

useragent = UserAgent()


class PandasTableModel(QtCore.QAbstractTableModel):
    ROW_BATCH_COUNT = 100

    def __init__(self, data, parent=None):
        QtCore.QAbstractTableModel.__init__(self, parent)
        super().__init__(parent)
        self._data = data
        self._cols = data.columns

        self.rowsLoaded = PandasTableModel.ROW_BATCH_COUNT

    def rowCount(self, parent=None):
        try:
            if not self._data.shape[0]:
                return 0

            if self._data.shape[0] <= self.rowsLoaded:
                return self._data.shape[0]
            else:
                return self.rowsLoaded
        except Exception as e:
            print("rowCount:", e)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def insertRows(self, position, rows=1, index=QModelIndex()):
        self.beginInsertRows(QModelIndex(), position, position + rows - 1)
        self.endInsertRows()
        return True

    def removeRows(self, position, rows=1, index=QModelIndex()):
        self.beginRemoveRows(QModelIndex(), position, position + rows - 1)
        del self.display[position:position + rows]
        self.endRemoveRows()
        return True

    def data(self, index, role=QtCore.Qt.DisplayRole):
        try:
            if index.isValid():
                if role == QtCore.Qt.DisplayRole or role == Qt.EditRole:
                    return str(self._data.iloc[index.row(), index.column()])
            return None
        except Exception as e:
            print("data:", e)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def setData(self, index, value, role=QtCore.Qt.DisplayRole):
        try:
            if index.isValid():
                if role == QtCore.Qt.DisplayRole:
                    item = index.internalPointer()
                    item.set_name(value)
                    self.layoutChanged.emit()
                    self.dataChanged.emit(index, index)
                    return True
            return False
        except Exception as e:
            print("setData:", e)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def canFetchMore(self, index=QModelIndex()):
        try:
            if self._data.shape[0] > self.rowsLoaded:
                return True
            else:
                return False
        except Exception as e:
            print("canFetchMore:", e)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def fetchMore(self, index=QModelIndex()):
        try:
            reminder = self._data.shape[0] - self.rowsLoaded
            itemsToFetch = min(reminder, PandasTableModel.ROW_BATCH_COUNT)
            self.beginInsertRows(QModelIndex(), self.rowsLoaded, self.rowsLoaded + itemsToFetch - 1)
            self.rowsLoaded += itemsToFetch
            self.endInsertRows()
        except Exception as e:
            print("fetchMore:", e)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def flags(self, index):
        flags = super(self.__class__, self).flags(index)
        flags |= QtCore.Qt.ItemIsEditable
        flags |= QtCore.Qt.ItemIsSelectable
        flags |= QtCore.Qt.ItemIsEnabled
        flags |= QtCore.Qt.ItemIsDragEnabled
        flags |= QtCore.Qt.ItemIsDropEnabled
        return flags

    def headerData(self, col, orientation, role=QtCore.Qt.DisplayRole):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self._data.columns[col]
        return None

    def sort(self, Ncol, order):
        """Sort table by given column number."""
        try:
            self.layoutAboutToBeChanged.emit()
            self._data = self._data.sort_values(self._cols[Ncol], ascending=order == Qt.AscendingOrder)
            self.layoutChanged.emit()
        except Exception as e:
            print("sort:", e)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def update_item(self, row, col, value):
        ix = self.index(row, col)
        self.setData(ix, value)


class DateTime(QThread):
    def __init__(self):
        QThread.__init__(self)

    def run(self):
        while True:
            connection.autocommit = True
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT market_hash_name, csm_live_datetime FROM items WHERE csm_live_datetime is not null"
                )
                csm_live_datetime = cursor.fetchall()

                current_datetime = datetime.now()

                for i in csm_live_datetime:

                    time_has_passed = current_datetime - i[1]
                    after_30_sec = time_has_passed < timedelta(seconds=81)

                    market_hash_name = i[0].replace("'", "''")

                    if i[1].date() == current_datetime.date() and after_30_sec:
                        pass
                    else:
                        query_table_basic = (
                            f"UPDATE items "
                            f"SET csm_live_en = NULL, csm_live_ru = NULL, csm_live_datetime = NULL "
                            f"WHERE market_hash_name = '{market_hash_name}' "
                        )
                        cursor.execute(query_table_basic)

            except Exception as ex:
                print("Ошибка update_datetime_csm_live", ex)
                sys.exit(-1)
            time.sleep(random.randrange(6, 8))


class MyWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        try:
            connection.autocommit = True
            cursor = connection.cursor()

            cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('items',))
            bool_value = cursor.fetchone()[0]

            if not bool_value:
                create_table = """CREATE TABLE IF NOT EXISTS items (
                                                           id SERIAL PRIMARY KEY,
                                                           market_hash_name TEXT, 
                                                           steam_en numeric(12,2),
                                                           steam_ru numeric(12,2),
                                                           item_nameid integer,
                                                           steam_buy_en numeric(12,2),
                                                           steam_buy_ru numeric(12,2),
                                                           csm_sale_en numeric(12,2),
                                                           csm_sale_ru numeric(12,2),
                                                           status text,
                                                           csm_live_en numeric(12,2),
                                                           csm_live_ru numeric(12,2),
                                                           csm_live_count numeric,
                                                           csm_live_datetime timestamp without time zone,
                                                           dm_offers_en numeric(12,2),
                                                           dm_offers_ru numeric(12,2),
                                                           dm_targets_en numeric(12,2),
                                                           dm_targets_ru numeric(12,2)
                                                         )"""
                execute_query(connection, create_table)

            self.ui = Ui_MainWindow()
            self.ui.setupUi(self)

            self.timer_dollar = QtCore.QTimer()
            self.timer_table = QtCore.QTimer()
            self.timer_item_nameid_and_steam_buy = QtCore.QTimer()
            self.timer = QtCore.QTimer()

            self.setting = QtCore.QSettings('trade', 'company_trade', self)
            self.load_setting()

            validator = QDoubleValidator(0.99, 99.99, 4).setLocale(QtCore.QLocale("ru_RU"))

            self.ui.input_rub.setValidator(validator)
            self.ui.input_dollar.setValidator(validator)
            self.ui.input_dm.setValidator(validator)
            self.ui.input_csm.setValidator(validator)
            self.ui.input_steam.setValidator(validator)
            self.ui.exchangers_rub_ot.setValidator(validator)
            self.ui.exchangers_rub_do.setValidator(validator)
            self.ui.sm_rub_do.setValidator(validator)
            self.ui.sm_rub_ot.setValidator(validator)

            self.dollar()
            self.update_item_nameid_and_steam_buy()

            self.ui.progressBar_bsteam.setMaximum(100)

            self.ui.input_dollar.textEdited.connect(self.input_compute)
            self.ui.input_rub.textEdited.connect(self.input_compute)

            self.ui.input_steam.textEdited.connect(self.sale_steam)
            self.ui.input_csm.textEdited.connect(self.sale_steam)
            self.ui.input_dm.textEdited.connect(self.sale_steam)

            self.ui.btn_sm_name_item.setEnabled(True)
            self.ui.btn_sm_name_item.clicked.connect(self.on_upload_steam)
            self.ui.btn_sm_name_item.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
            self.ui.btn_sm_name_item.customContextMenuRequested.connect(self.on_stop_steam)

            self.ui.btn_sm_item_nameid.setEnabled(True)
            self.ui.btn_sm_item_nameid.clicked.connect(self.on_upload_item_nameid)
            self.ui.btn_sm_item_nameid.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
            self.ui.btn_sm_item_nameid.customContextMenuRequested.connect(self.on_stop_item_nameid)

            self.ui.btn_csm_sail.setEnabled(True)
            self.ui.btn_csm_sail.clicked.connect(self.on_upload_csm)
            self.ui.btn_csm_sail.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
            self.ui.btn_csm_sail.customContextMenuRequested.connect(self.on_stop_csm)

            self.ui.btn_dm_name_item.setEnabled(True)
            self.ui.btn_dm_name_item.clicked.connect(self.on_upload_dm)
            self.ui.btn_dm_name_item.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
            self.ui.btn_dm_name_item.customContextMenuRequested.connect(self.on_stop_dm)

            self.ui.btn_clear.clicked.connect(self.del_csm_live_price)
            self.ui.btn_clear_2.clicked.connect(self.del_csm_live_old)

            self.timer_dollar.timeout.connect(self.dollar)
            self.timer_dollar.start(3600 * 1000)

            self.timer_table.timeout.connect(self.table_items)
            self.timer_table.start(400)

            self.timer_item_nameid_and_steam_buy.timeout.connect(self.update_item_nameid_and_steam_buy)
            self.timer_item_nameid_and_steam_buy.start(13000)

            self.TradeSteam = TradeSteam(self.ui.dollar_rate_lable.text())
            self.Item_NameID = Item_NameID()
            self.TradeCSMSale = TradeCSMSale(self.ui.dollar_rate_lable.text())
            self.TradeCSMLIVE = TradeCSMLive(self.ui.dollar_rate_lable.text())
            self.TradeCSMLIVE.start()
            self.TradeDMarket = TradeDMarket(self.ui.dollar_rate_lable.text())

            self.DateTime = DateTime()
            self.DateTime.start()

            self.ui.tableView.clicked.connect(self.on_Click)

            app.aboutToQuit.connect(self.closeEvent)

        except Exception as ex:
            print("Ошибка MAIN: ", ex)

    def del_csm_live_price(self):
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            query_table_basic = (
                f"UPDATE items SET csm_live_en = NULL,  csm_live_ru = NULL, csm_live_datetime = NULL "
            )
            cursor.execute(query_table_basic)

        except Exception as ex:
            print("Ошибка del_csm_live_price", ex)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def del_csm_live_old(self):
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            query_table_basic = (
                f"UPDATE items SET csm_live_en = NULL,  csm_live_ru = NULL, csm_live_count = 0, "
                f"csm_live_datetime = NULL "
            )
            cursor.execute(query_table_basic)

        except Exception as ex:
            print("Ошибка del_csm_live_old", ex)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def save_Setting(self):
        self.setting.setValue('rec_com_steam', self.ui.rec_com_steam.value())
        self.setting.setValue('rec_com_csm', self.ui.rec_com_csm.value())
        self.setting.setValue('rec_com_dm', self.ui.rec_com_csm.value())

        self.setting.setValue('sm_rub_ot', self.ui.sm_rub_ot.text())
        self.setting.setValue('sm_rub_do', self.ui.sm_rub_do.text())
        self.setting.setValue('exchangers_rub_ot', self.ui.exchangers_rub_ot.text())
        self.setting.setValue('exchangers_rub_do', self.ui.exchangers_rub_do.text())

    def load_setting(self):
        self.ui.rec_com_steam.setValue(self.setting.value('rec_com_steam', 13))
        self.ui.rec_com_csm.setValue(self.setting.value('rec_com_csm', 7))
        self.ui.rec_com_dm.setValue(self.setting.value('rec_com_csm', 7))

        self.ui.sm_rub_ot.setText(self.setting.value('sm_rub_ot', '0'))
        self.ui.sm_rub_do.setText(self.setting.value('sm_rub_do', '10000'))
        self.ui.exchangers_rub_ot.setText(self.setting.value('exchangers_rub_ot', '0'))
        self.ui.exchangers_rub_do.setText(self.setting.value('exchangers_rub_do', '10000'))

    def on_update_steam(self, value):
        self.ui.progressBar_bsteam.setValue(value)

    @pyqtSlot()
    def on_upload_steam(self):

        if self.Item_NameID.isRunning():
            self.Item_NameID.stop()
            print("Поток Item_nameid остановлен!")
        else:
            print("Поток Item_nameid не запущен!")
        self.TradeSteam.start()
        print("Поток Steam запустился!")
        self.TradeSteam.updated_steam.connect(self.on_update_steam)

    @pyqtSlot()
    def on_stop_steam(self):
        self.TradeSteam.stop()
        print("STOP Steam!")
        self.ui.progressBar_bsteam.setValue(0)

    def update_item_nameid_and_steam_buy(self):
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT count(*) AS exact_count FROM items "
                "WHERE item_nameid IS NULL and market_hash_name not SIMILAR TO "
                "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
            )
            item_nameid = cursor.fetchone()

            for name in item_nameid:
                text_item_nameid = np.asarray(name)
                self.ui.label_sm_item_nameid.setText(str(text_item_nameid))

        except Exception as ex:
            print("Ошибка item_nameid", ex)
            sys.exit(-1)

        try:
            cursor.execute(
                "SELECT count(*) AS exact_count FROM items "
                "WHERE steam_buy_en IS NULL and market_hash_name not SIMILAR TO "
                "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case|%Sticker%|%Souvenir%)' "
            )
            steam_buy = cursor.fetchone()

            for name in steam_buy:
                text_steam_buy = np.asarray(name)
                self.ui.label_steam_buy.setText(str(text_steam_buy))

        except Exception as ex:
            print("Ошибка steam_buy", ex)
            sys.exit(-1)

    def on_upload_item_nameid(self):

        if self.TradeSteam.isRunning():
            self.TradeSteam.stop()
            self.ui.progressBar_bsteam.setValue(0)
            print("Поток Steam остановлен!")
        else:
            print("Поток Steam не запущен!")
        self.Item_NameID.start()
        print("Поток Item_nameid запустился!")

    def on_stop_item_nameid(self):
        self.Item_NameID.stop()
        print("STOP Item_nameid!")

    def on_update_csm(self, value):
        self.ui.progressBar_bcsmsale.setValue(value)

    def on_upload_csm(self):
        self.TradeCSMSale.start()
        print("Поток CSM_sale запустился!")
        self.TradeCSMSale.updated_csm.connect(self.on_update_csm)
        self.TradeCSMSale.Update_TradeCSM.connect(PandasTableModel.update_item)

    def on_stop_csm(self):
        self.TradeCSMSale.stop()
        print("STOP CSM_sale!")
        self.ui.progressBar_bcsmsale.setValue(0)

    def on_update_dm(self, value):
        self.ui.progressBar_bdm.setValue(value)

    def on_upload_dm(self):
        self.TradeDMarket.start()
        print("START DMarket!")
        self.TradeDMarket.updated_dm.connect(self.on_update_dm)

    def on_stop_dm(self):
        self.TradeDMarket.stop()
        print("STOP DMarket!")
        self.ui.progressBar_bdm.setValue(0)

    def sale_steam(self):

        try:
            input_steam = self.ui.input_steam.text().replace(",", ".")
            commission_steam = self.ui.rec_com_steam.text().replace(",", ".")

            input_csm = self.ui.input_csm.text().replace(",", ".")
            commission_csm = self.ui.rec_com_csm.text().replace(",", ".")

            input_dm = self.ui.input_dm.text().replace(",", ".")
            commission_dm = self.ui.rec_com_dm.text().replace(",", ".")

            if input_steam != "":
                h = np.asarray(float(input_steam)) - \
                    ((np.asarray(float(input_steam)) * np.asarray(float(commission_steam))) / 100)
                self.ui.rec_steam.setText(str("{:.2f}".format(h)))
            else:
                self.ui.rec_steam.setText("")

            if input_csm != "":
                h = np.asarray(float(input_csm)) - \
                    ((np.asarray(float(input_csm)) * np.asarray(float(commission_csm))) / 100)
                self.ui.rec_csm.setText(str("{:.2f}".format(h)))
            else:
                self.ui.rec_csm.setText("")

            if input_dm != "":
                h = np.asarray(float(input_dm)) - \
                    ((np.asarray(float(input_dm)) * np.asarray(float(commission_dm))) / 100)
                self.ui.rec_dm.setText(str("{:.2f}".format(h)))
            else:
                self.ui.rec_dm.setText("")

        except Exception as ex:
            print("Ошибка ввода", ex)

    def dollar(self):
        DOLLAR = 'https://www.cbr-xml-daily.ru/daily_json.js'
        headers = {'User-Agent': f'{useragent.random}'}

        full_page = requests.get(DOLLAR, headers)
        data = full_page.json()
        text_dollar = np.asarray(float("{:.4f}".format(data['Valute']['USD']['Value'])))

        self.ui.dollar_rate_lable.setText(str(text_dollar))

    def input_compute(self):

        try:
            text_dollar = self.ui.dollar_rate_lable.text()
            rec_input_rub = self.ui.input_rub.text().replace(",", ".")
            rec_input_dollar = self.ui.input_dollar.text().replace(",", ".")

            if rec_input_rub != "":
                h = np.asarray(float(rec_input_rub)) / np.asarray(float(text_dollar))

                self.ui.rec_dollar.setText(str("{:.2f}".format(h)))
            else:
                self.ui.rec_dollar.setText("")

            if rec_input_dollar != "":
                h = np.asarray(float(rec_input_dollar)) * np.asarray(float(text_dollar))
                self.ui.rec_rub.setText(str("{:.2f}".format(h)))
            else:
                self.ui.rec_rub.setText("")

        except Exception as ex:
            print("Ошибка ввода", ex)

    def table_items(self):

        self.ui.tableView.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.ui.tableView.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.ui.tableView.horizontalHeader().resizeSection(0, 310)

        self.ui.tableView.horizontalHeader().setStretchLastSection(True)

        try:
            connection.autocommit = True
            cursor = connection.cursor()

            one_ser_rub_ot = self.ui.sm_rub_ot.text()
            one_ser_rub_do = self.ui.sm_rub_do.text()

            two_ser_rub_ot = self.ui.exchangers_rub_ot.text()
            two_ser_rub_do = self.ui.exchangers_rub_do.text()

            if two_ser_rub_ot == "":
                self.ui.exchangers_rub_ot.setText("0")
                two_ser_rub_ot = "0"
            elif two_ser_rub_do == "":
                self.ui.exchangers_rub_do.setText("0")
                two_ser_rub_do = "0"
            elif one_ser_rub_ot == "":
                self.ui.sm_rub_ot.setText("0")
                one_ser_rub_ot = "0"
            elif one_ser_rub_do == "":
                self.ui.sm_rub_do.setText("0")
                one_ser_rub_do = "0"

            one_service = self.ui.one_service.currentText()
            two_service = self.ui.two_service.currentText()

            self.ui.tableView.setStyleSheet("QHeaderView::section { background-color: "
                                            "qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0.96, "
                                            "stop:0 rgba(0, 0, 0, 255), "
                                            "stop:0.948864 rgba(0, 0, 127, 255)); }")

            if one_service == "CSMoney live" and two_service == "Steam":

                self.ui.tableView.horizontalHeader().resizeSection(0, 380)
                self.ui.tableView.horizontalHeader().resizeSection(1, 80)
                self.ui.tableView.horizontalHeader().resizeSection(2, 144)
                self.ui.tableView.horizontalHeader().resizeSection(3, 144)

                rec_com = self.ui.rec_com_steam.text()
                self.ui.groupBox_exchangers_one.setTitle("CSM Live")
                self.ui.groupBox_exchangers_two.setTitle("STEAM")

                sql = (
                    "SELECT i.market_hash_name, i.percent, i.csm_live, i.steam, i.csm_live_count "
                    "FROM (SELECT b.market_hash_name, CONCAT(b.steam_en,' ---- ',b.steam_ru) as steam, "
                    "b.steam_en, b.steam_ru, CONCAT(b.csm_live_en,' ---- ',b.csm_live_ru) as csm_live, "
                    "b.csm_live_en, b.csm_live_ru, "
                    "CASE WHEN b.steam_en = '0' or b.steam_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.steam_en - (b.steam_en * {rec_com}) / 100) "
                    "/ b.csm_live_en * 100) - 100 as numeric), 2) END as percent, "
                    "b.csm_live_count "
                    "FROM items b "
                    "WHERE b.csm_live_en IS NOT NULL and not b.csm_live_en = '0' "
                    "and b.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
                    f"and b.csm_live_ru between {one_ser_rub_ot} and {one_ser_rub_do} "
                    f"and b.steam_ru between {two_ser_rub_ot} and {two_ser_rub_do}) i "
                    "WHERE i.percent > -20"
                )

                columns = ['Наиманование', 'Steam %', 'CSM live', 'Steam', 'Количество']

            elif one_service == "CSMoney live" and two_service == "CSMoney sale":

                self.ui.tableView.horizontalHeader().resizeSection(0, 340)
                self.ui.tableView.horizontalHeader().resizeSection(1, 80)
                self.ui.tableView.horizontalHeader().resizeSection(2, 144)
                self.ui.tableView.horizontalHeader().resizeSection(3, 50)
                self.ui.tableView.horizontalHeader().resizeSection(4, 144)

                rec_com = self.ui.rec_com_csm.text()
                self.ui.groupBox_exchangers_one.setTitle("CSM Live")
                self.ui.groupBox_exchangers_two.setTitle("CSM sale")

                sql = (
                    "SELECT i.market_hash_name, i.percent, i.csm_live, i.csm_live_count, i.csm_sale, i.status "
                    "FROM (SELECT b.market_hash_name, CONCAT(b.csm_live_en,' ---- ',b.csm_live_ru) as csm_live, "
                    "b.csm_live_en, b.csm_live_ru, CONCAT(b.csm_sale_en,' ---- ',b.csm_sale_ru) as csm_sale, "
                    "b.csm_sale_en, b.csm_sale_ru, b.status, "
                    "CASE WHEN b.csm_sale_en = '0' or b.csm_sale_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.csm_sale_en - (b.csm_sale_en * {rec_com}) / 100) "
                    "/ b.csm_live_en * 100) - 100 as numeric), 2) END as percent, "
                    "b.csm_live_count "
                    "FROM items b "
                    "WHERE b.csm_live_en IS NOT NULL and not b.csm_live_en = '0' "
                    "and not b.status SIMILAR TO '(Over|Unav)'"
                    "and b.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
                    f"and b.csm_live_ru between {one_ser_rub_ot} and {one_ser_rub_do} "
                    f"and b.csm_sale_ru between {two_ser_rub_ot} and {two_ser_rub_do}) i "
                    "WHERE i.percent > -7 "
                )

                columns = ['Наиманование', 'CSM sale %', 'CSM live', 'COUNT', 'CSM sale', 'STATUS']

            elif one_service == "CSMoney live" and two_service == "DMarket":

                self.ui.tableView.horizontalHeader().resizeSection(0, 300)
                self.ui.tableView.horizontalHeader().resizeSection(1, 95)
                self.ui.tableView.horizontalHeader().resizeSection(2, 95)
                self.ui.tableView.horizontalHeader().resizeSection(3, 130)
                self.ui.tableView.horizontalHeader().resizeSection(4, 50)
                self.ui.tableView.horizontalHeader().resizeSection(5, 130)
                self.ui.tableView.horizontalHeader().resizeSection(6, 130)

                rec_com = self.ui.rec_com_dm.text()
                self.ui.groupBox_exchangers_one.setTitle("CSM Live")
                self.ui.groupBox_exchangers_two.setTitle("DMarket")

                sql = (
                    "SELECT i.market_hash_name, i.percent_targets, i.percent_offers, i.csm_live, i.csm_live_count, "
                    "i.dm_targets, i.dm_offers "
                    "FROM (SELECT b.market_hash_name, CONCAT(b.csm_live_en,' ---- ',b.csm_live_ru) as csm_live, "
                    "b.csm_live_en, b.csm_live_ru, CONCAT(b.dm_targets_en,' ---- ',b.dm_targets_ru) as dm_targets, "
                    "b.dm_targets_en, b.dm_targets_ru, CONCAT(b.dm_offers_en,' ---- ',b.dm_offers_ru) as dm_offers, "
                    "b.dm_offers_ru, b.dm_offers_en, "
                    "CASE WHEN b.dm_targets_en = '0' or b.dm_targets_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.dm_targets_en - (b.dm_targets_en * {rec_com}) / 100) "
                    "/ b.csm_live_en * 100) - 100 as numeric), 2) END as percent_targets, "
                    "CASE WHEN b.dm_offers_en = '0' or b.dm_offers_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.dm_offers_en - (b.dm_offers_en * {rec_com}) / 100) "
                    "/ b.csm_live_en * 100) - 100 as numeric), 2) END as percent_offers, "
                    "b.csm_live_count "
                    "FROM items b "
                    "WHERE b.csm_live_en IS NOT NULL and not b.csm_live_en = '0' "
                    "and b.dm_targets_en < b.dm_offers_en "
                    "and b.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
                    f"and b.csm_live_ru between {one_ser_rub_ot} and {one_ser_rub_do} "
                    f"and b.dm_offers_ru between {two_ser_rub_ot} and {two_ser_rub_do}) i "
                    "WHERE not i.percent_targets = '0' and i.percent_targets > -44 and i.percent_offers > -44"
                )

                columns = ['Наиманование', 'DM TARGETS %', 'DM OFFERS %', 'CSM live', 'COUNT',
                           'DM TARGETS', 'DM OFFERS']

            elif one_service == "Steam" and two_service == "CSMoney sale":

                self.ui.tableView.horizontalHeader().resizeSection(0, 380)
                self.ui.tableView.horizontalHeader().resizeSection(1, 80)
                self.ui.tableView.horizontalHeader().resizeSection(2, 144)
                self.ui.tableView.horizontalHeader().resizeSection(3, 144)

                rec_com = self.ui.rec_com_csm.text()
                self.ui.groupBox_exchangers_one.setTitle("STEAM")
                self.ui.groupBox_exchangers_two.setTitle("CSM sale")

                sql = (
                    "SELECT i.market_hash_name, i.percent, i.steam, i.csm_sale, i.status "
                    "FROM (SELECT b.market_hash_name, b.steam_en, "
                    "b.steam_ru, CONCAT(b.steam_en,' ---- ',b.steam_ru) as steam, "
                    "b.csm_sale_en, b.csm_sale_ru, CONCAT(b.csm_sale_en,' ---- ',b.csm_sale_ru) as csm_sale, b.status, "
                    "CASE WHEN b.csm_sale_en = '0' or b.csm_sale_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.csm_sale_en - (b.csm_sale_en * {rec_com}) / 100) "
                    "/ b.steam_en * 100) - 100 as numeric), 2) END as percent "
                    "FROM items b "
                    "WHERE b.steam_en IS NOT NULL and not b.steam_en = '0' "
                    "and not b.status SIMILAR TO '(Over|Unav)'"
                    "and b.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)'  "
                    f"and b.steam_ru between {one_ser_rub_ot} and {one_ser_rub_do} "
                    f"and b.csm_sale_ru between {two_ser_rub_ot} and {two_ser_rub_do}) i "
                    "WHERE i.percent > 25 "
                )

                columns = ['Наиманование', 'CSM sale %', 'Steam', 'CSM sale', 'STATUS']

            elif (one_service == "Steam" and two_service == "DMarket") \
                    or (one_service == "DMarket" and two_service == "Steam"):

                self.ui.tableView.horizontalHeader().resizeSection(0, 320)
                self.ui.tableView.horizontalHeader().resizeSection(1, 95)
                self.ui.tableView.horizontalHeader().resizeSection(2, 95)
                self.ui.tableView.horizontalHeader().resizeSection(3, 130)
                self.ui.tableView.horizontalHeader().resizeSection(4, 130)
                self.ui.tableView.horizontalHeader().resizeSection(5, 130)

                rec_com = self.ui.rec_com_dm.text()

                if one_service == "Steam" and two_service == "DMarket":
                    self.ui.groupBox_exchangers_one.setTitle("STEAM")
                    self.ui.groupBox_exchangers_two.setTitle("DMarket")

                    one_targets = 'dm_targets_en'
                    two_targets = 'steam_en'
                    one_offers = 'dm_offers_en'
                    two_offers = 'steam_en'
                    i = "i.market_hash_name, i.percent_targets, i.percent_offers, i.steam, i.dm_targets, i.dm_offers"
                    one_ser_range = 'steam_ru'
                    two_ser_range = 'dm_targets_ru'

                    columns = ['Наиманование', 'DM TARGETS %', 'DM OFFERS %', 'Steam', 'DM TARGETS', 'DM OFFERS']

                else:
                    self.ui.groupBox_exchangers_one.setTitle("DMarket")
                    self.ui.groupBox_exchangers_two.setTitle("STEAM")

                    one_targets = 'steam_en'
                    two_targets = 'dm_targets_en'
                    one_offers = 'steam_en'
                    two_offers = 'dm_offers_en'
                    i = "i.market_hash_name, i.percent_targets, i.percent_offers, i.dm_targets, i.dm_offers, i.steam"
                    one_ser_range = 'dm_targets_ru'
                    two_ser_range = 'steam_ru'

                    columns = ['Наиманование', 'DM TARGETS %', 'DM OFFERS %', 'DM TARGETS', 'DM OFFERS', 'Steam']

                sql = (
                    f"SELECT {i} "
                    "FROM (SELECT b.market_hash_name, b.steam_en, b.steam_ru, "
                    "CONCAT(b.steam_en,' ---- ',b.steam_ru) as steam, b.dm_targets_en, b.dm_targets_ru, "
                    "CONCAT(b.dm_targets_en,' ---- ',b.dm_targets_ru) as dm_targets, b.dm_offers_en, b.dm_offers_ru, "
                    "CONCAT(b.dm_offers_en,' ---- ',b.dm_offers_ru) as dm_offers, "
                    f"CASE WHEN b.dm_targets_en = '0' or b.dm_targets_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.{one_targets} - (b.{one_targets} * {rec_com}) / 100) "
                    f"/ b.{two_targets} * 100) - 100 as numeric), 2) END as percent_targets, "
                    f"CASE WHEN b.dm_offers_en = '0' or b.dm_offers_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.{one_offers} - (b.{one_offers} * {rec_com}) / 100) "
                    f"/ b.{two_offers} * 100) - 100 as numeric), 2) END as percent_offers "
                    "FROM items b "
                    "WHERE b.steam_en IS NOT NULL and not b.steam_en = '0' "
                    "and b.dm_targets_en < b.dm_offers_en "
                    "and b.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
                    f"and b.{one_ser_range} between {one_ser_rub_ot} and {one_ser_rub_do} "
                    f"and b.{two_ser_range} between {two_ser_rub_ot} and {two_ser_rub_do}) i "
                    "WHERE i.percent_targets > -20 and i.percent_offers > -20 and "
                    "i.percent_targets != 0 "
                )

            elif one_service == "DMarket" and two_service == "CSMoney sale":

                self.ui.tableView.horizontalHeader().resizeSection(0, 280)
                self.ui.tableView.horizontalHeader().resizeSection(1, 95)
                self.ui.tableView.horizontalHeader().resizeSection(2, 95)
                self.ui.tableView.horizontalHeader().resizeSection(3, 130)
                self.ui.tableView.horizontalHeader().resizeSection(4, 130)
                self.ui.tableView.horizontalHeader().resizeSection(5, 130)
                self.ui.tableView.horizontalHeader().resizeSection(6, 60)

                rec_com = self.ui.rec_com_csm.text()
                self.ui.groupBox_exchangers_one.setTitle("DMarket")
                self.ui.groupBox_exchangers_two.setTitle("CSM sale")

                sql = (
                    "SELECT i.market_hash_name, i.percent_targets, i.percent_offers, i.dm_targets, i.dm_offers, "
                    "i.csm_sale, i.status "
                    "FROM (SELECT market_hash_name, "
                    "CASE WHEN b.dm_targets_en = '0' or b.dm_targets_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.csm_sale_en - (b.csm_sale_en * {rec_com}) / 100) "
                    "/ b.dm_offers_en * 100) - 100 as numeric), 2) END as percent_targets, "
                    "CASE WHEN b.dm_offers_en = '0' or b.dm_offers_en IS NULL THEN 0 ELSE "
                    f"ROUND( CAST(float8 ((b.csm_sale_en - (b.csm_sale_en * {rec_com}) / 100) "
                    "/ b.dm_targets_en * 100) - 100 as numeric), 2) END as percent_offers, "
                    "b.dm_targets_en, b.dm_targets_ru, "
                    "CONCAT(b.dm_targets_en,' ---- ',b.dm_targets_ru) as dm_targets, b.dm_offers_en, b.dm_offers_ru, "
                    "CONCAT(b.dm_offers_en,' ---- ',b.dm_offers_ru) as dm_offers, "
                    "CONCAT(b.csm_sale_en,' ---- ',b.csm_sale_ru) as csm_sale, b.status "
                    "FROM items b "
                    "WHERE b.csm_sale_en IS NOT NULL and not b.csm_sale_en = '0' "
                    "and b.dm_targets_en < b.dm_offers_en "
                    "and not b.status SIMILAR TO '(Over|Unav)'"
                    "and b.market_hash_name not SIMILAR TO "
                    "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
                    f"and b.dm_offers_ru between {one_ser_rub_ot} and {one_ser_rub_do} "
                    f"and b.csm_sale_ru between {two_ser_rub_ot} and {two_ser_rub_do}) i "
                    "WHERE i.percent_targets > 0 and i.percent_offers > 0 "
                )

                columns = ['Наиманование', 'DM TARGETS %', 'DM OFFERS %', 'DM TARGETS', 'DM OFFERS',
                           'CSM sale', 'STATUS']

            else:
                rec_com = self.ui.rec_com_steam.text()
                self.ui.groupBox_exchangers_one.setTitle("DMarket")
                self.ui.groupBox_exchangers_two.setTitle("STEAM")

                sql = ("SELECT i.* "
                       "FROM (SELECT market_hash_name, steam_en, steam_ru, csm_sale_en, "
                       "csm_sale_ru, status, csm_live_en, csm_live_ru, dm_offers_en, "
                       "dm_offers_ru, dm_targets_en, dm_targets_ru, "
                       f"ROUND( CAST(float8 ((steam_en - (steam_en * {rec_com}) / 100) "
                       f"/ steam_en * 100) - 100 as numeric), 2) as percent, "
                       "csm_live_count "
                       "FROM items) i "
                       "WHERE i.steam_en IS NOT NULL "
                       "and not i.steam_en = '0' "
                       "and i.market_hash_name not SIMILAR TO "
                       "'(%Capsule%|Sealed Graffiti%|%Pin|%Music Kit%|%Patch%|%Package%|%Case Key%|%Case)' "
                       f"and i.steam_en between {one_ser_rub_ot} and {one_ser_rub_do} "
                       f"and i.steam_en between {two_ser_rub_ot} and {two_ser_rub_do} "
                       )

                columns = ['Наиманование', 'Steam EN', 'Steam RU', 'CSM sale EN',
                           'CSM sale RU', 'STATUS', 'CSM live EN', 'CSM live RU',
                           'DM OFS EN', 'DM OFFS RU', 'DM TARS EN', 'DM TARS RU',
                           '%', 'Количество']

            cursor.execute(sql)
            result_csm = cursor.fetchall()

            labels = pd.DataFrame(result_csm, columns=columns)

            model = PandasTableModel(labels)
            self.ui.tableView.setModel(model)
            self.ui.tableView.setSortingEnabled(True)

        except Exception as ex:
            print("Ошибка таблицы", ex)
            self.TradeCSMLIVE.stop()
            sys.exit(-1)

    def negative_red(self, x):
        color = 'red' if x < 0 else 'green'
        return 'color: %s' % color

    def on_Click(self):
        index = (self.ui.tableView.selectionModel().currentIndex())
        value = index.sibling(index.row(), index.column()).data()
        pyperclip.copy(value)

    def closeEvent(self, event):
        connection.autocommit = True
        cursor = connection.cursor()
        self.save_Setting()
        query_table_basic = (
            f"UPDATE items SET csm_live_en = NULL, csm_live_ru = NULL, csm_live_count = 0, "
            f"csm_live_datetime = NULL "
        )
        cursor.execute(query_table_basic)

        if self.TradeCSMSale.isRunning():
            self.TradeCSMSale.stop()

        if self.TradeSteam.isRunning():
            self.TradeSteam.stop_driver()

        if self.TradeDMarket.isRunning():
            self.TradeDMarket.stop()

        self.TradeCSMLIVE.stop()

        self.Item_NameID.stop()
        self.Item_NameID.stop_driver()

        # super().closeEvent(event)
        # sys.exit(0)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    QtCore.QCoreApplication.setOrganizationName("Endresk")
    QtCore.QCoreApplication.setOrganizationDomain("endresk.com")
    QtCore.QCoreApplication.setApplicationName("Trade")
    ui = MyWindow()
    ui.show()
    sys.exit(app.exec())
