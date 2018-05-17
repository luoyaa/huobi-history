from websocket import create_connection
import gzip
import time
import json
import csv
import pymysql

global globalTime
# 获取历史数据起始时间
globalTime = 1526392680
#1506787200



def loop_data(o, k=''):
    global json_ob, c_line
    if isinstance(o, dict):
        for key, value in o.items():
            if (k == ''):
                loop_data(value, key)
            else:
                loop_data(value, k + '.' + key)
    elif isinstance(o, list):
        for ov in o:
            loop_data(ov, k)
    else:
        if not k in json_ob:
            json_ob[k] = {}
        json_ob[k][c_line] = o


def get_title_rows(json_ob):
    title = []
    row_num = 0
    # global rows
    rows = []
    for key in json_ob:
        title.append(key)
        v = json_ob[key]
        if len(v) > row_num:
            row_num = len(v)
        continue
    for i in range(row_num):
        row = {}
        for k in json_ob:
            v = json_ob[k]
            if i in v.keys():
                row[k] = v[i]
            else:
                row[k] = ''
        rows.append(row)
    for dbRow in rows:
        sql = """insert into zecusdt_min(id,open,close,low,high,amount,vol,count) VALUES (""" + str(
            dbRow.get('id')) + """,""" + str(dbRow.get('open')) + """,""" + str(dbRow.get('close')) + """,""" + str(
            dbRow.get('low')) + """,""" + str(dbRow.get('high')) + """,""" + str(
            dbRow.get('amount')) + """,""" + str(dbRow.get('vol')) + """,""" + str(dbRow.get('count')) + """)"""
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
    return title, rows


def write_csv(title, rows, csv_file_name):
    with open(csv_file_name, 'a+', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=title)
        writer.writerows(rows)


def json_to_csv(object_list):
    global json_ob, c_line
    json_ob = {}
    c_line = 0
    for ov in object_list:
        loop_data(ov)
        c_line += 1
    title, rows = get_title_rows(json_ob)


global count
count = 0
global x
x = globalTime
if __name__ == '__main__':
    while (1):
        try:
            ws = create_connection("wss://api.huobi.pro/ws")
            # ws = create_connection("wss://api.huobipro.com/ws")

            while (1):
                globalTime += 18000
                tradeStr = """{"req": "market.zecusdt.kline.1min","id": "id10", "from": """ + str(
                    globalTime) + """, "to":""" + str(globalTime + 18000) + """ }"""
                ws.send(tradeStr)
                compressData = ws.recv()
                if compressData != '':
                    result = gzip.decompress(compressData).decode('utf-8')
                else:
                    print("正在重新连接")
                    ws.connect("wss://api.huobi.pro/ws")
                    # ws.connect("wss://api.huobipro.com/ws")

                    globalTime = x + (18000 * (count + 1))
                    tradeStr2 = """{"req": "market.zecusdt.kline.1min","id": "id10", "from": """ + str(
                        globalTime) + """, "to":""" + str((globalTime + 18000)) + """ }"""
                    ws.send(tradeStr2)
                    compressData = ws.recv()
                    result = gzip.decompress(compressData).decode('utf-8')

                if result[:7] == '{"ping"':
                    ts = result[8:21]
                    pong = '{"pong":' + ts + '}'
                    ws.send(pong)
                else:
                    resutlJson = json.loads(result)
                    data = resutlJson['data']
                    db = pymysql.connect("192.168.1.167", "root", "123", "test3")
                    cursor = db.cursor()
                    json_to_csv(data)
                    count += 1
                    db.close()
                    print(data)
        except:
            print('connect ws error,retry...')
            break
