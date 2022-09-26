import re
from time import sleep
import urllib3
import xmltodict
import mysql.connector

url = "http://img.unimet.pl/kurier.xml"
http = urllib3.PoolManager()
response = http.request('GET', url)
data = xmltodict.parse(response.data)
items = data['items']['item']

conn = mysql.connector.connect(
    user='muoyogiduw_pr1', password='R*mr0Y)acSvMBt[(kL.65##6', host='s183.cyber-folks.pl', database='muoyogiduw_pr1')

if conn:
    print("Connected successfully")
else:
    print("Connection not estabilished")

row = 0

cursor = conn.cursor()

for item in items:
    # if item['TYP_KURIERA'] == '2B.PACZKOMAT-B' or item['TYP_KURIERA'] == '2C.PACZKOMAT-C' or item['TYP_KURIERA'] == '2A.PACZKOMAT-A' or item['TYP_KURIERA] == '1. KOPERTA':
    #     continue
    if item['EAN'] == '':
        continue
    product_id = "SELECT id_product FROM pr_product WHERE ean13 = %s;"
    ean13 = (item['EAN'])
    cursor.execute(product_id, (ean13,))
    result_product_ids = cursor.fetchall()
    if result_product_ids is None:
        continue
    for result_product_id in result_product_ids:
        carrier_type = "SELECT pr_carrier.id_carrier_type FROM pr_carrier JOIN pr_carrier_type ON pr_carrier_type.id_carrier_type = pr_carrier.id_carrier_type WHERE pr_carrier_type.typ_kuriera = %s;"
        carrier = (item['TYP_KURIERA'])
        cursor.execute(carrier_type, (carrier,))
        result_carrier_types = cursor.fetchall()
        current_product_carriers = "SELECT pr_carrier.id_carrier_type FROM pr_carrier_type JOIN pr_carrier ON pr_carrier_type.id_carrier_type = pr_carrier.id_carrier_type JOIN pr_product_carrier ON pr_carrier_type.id_carrier_type = pr_product_carrier.id_carrier_type WHERE pr_product_carrier.id_product = %s;"
        cursor.execute(current_product_carriers, result_product_id)
        current_carrier_types = cursor.fetchall()
        # print(current_carrier_types)
        result_product_id = str(result_product_id)[
            1:len(str(result_product_id)) - 2]
        if result_product_id == "[]":
            continue
        if result_carrier_types is None:
            continue
        for result_carrier_type in result_carrier_types:
            for current_carrier_type in current_carrier_types:
                print(result_carrier_type)
                print(current_carrier_type)
                if int(str(result_carrier_type)[1:len(str(result_carrier_type)) - 2]) == int(str(current_carrier_type)[1:len(str(current_carrier_type)) - 2]):
                    print("rekord skipped")
                    continue
                else:
                    if int(str(result_carrier_type)[1:len(str(result_carrier_type)) - 2]) == 7:
                        continue
                    print(result_product_id)
                    sql = "INSERT INTO pr_product_carrier (id_product,id_carrier_type,id_shop) VALUES (%s, %s, 1);"
                    values = (result_product_id, str(result_carrier_type)
                              [1:len(str(result_carrier_type)) - 2])
                    cursor.execute(sql, values)
                    conn.commit()
                    print("Row inserted")
            print(item['TYP_KURIERA'])
print("Rows inserted")

conn.disconnect()
