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
    if item['EAN'] == '':
        continue
    # Search for product ID that is associated with specified EAN13 number
    product_id = "SELECT id_product FROM pr_product WHERE ean13 = %s;"
    ean13 = (item['EAN'])
    cursor.execute(product_id, (ean13,))
    result_product_ids = cursor.fetchall()
    if result_product_ids is None:
        continue
    for result_product_id in result_product_ids:
        # Search for carrier IDs based on carrier name from XML
        carrier_type = "SELECT pr_carrier.id_carrier_type FROM pr_carrier JOIN pr_carrier_type ON pr_carrier_type.id_carrier_type = pr_carrier.id_carrier_type WHERE pr_carrier_type.typ_kuriera = %s;"
        carrier = (item['TYP_KURIERA'])
        cursor.execute(carrier_type, (carrier,))
        result_carrier_types = cursor.fetchall()
        # Search for current carrier for specified product
        current_product_carriers = "SELECT pr_carrier.id_carrier_type FROM pr_carrier_type JOIN pr_carrier ON pr_carrier_type.id_carrier_type = pr_carrier.id_carrier_type JOIN pr_product_carrier ON pr_carrier_type.id_carrier_type = pr_product_carrier.id_carrier_type WHERE pr_product_carrier.id_product = %s;"
        cursor.execute(current_product_carriers, result_product_id)
        current_carrier_types = cursor.fetchall()
        # Parsing data
        result_product_id = str(result_product_id)[
            1:len(str(result_product_id)) - 2]
        if result_product_id == "[]":
            continue
        if result_carrier_types is None:
            continue
        for result_carrier_type in result_carrier_types:
            # Check if product already has that type of carrier to avoid repeating data
            for current_carrier_type in current_carrier_types:
                if int(str(result_carrier_type)[1:len(str(result_carrier_type)) - 2]) == int(str(current_carrier_type)[1:len(str(current_carrier_type)) - 2]):
                    print("rekord skipped")
                    continue
                else:
                    if int(str(result_carrier_type)[1:len(str(result_carrier_type)) - 2]) == 7:
                        continue
                    sql = "INSERT INTO pr_product_carrier (id_product,id_carrier_type,id_shop) VALUES (%s, %s, 1);"
                    values = (result_product_id, str(result_carrier_type)
                              [1:len(str(result_carrier_type)) - 2])
                    cursor.execute(sql, values)
                    conn.commit()
                    print("Row inserted")
                    row += 1
        carriers_in_database_querry = "SELECT pr_carrier_type.typ_kuriera FROM pr_carrier_type JOIN pr_product_carrier ON pr_carrier_type.id_carrier_type = pr_product_carrier.id_carrier_type WHERE pr_product_carrier.id_product = %s;"
        cursor.execute(carriers_in_database_querry, (result_product_id,))
        carriers_in_database = cursor.fetchall()
        carrier_names = item["TYP_KURIERA"]
print(row + " rows inserted")

conn.disconnect()
