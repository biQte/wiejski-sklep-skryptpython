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

for item in items:
    if item['TYP_KURIERA'] == '2B.PACZKOMAT-B' or item['TYP_KURIERA'] == '2C.PACZKOMAT-C' or item['TYP_KURIERA'] == '2A.PACZKOMAT-A':
        continue
    if item['EAN'] == '':
        continue
    product_id = "SELECT id_product FROM pr_product WHERE ean13 = %s;"
    ean13 = (item['EAN'])
    cursor = conn.cursor()
    cursor.execute(product_id, (ean13,))
    result_product_id = cursor.fetchall()
    if result_product_id is None:
        continue
    carrier_type = "SELECT pr_carrier.id_carrier_type FROM pr_carrier JOIN pr_carrier_type ON pr_carrier_type.id_carrier_type = pr_carrier.id_carrier_type WHERE pr_carrier_type.typ_kuriera = %s;"
    carrier = (item['TYP_KURIERA'])
    cursor.execute(carrier_type, (carrier,))
    result_carrier_types = cursor.fetchall()
    if result_carrier_types is None:
        continue
    for result_carrier_type in result_carrier_types:
        if int((str(result_carrier_type))[1:len(str(result_carrier_type)) - 2]) == 7:
            continue
        if str(result_product_id) == "[]":
            continue
        check_if_record_exist = "SELECT id_product FROM pr_product_carrier WHERE id_product = %s;"
        values_for_check = (str(result_product_id)[
                            2:len(str(result_product_id)) - 3],)
        cursor.execute(check_if_record_exist, values_for_check)
        exists = cursor.fetchall()
        if exists == result_product_id:
            print("Record exist")
            check_if_record_needs_update = "SELECT id_carrier_type FROM pr_product_carrier WHERE id_product = %s;"
            check_if_record_needs_update_values = (
                str(result_product_id)[2:len(str(result_product_id)) - 3],)
            cursor.execute(check_if_record_needs_update,
                           check_if_record_needs_update_values)
            needs_update = cursor.fetchall()
            if int(str(needs_update)[2:len(str(needs_update)) - 3]) != int(str(result_carrier_type)[
                    1:len(str(result_carrier_type)) - 2]):
                print("and needs update")
                update = "UPDATE pr_product_carrier SET id_carrier_type = %s WHERE id_product = %s;"
                update_values = (str(result_carrier_type)[1:len(str(
                    result_carrier_type)) - 2], str(result_product_id)[2:len(str(result_product_id)) - 3])
                cursor.execute(update, update_values)
                conn.commit()
                print("Record updated")
                continue
            print("and doesn't need update")
            continue
        sql = "INSERT INTO pr_product_carrier (id_product,id_carrier_type,id_shop) VALUES (%s, %s, 1);"
        values = (str(result_product_id)[
                  2:len(str(result_product_id)) - 3], str(result_carrier_type)[1:len(str(result_carrier_type)) - 2])
        cursor.execute(sql, values)
        conn.commit()
        row += 1
print(row, "records inserted")

conn.disconnect()
