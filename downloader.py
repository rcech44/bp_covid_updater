from time import sleep
import json
from urllib.request import urlopen
import urllib.parse
import mysql.connector
from datetime import datetime, timedelta

url_summary = 'https://onemocneni-aktualne.mzcr.cz/api/v3/zakladni-prehled?page=1&itemsPerPage=100&apiToken=c54d8c7d54a31d016d8f3c156b98682a'
url_reinfection = 'https://onemocneni-aktualne.mzcr.cz/api/v3/prehled-reinfekce/XYZ?apiToken=c54d8c7d54a31d016d8f3c156b98682a'

def downloader():
    req = urllib.request.Request(url_summary)
    req.add_header('accept', 'application/json')

    while True:
        datum_string_now = datetime.now().strftime("%Y-%m-%d")
        datum_string_yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            with mysql.connector.connect(host="remotemysql.com", user="9qMwE320zO", password="gmnNuBYtIX", database="9qMwE320zO") as conn:
                cur = conn.cursor(buffered=True)
                cur.execute('SELECT * FROM covid_summary WHERE datum = %s', [datum_string_now])
                response = cur.fetchone()
                if response is None:
                    response = urllib.request.urlopen(req)
                    d = json.load(response)[0]
                    if d['datum'] == datum_string_now:
                        current_url = url_reinfection.replace('XYZ', datum_string_yesterday)
                        req2 = urllib.request.Request(current_url)
                        req2.add_header('accept', 'application/json')
                        try:
                            response2 = urllib.request.urlopen(req2)
                            reinfekce = json.load(response2)['60_dnu']
                        except:
                            print(f"[DATABASE] Tried to update database but still not reinfection data available - {datetime.now()}")
                            sleep(300)
                            continue

                        cur.execute('INSERT INTO covid_summary ' \
                            '(datum, ' \
                            'aktivni_pripady, ' \
                            'aktualne_hospitalizovani, ' \
                            'ockovane_osoby_celkem, ' \
                            'ockovane_osoby_vcerejsi_den, ' \
                            'ockovane_osoby_vcerejsi_den_datum, ' \
                            'potvrzene_pripady_65_celkem, ' \
                            'potvrzene_pripady_65_vcerejsi_den, ' \
                            'potvrzene_pripady_65_vcerejsi_den_datum, ' \
                            'potvrzene_pripady_celkem, ' \
                            'potvrzene_pripady_vcerejsi_den, ' \
                            'potvrzene_pripady_vcerejsi_den_datum, ' \
                            'provedene_antigenni_testy_celkem, ' \
                            'provedene_antigenni_testy_vcerejsi_den, ' \
                            'provedene_antigenni_testy_vcerejsi_den_datum, ' \
                            'provedene_testy_celkem, ' \
                            'provedene_testy_vcerejsi_den, ' \
                            'provedene_testy_vcerejsi_den_datum, ' \
                            'umrti, ' \
                            'vykazana_ockovani_celkem, ' \
                            'vykazana_ockovani_vcerejsi_den, ' \
                            'vykazana_ockovani_vcerejsi_den_datum, ' \
                            'vyleceni, ' \
                            'reinfekce) VALUES ' \
                            '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', \
                            [
                                d['datum'],
                                d['aktivni_pripady'],
                                d['aktualne_hospitalizovani'],
                                d['ockovane_osoby_celkem'],
                                d['ockovane_osoby_vcerejsi_den'],
                                d['ockovane_osoby_vcerejsi_den_datum'],
                                d['potvrzene_pripady_65_celkem'],
                                d['potvrzene_pripady_65_vcerejsi_den'],
                                d['potvrzene_pripady_65_vcerejsi_den_datum'],
                                d['potvrzene_pripady_celkem'],
                                d['potvrzene_pripady_vcerejsi_den'],
                                d['potvrzene_pripady_vcerejsi_den_datum'],
                                d['provedene_antigenni_testy_celkem'],
                                d['provedene_antigenni_testy_vcerejsi_den'],
                                d['provedene_antigenni_testy_vcerejsi_den_datum'],
                                d['provedene_testy_celkem'],
                                d['provedene_testy_vcerejsi_den'],
                                d['provedene_testy_vcerejsi_den_datum'],
                                d['umrti'],
                                d['vykazana_ockovani_celkem'],
                                d['vykazana_ockovani_vcerejsi_den'],
                                d['vykazana_ockovani_vcerejsi_den_datum'],
                                d['vyleceni'],
                                reinfekce
                            ])
                        conn.commit()
                        print(f"[DATABASE] Database updated with new values - {datetime.now()}")
                    else:
                        print(f"[DATABASE] Expecting new values but still not available - {datetime.now()}")
                else:
                    print(f"[DATABASE] Up to date - {datetime.now()}")
        except mysql.connector.Error as e:
            print(e)
        
        sleep(300)

downloader()

# def temp():
#     try:
#         with mysql.connector.connect(host="remotemysql.com", user="9qMwE320zO", password="gmnNuBYtIX", database="9qMwE320zO") as conn:
#             cur = conn.cursor(buffered=True)
#             cur.execute('SELECT id, datum FROM covid_summary ORDER BY id')
#             response = cur.fetchall()
#             if response is not None:
#                 for row in response:
#                     datum = row[1]
#                     current_url = url_reinfection.replace('XYZ', datum)
#                     req = urllib.request.Request(current_url)
#                     req.add_header('accept', 'application/json')
#                     response = urllib.request.urlopen(req)
#                     d = json.load(response)
#                     print(d)
#     except mysql.connector.Error as e:
#         print(e)