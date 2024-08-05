import requests
import xml.etree.ElementTree as ET
import pendulum
import psycopg2
from datetime import datetime
import json

def eqk_function():
    now = pendulum.now('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss')
    
    conn = psycopg2.connect(
        dbname='okky1_db',
        user='okky1',
        password='Teamokky1',
        host='team-okky-1-redshift-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com',
        port='5439'
    )
    cur = conn.cursor()
    
    url = 'https://apihub.kma.go.kr/api/typ09/url/eqk/urlNewNotiEqk.do'
    key = 'KowdqwCsSM-MHasArOjPyQ'
    params = {
        'frDate' : '20240101',
        'laDate' : '20241231',
        'msgCode' : 102,
        'cntDiv' : 'Y',
        'nkDiv' : 'N',
        'orderTy' : 'xml',
        'authKey': key
    }

    res = requests.get(url, params=params)
    
    equake_items = []

    if res.status_code == 200:
        temp = ET.fromstring(res.text)
        info = temp.findall('.//info')
        cnt = len(info)

        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET EQUAKE = {cnt}, updated_at = '{now}'
            WHERE YEAR = '2024'
        """)
    
        for item in info:
            eqPt = item.find('eqPt').text if item.find('eqPt') is not None else None
            eqLt = item.find('eqLt').text if item.find('eqLt') is not None else None
            eqLn = item.find('eqLn').text if item.find('eqLn') is not None else None
            magMl = item.find('magMl').text if item.find('magMl') is not None else None
            jdLoc = item.find('jdLoc').text if item.find('jdLoc') is not None else None
            eqDate = item.find('eqDate').text if item.find('eqDate') is not None else None
            tmIssue = item.find('tmIssue').text if item.find('tmIssue') is not None else None

            # Convert to appropriate types
            eqLt = float(eqLt) if eqLt is not None else None
            eqLn = float(eqLn) if eqLn is not None else None
            magMl = float(magMl) if magMl is not None else None
            eqDate = datetime.strptime(eqDate, '%Y%m%d%H%M%S') if eqDate is not None else None
            created_at = datetime.strptime(tmIssue, '%Y%m%d%H%M%S') if tmIssue is not None else None
            
            equake_items.append((
                eqPt, eqLt, eqLn, magMl, jdLoc, eqDate, now, created_at, now
            ))

    cur.execute("DROP TABLE IF EXISTS mart_data.equake2024")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart_data.equake2024 (
            eqPt VARCHAR(64),
            eqLt FLOAT,
            eqLn FLOAT,
            magMl FLOAT,
            jdLoc VARCHAR(8),
            eqDate TIMESTAMP,
            data_key TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)
    
    if equake_items:
        cur.executemany("""
            INSERT INTO mart_data.equake2024 (eqPt, eqLt, eqLn, magMl, jdLoc, eqDate, data_key, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, equake_items)

    conn.commit()
    cur.close()
    conn.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success eqk_function'),
    }
