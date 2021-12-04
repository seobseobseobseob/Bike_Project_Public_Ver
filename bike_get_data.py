from urllib.request import urlopen
import pandas as pd
import json
import datetime
import time
import pytz
import pymysql

# 서울 시간 설정
KST = pytz.timezone('Asia/Seoul')
certified_keys = 

# aws rds 연결
def connetion():
    global db
    global cursor
    db = pymysql.connect(host = ,
                        port = ,
                        user = ,
                        passwd = ,
                        db = ,
                        charset = 'utf8')
    cursor = db.cursor(pymysql.cursors.DictCursor)


def getdata(now,certified_keys):
    # API에 대한 응답이 빈 경우가 있어서, 빈 값이 온 경우 다시 API를 날리도록 만듦.
    global result
    result=[]
    while True:
        try:
            # 따릉이 정류소가 계속 생겨서 10000개까지 API를 날려보게 함.
            # 에러 메시지(정류소index가 없는 경우)가 나오면 pass하게 만듦.
            for i in range(1,3000, 1000): 
                url = f'http://openapi.seoul.go.kr:8088/{certified_keys}/json/bikeList/{i}/{i+999}/'
                response = urlopen(url)
                results = response.read().decode("utf-8")
                result = result + json.loads(results)['rentBikeStatus']['row']
            # break전 부분에서 에러가 나기 때문에, break까지 갔다면 문제없이 데이터를 받아왔기 때문에 while문을 멈춤
            break
        except:
            # 위의 try에서 문제가 생겼다면, except으로 오는데 여기서 continue를 만나면 다시 while의 첫 부분, try로 가게 되어
            # 다시 API를 날리게 됨.
            continue
    
    # 위에서 받은 데이터를 필요한 정보만 추출 및 이중 리스트 형태로 변환
    # executemany를 사용하기 위함
    result=pd.DataFrame(data = result)
    result = result[['rackTotCnt', 'parkingBikeTotCnt', 'shared', 'stationId']]
    total_list = []
    for i in range(len(result)):
            total_list = total_list + [list(result.iloc[i,:])]
            total_list[i].insert(0,now)
            total_list[i].append(now.date())
            total_list[i].append(now.time())
            
    # db에 올리는 과정에서 에러가 나는 것을 방지
    while True:
        try:
            connetion()
            # rds에 입력할 쿼리
            sql = "INSERT INTO bike_raw_table(datetime, rackTotCnt, parkingBikeTotCnt,shared,stationId, date, time) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            # 위의 쿼리를 한 정류장당 한번씩 올려두고 commit을 할 때 한번에 Table 업데이트.
            # 이 중에서 정류장 위도와 경도, 한글 이름의 경우, 매 5분마다 중복되는 정보가 저장됨으로 따로 저장.
            cursor.executemany(sql, total_list)
            db.commit()
            db.close()
            break
        except:
            db.close()
            continue




# 무한 반복문으로 ec2에 실행할 예정
while True:
      # 시간을 계속 보다가, 분이 0이 되면 api 호출 + 데이터 rds에 저장
    if datetime.datetime.now(KST).minute == 0:
        getdata(datetime.datetime.now(KST), certified_keys)
        # 너무 빨리 호출 및 데이터 저장이 이루어져, 같은 데이터가 2번씩 올라가는 경우가 있어
        # 이를 방지하기 위해 2분 동안 동작을 멈춰놓음
        time.sleep(120)