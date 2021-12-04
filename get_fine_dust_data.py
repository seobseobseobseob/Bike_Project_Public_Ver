from urllib.request import urlopen
import pandas as pd
import json
import datetime
import time
import pytz
import pymysql
from urllib.parse import quote 
import numpy as np

# aws rds 연결
def connection():
    global db
    global cursor
    db = pymysql.connect(host = ,
                        port = ,
                        user = ,
                        passwd = ,
                        db = ,
                        charset = 'utf8')
    cursor = db.cursor(pymysql.cursors.DictCursor)

# 한국 시간 설정
KST = pytz.timezone('Asia/Seoul')

# 인증키 설정
certified_keys = 

# 아스키코드로 바꿔줌
city = quote('서울')

# 수치형 데이터 컬럼
numerical_columns = ['pm25Value', 'pm25Value24','pm10Value', 'pm10Value24'] 
# 카테고리형 데이터 컬럼
categorical_columns = ['pm25Grade1h', 'pm25Grade','pm10Grade1h', 'pm10Grade']

def get_fine_dust_data(time, certified_keys, city):
  # open api에서 데이터 가져오기
  # ver = 1.3
  # api로 정보는 가져오는 과정에서 에러가 나면 다시 가져오게 함
    while True:
        try:
            url = f'''http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?sidoName={city}&pageNo=1&numOfRows=100&returnType=json&serviceKey={certified_keys}&ver=1.3'''
            response = urlopen(url)
            results = response.read().decode("utf-8")
            result = json.loads(results)
            break
        except:
            continue
              
    # 데이터 프레임으로 변환
    data = pd.DataFrame(data = result['response']['body']['items'])
     
    # 서울시 구별로, 미세먼지에 관한 정보만 추출
    data = data[data['mangName'] =='도시대기'][['stationName','pm25Value','pm25Value24','pm25Grade1h','pm25Grade','pm10Value','pm10Value24','pm10Grade1h','pm10Grade']]
     
    # 인덱스를 초기화
    data = data.reset_index(drop = True)
    
    # 결측치는 '-'와 NonE으로 표시되어 np.NaN으로 통합하여 기입
    data = data.replace("-",np.NaN)
    data = data.fillna(np.NaN)
    
    # 결측치를 제외한 값들은 int로 변환(원래는 str으로 존재)
    for i in data.columns[1:]:
        for j in data[data[i].notnull()][i].index:
            data.loc[j,i]=int(data.loc[j,i])

    # 결측치를 그대로 db에 올리기 위해 none으로 변경
    # nan은 에러남
    data=data.where(pd.notnull(data),None)

    # 수치형 데이터의 결측치 - 평균으로 대체
    # 카테고리형 데이터의 결측치 - 최빈값으로 대체
    # 비가 오는 날에는 모든 측정소의 측정값이 nan으로 오기 때문에 조건문 처리
    for i in data.columns[1:]:
        if i in numerical_columns:
            if data[i].isnull().sum() < 25:
                data[i]=data[i].fillna(data[data[i].notnull()][i].mean())
            else:
                pass
        elif i in categorical_columns:
            if data[i].isnull().sum() < 25:
                data[i]=data[i].fillna(data[data[i].notnull()][i].mode()[0])
            else:
                pass
    
    # executemany를 사용하기 위해 이중 리스트형태로 만듦
    total_list = []
    for i in range(len(data)):
        total_list = total_list + [list(data.iloc[i,:])]
        total_list[i].insert(0, time)
    
    # db에 올리는 과정에서 에러가 나는 것을 방지
    # RDS에 적재
    while True:
        try:
            connection()
            sql = f'''INSERT INTO fine_dust_table(datetime, stationName, pm25Value, pm25Value24, pm25Grade1h, pm25Grade,pm10Value, pm10Value24, pm10Grade1h, pm10Grade) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
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
    if datetime.datetime.now(KST).minute == 15:
        get_fine_dust_data(datetime.datetime.now(KST), certified_keys, city)
        # 너무 빨리 호출 및 데이터 저장이 이루어져, 같은 데이터가 2번씩 올라가는 경우가 있어
        # 이를 방지하기 위해 2분 동안 동작을 멈춰놓음
        time.sleep(120)