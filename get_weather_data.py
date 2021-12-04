from urllib.request import urlopen
import pandas as pd
import json
import datetime
import time
import pytz
import pymysql

# 서울 시간 설정
KST = pytz.timezone('Asia/Seoul')

# 발급 받은 인증키
certified_keys = 

# aws rds 연결
def connetion():
    global db
    global cursor
    db = pymysql.connect(host = ,
                        port = 3306,
                        user = ,
                        passwd = ,
                        db = ,
                        charset = 'utf8')
    cursor = db.cursor(pymysql.cursors.DictCursor)
    
# 데이터 크기를 줄이기 위해서 좌표가 중복되는 구들은 한번만 데이터를 가져오도록 함.
# 리스트의 맨 앞은 x,y좌표
# 중간은 좌표가 같은 구들 -> db에 따로 저장해 둠
# 마지막은 그 구들의 그룹을 식별하기 위한 식별자 -> db에 따로 저장해 둠
# stastion_meta에는 구가 없는 정류소도 있기 때문에, 실제 위도 경도도 같이 저장해둠 
location_list = [[[58, 125], ['구로구'], [0]],
            [[58, 126], ['양천구', '강서구', '영등포구'], [1]],
            [[59, 124], ['금천구'], [2]],
            [[59, 125], ['동작구', '관악구'], [3]],
            [[59, 127], ['은평구', '서대문구', '마포구'], [4]],
            [[60, 126], ['용산구'], [5]],
            [[60, 127], ['종로구', '중구'], [6]],
            [[61, 128], ['강북구'], [7]],
            [[61, 129], ['도봉구', '노원구'], [8]],
            [[61, 125], ['서초구'], [9]],
            [[61, 126], ['강남구'], [10]],
            [[61, 127], ['성동구', '동대문구', '성북구'], [11]],
            [[62, 128], ['중랑구'], [12]],
            [[62, 126], ['광진구', '송파구', '강동구'], [13]]]

def get_weather_data(certified_keys, time, base_date, base_time):
  # 받아온 데이터는 all_data에 정리됨
  all_data = pd.DataFrame([])
  
  # 위에서 정의한 location_list의 인자들을 빼와서 사용
  for k in range(len(location_list)):
      x = location_list[k][0][0]
      y = location_list[k][0][1]
      point = location_list[k][2][0]
      # 데이터를 제대로 가져올 때까지 반복 실행
      while True:
        # 데이터를 가져오는 과정상에서 문제가 생길 때를 대비한 try except
        try:
          url = f'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey={certified_keys}&pageNo=1&numOfRows=1000&dataType=JSON&base_date={base_date}&base_time={base_time}&nx={x}&ny={y}'
          response = urlopen(url)
          results = response.read().decode("utf-8")
          result = json.loads(results)
          # code 00은 데이터가 정상임을 의미
          if result['response']['header']['resultCode'] == '00':
            data = pd.DataFrame(data = result['response']['body']['items']['item'])
            data['point'] = point
            data_1 = data.pivot("point","category","obsrValue").reset_index()
            all_data = all_data.append(data_1)
            break
          # code 00이 아닐 경우 while문의 처음으로 돌아가 다시 데이터를 가져옴
          else:
            continue
        # 데이터를 가져오는 과정에서 문제가 생기면 while문의 처음으로 이동하여 다시 데이터를 가져옴
        except:
          continue
  
  
  
  # index 초기화
  all_data.index = range(len(all_data))
  
  # 결측치는 none으로 바꾸어 db에 올라가는데 문제가 없게함
  all_data=all_data.where(pd.notnull(all_data),None)

  # db에 올리기
  while True:
    try:
      connetion()
      sql = f"INSERT INTO weather_table(datetime, base_date,base_time,point,temperature,1h_rain,east_west_wind,south_north_wind,humidity,rain_kind,wind_direction,wind_speed) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
      # 혹시라도 가져오는 데이터 순서가 바뀔까봐 executemany대신 execute사용
      # executemany를 사용하려면 컬럼 이름 없이 이중 리스트로 데이터 형태를 바꿔야하는데 불안함
      for i in range(len(all_data)):
        cursor.execute(sql, (time, base_date, base_time,
                                                          all_data.loc[i,'point'],
                                                          all_data.loc[i,'T1H'],
                                                          all_data.loc[i,'RN1'],
                                                          all_data.loc[i,'UUU'],
                                                          all_data.loc[i,'VVV'],
                                                          all_data.loc[i,'REH'],
                                                          all_data.loc[i,'PTY'],
                                                          all_data.loc[i,'VEC'],
                                                          all_data.loc[i,'WSD']))
      db.commit()
      db.close()
      break
    except:
      db.close()
      continue


while True:
    if datetime.datetime.now(KST).minute == 45:
        clock = datetime.datetime.now(KST)
        # datetime.datetime.now(KST).hour가 1자리수면 api의 규칙에서 벗어나 에러가 남.
        # 1자리수라면 앞에 0을 붙이게 만듦
        if len(str(clock.hour)) > 1:
            get_weather_data(certified_keys, clock, int(str(clock.date()).replace("-","")), str(clock.hour)+'00')
        else:
            get_weather_data(certified_keys, clock, int(str(clock.date()).replace("-","")), '0'+str(clock.hour)+'00')
        time.sleep(120)