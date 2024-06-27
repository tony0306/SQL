import MySQLdb
import pandas as pd

data = pd.read_csv("各鄉鎮市區人口密度109年.csv", header=1, nrows=370, encoding="utf8")
# 處理異常值
data["年底人口數"] = data["年底人口數"].replace("…", 0).astype("int")
data["人口密度"] = data["人口密度"].replace("…", 0).astype("int")

try:
    # 開啟資料庫連接
    conn = MySQLdb.connect(host="localhost",     # 主機名稱
                            user="root",        # 帳號
                            password="tony0306", # 密碼
                            database = "testdb1", #資料庫
                            port=3306,           # port
                            charset="utf8")      # 資料庫編碼
    
    # 使用cursor()方法操作資料庫
    cursor = conn.cursor()
    
    # 建立表格towndata
    sql = """CREATE TABLE IF NOT EXISTS towndata (year CHAR(4) ,
                                                  site VARCHAR(20),
                                                  people_total int(10),
                                                  area float(10),
                                                  population int(10))"""
    cursor.execute(sql)
    print("資料表建立完畢")   
    # 將資料data寫到資料庫中
    # 將數據組合成元組可以使代碼更易讀和維護
    try:
        
        for i in range(len(data)):
            sql = """INSERT INTO towndata (year, site, people_total, area, population)
                                    VALUES (%s, %s, %s, %s, %s)"""
            var = (data.iloc[i,0], data.iloc[i,1], data.iloc[i,2], data.iloc[i,3], data.iloc[i,4])     
            cursor.execute(sql, var)
            
        conn.commit()
        print("資料寫入完成")
        
    except Exception as e:
        print("錯誤訊息：", e)
except Exception as e:
    print("資料庫連接失敗：", e)

finally:
    conn.close()
    print("資料庫連線結束")
        
try:
    conn = MySQLdb.connect(host="localhost",     
                           user="root",          
                           password="tony0306",  
                           database="testdb1",   
                           port=3306,            
                           charset="utf8")       
    cursor = conn.cursor()

    try:
        # 查詢資料庫列表
        sq1 = "SHOW DATABASES"
        cursor.execute(sq1)
        for bd in cursor:
            print(bd)

        # 查詢表格towndata的全部內容
        cursor.execute("""SELECT * FROM towndata""")
        data_query = cursor.fetchall()
        data_df = pd.DataFrame(data_query, columns=["統計年", "區域別", "年底人口數", "土地面積", "人口密度"])
        print(data_df)

        # 查詢表格towndata的指定縣市資料
        key = input("請輸入要查詢的縣市名：")
        cursor.execute("SELECT site, people_total FROM towndata WHERE site LIKE '%s'" % (key + '%'))
        data_result = cursor.fetchall()
        for row in data_result:
            print(row)
        print("%s一共有%d筆資料" % (key, len(data_result)))

    except Exception as e:
        print("資料庫查詢錯誤：", e)

except Exception as e:
    print("資料庫連接失敗：", e)

finally:
    conn.close()
    print("資料庫連線結束")