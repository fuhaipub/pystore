#   coding: utf-8

import pymysql


def importDataToDB(DB):

    cur = DB.cursor()

    try:
        with open("./ml-1m/users.dat") as data:
            map (cur.execute,
                ["insert into USERS(userid,gender,age,occupation,zipcode) VALUES('%s','%s','%s','%s','%s');"
                 % tuple(line.strip().replace('\'','\\\'').split('::')) for line in data.readlines()  if line.strip() != ""]
               )
            DB.commit()
        with open("./ml-1m/movies.dat") as data:
            map (cur.execute,
                ["insert into MOVIES(movieid, title, genres) VALUES('%s','%s','%s');"
                 % tuple(line.strip().replace('\'','\\\'').split('::')) for line in data.readlines() if line.strip() != ""]
               )
            #DB.commit()
        with open("./ml-1m/ratings.dat") as data:
            map (cur.execute,
                ["insert into RATINGS(userid,movieid,rating,timestamp) VALUES('%s','%s','%s','%s');"
                 % tuple(line.strip().replace("\'","\\\'").split('::')) for line in data.readlines()  if line.strip() != ""]
               )
            DB.commit()
    except Exception as e:
        print e


# 打开数据库连接
db = pymysql.connect("localhost","dfh","dfh123","DFHDB" )

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute() 方法执行 SQL，如果表存在则删除
cursor.execute("DROP TABLE if EXISTS USERS")
'''UserID::Gender::Age::Occupation::Zip-code'''

cursor.execute("DROP TABLE if EXISTS MOVIES")
'''MovieID::Title::Genres'''

cursor.execute("DROP TABLE if EXISTS RATINGS")
'''UserID::MovieID::Rating::Timestamp'''
# 码表:

# 使用预处理语句创建表
sql_tb_users = '''CREATE TABLE  USERS(
          userid VARCHAR(20) NOT NULL,
          gender VARCHAR(10) NOT NULL,
          age VARCHAR(2) NOT NULL,
          occupation VARCHAR(10) NOT NULL,
          zipcode VARCHAR(50),
          PRIMARY KEY (userid)
          );'''

sql_tb_movies ='''CREATE TABLE MOVIES(
          movieid VARCHAR(100) NOT NULL,
          title VARCHAR(100) NOT NULL,
          genres VARCHAR(100) NOT NULL,
          PRIMARY KEY (movieid)
          );'''

sql_tb_ratings ='''CREATE TABLE RATINGS(
          userid VARCHAR(20) NOT NULL,
          movieid VARCHAR(100) NOT NULL,
          rating VARCHAR(10),
          timestamp VARCHAR(100) );
          '''

try:
    cursor.execute(sql_tb_movies)
    cursor.execute(sql_tb_users)
    cursor.execute(sql_tb_ratings)
    db.commit()
    importDataToDB(db)

except Exception as e:
    print e
finally:
    db.close()

