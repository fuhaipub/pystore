#   coding: utf-8

import pymysql

def importDataToDB(DB):

    cur = DB.cursor()

    try:
        #with open("./ml-1m/inout.dat") as data:
        with open("/Users/dingfuhai/Documents/finereport/dou.csv") as data:
            #print data.readlines()
            map (cur.execute,
                ["insert into t_inoutdetails(paydate,trader,department,company,bank,paytype,currency,amount,exrate) VALUES('%s','%s','%s','%s','%s','%s','%s',%s,%s);"
                 % tuple(line.decode("gb2312").strip().replace('\'','\\\'').split(',')) for line in data.readlines()  if line.strip() != ""]
               )
            DB.commit()
    except Exception as e:
        print e


# 打开数据库连接
db = pymysql.connect("localhost","dfh","dfh123","DFHDB", charset="utf8")

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute() 方法执行 SQL，如果表存在则删除
cursor.execute("DROP TABLE if EXISTS t_inoutdetails")
'''字段：
    日期：Date
    收款人：字符串
    部门：字符串
    公司：字符串{ ‘KRM’,’AJ’,’TMT’,’DD’,’JX’,’EF’,’ZD’,’cash'}
    银行:  字符串
    收支类型 :字符串{‘RR’, ‘FY’, ‘FH’, ‘HK’,’YH’,’TS’}
    币种:  字符串 {‘RMB’, ‘USD’}
    金额: Float
    汇率: Float
'''



# 使用预处理语句创建表
sql_tb_t_inoutdetails = '''  CREATE TABLE  t_inoutdetails
(
    paydate  DATE  NOT NULL,
    trader VARCHAR(256),
    department VARCHAR(256) NOT NULL,
    company VARCHAR(10) NOT NULL,
    bank   VARCHAR(256) NOT NULL,
    paytype  VARCHAR(10) NOT NULL,
    currency  VARCHAR(10) NOT NULL,
    amount  DECIMAL(11,2) NOT NULL,
    exrate  DECIMAL(6,4) NOT NULL
);'''


try:
    cursor.execute(sql_tb_t_inoutdetails)
    db.commit()
    importDataToDB(db)

except Exception as e:
    print e
finally:
    db.close()

