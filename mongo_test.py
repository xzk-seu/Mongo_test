import pymongo
import os
import json


# mydict = {"name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com"}
#
# x = mycol.insert_one(mydict)
# print(x.inserted_id)
# print(x)

if __name__ == '__main__':
    # C:\Users\xzk09\Desktop\Med_author_d&p\data
    my_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_test_db = my_client["mongo_test_db"]
    test_collection = mongo_test_db["test_collection"]
    # dblist = my_client.list_database_names()
    # c_list = mydb.list_collection_names()
    # print(c_list)
    # if "mongo_test_db" in dblist:
    #     print("数据库已存在！")
    # collist = mydb.list_collection_names()
    # if "test_collection" in collist:  # 判断 sites 集合是否存在
    #     print("集合已存在！")
    c = test_collection.count()
    print(c)

# C:\Program Files\MongoDB\Server\4.0\bin
# mongoexport -d <数据库名称> -c <collection名称> -o <json文件名称>
# mongoexport.exe -d mongo_test_db -c test_collection --limit=1 -o my_limit_test.json
