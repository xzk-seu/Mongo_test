import pymongo
import os
import json
from multiprocessing import Pool
from mylog import logger


def dir_process(db, collection_number):
    collection_name = 'Medicine_author_data_%d' % collection_number
    db_collection = db[collection_name]
    path = os.path.join('C:/', 'Users', 'xzk09', 'Desktop', 'Med_author_d&p', 'data', 'data%d' % collection_number)
    file_list = os.listdir(path)
    for n, f in enumerate(file_list):
        f_id = int(f.split('.')[0])
        file_path = os.path.join(path, f)
        with open(file_path, 'r') as fr:
            temp = json.load(fr)
        temp['_id'] = f_id
        db_collection.insert_one(temp)
        if n % 100 == 0:
            logger.info('INDEX: %d \t FILE: %d/%d' % (collection_number, n, len(file_list)))


def safe_dir_process(db, collection_number):
    try:
        dir_process(db, collection_number)
    except Exception as e:
        logger.error(e)


def run():
    my_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = my_client["Medicine_author_data"]
    pool = Pool(8)
    for i in range(2, 60):
        safe_dir_process(db, i)
    pool.close()
    pool.join()


if __name__ == '__main__':
    run()


# C:\Program Files\MongoDB\Server\4.0\bin
# mongoexport -d <数据库名称> -c <collection名称> -o <json文件名称>
# mongoexport.exe -d mongo_test_db -c test_collection --limit=1 -o my_limit_test.json

# mongodump.exe -d mongo_test_db -c test_collection --limit=1 -o my_limit_test.json
