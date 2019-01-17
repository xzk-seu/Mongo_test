import tarfile
import os
import json
import pymongo
from mylog import logger


def tar_process(tar_path, db_collection):
    with tarfile.open(tar_path, "r") as tar:
        names = tar.getnames()
        for n, name in enumerate(names):
            member = tar.getmember(name)
            f = tar.extractfile(member)
            if not f:
                continue
            temp = json.load(f)
            if not isinstance(temp, dict):
                continue
            name = name.split('/')[1]
            _id = int(name.split('.')[0])
            temp['_id'] = _id
            try:
                db_collection.insert_one(temp)
            except Exception as e:
                logger.error(e)
            if n % 100 == 0:
                logger.info('COL: %s \t FILE: %d/%d' % (db_collection.name, n, len(names)))


def run():
    my_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = my_client["Med_co-author_0_data"]
    # E:\数据备份\学者知识图谱\学者信息（data_paper_MS）\1_co-author\Medicine co-author\med_co-author_0_data
    dir_path = os.path.join('E:/', '数据备份', '学者知识图谱', '学者信息（data_paper_MS）', '1_co-author',
                            'Medicine co-author', 'med_co-author_0_data')
    tar_list = os.listdir(dir_path)
    for file in tar_list:
        gz = None
        try:
            gz = file.split('.')[-1]
        except Exception as e:
            logger.error(e)
        if gz == 'gz':
            tar_path = os.path.join(dir_path, file)
            logger.info(file)
            db_collection = db[file.split('.')[0]]
            tar_process(tar_path, db_collection)


if __name__ == '__main__':
    run()
