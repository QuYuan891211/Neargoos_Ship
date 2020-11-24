#!/usr/bin/env python

# encoding: utf-8

# @Time    : 2020/11/19 22:23
# @Author  : Qu Yuan
# @Site    : 
# @File    : ShipDataService.py
# @Software: PyCharm
import datetime
import os
import BaseDao
import FTPManager
import DataModel
from apscheduler.schedulers.blocking import BlockingScheduler

global config_path
config_path = r'E:\projects\pycharm\neargoos_ship\ShipData\byQY\config\Config.ini'
global section_neargoos
section_neargoos = 'neargoos'
global section_mysql
section_mysql = 'mysql'


class ShipDataService:
    def __init__(self, config_path, section_neargoos, section_mysql):
        self.ftp_Manager = FTPManager.FTPManager(config_path, section_neargoos)
        self.username = ""
        self.password = ""
        self.config_path = config_path
        self.section_neargoos = section_neargoos
        self.section_mysql = section_mysql
        self.dao = BaseDao.BaseDao(config_path, section_mysql)

    def get_file_info(self):
        """
                获取数据信息列表
                :return file_infos: 数据信息列表
                """

        # 获取配置文件信息
        config = self.ftp_Manager.get_config()
        host = config.get(section_neargoos, 'host')
        self.username = config.get(section_neargoos, 'username')
        self.password = config.get(section_neargoos, 'password')
        target = config.get(section_neargoos, 'target')
        self.ftp_Manager.ftp_connect(host, self.username, self.password)

        # 获取时间
        # self.ftp_Manager.getCreateTime()
        dir_infos = self.ftp_Manager.get_filename("", target)
        file_names_with_dir_list = []
        for dir in dir_infos:
            file_names = self.ftp_Manager.get_filename("", target + dir)
            i = 0
            for name in file_names:
                file_names[i] = target + dir + '/' + file_names[i]
                i = i + 1
            file_names_with_dir_list = file_names_with_dir_list + file_names
        print(len(file_names_with_dir_list))
        return file_names_with_dir_list

    def save_files(self, file_names_with_dir_list):
        """
        转存文件到指定目录，且将文件信息存入数据库的DataInfo表中
        # TODO(QY) 后续可将重复代码优化
        :param file_infos: 数据信息列表
        :return:
        """

        # 1. 在遍历文件列表前先查出海区，类型，数据源对应ID
        # [to-do]暂时只要类型
        # TODO(QY) 1.1 [to-do]得到对应的海区ID（暂时均为默认海区）
        area_result = self.dao.find_by_name(DataModel.DataArea, 'China Sea')
        # TODO(QY) 1.2 [to-do]得到数据源的ID（暂时均为中国）
        source_result = self.dao.find_by_name(DataModel.DataSource, 'China')
        # 1.3 得到数据类型的ID
        category_result = self.dao.find_all(DataModel.DataCategory)
        dict_category = dict()
        for obj in category_result:
            dict_category[obj.name] = obj.id
        num_database = 0
        num_savefile = 0
        # for i in category_result:
        #     print(i.id)
        # print('source: ')
        # print(source_result.id)
        # print('area: ')
        # print(area_result.id)
        # 2.遍历文件名称列表
        i = 0
        for file_info in file_names_with_dir_list:
            # 2.1 一级目录（数据类型）
            folder_level1 = 'SHIP'
            # 2.2 三级目录文件名（年）
            folder_level2 = file_info[36:40]
            # 2.3 四级目录文件名（月）
            folder_level3 = file_info[40:42]
            # 2.4 五级目录文件名（日）
            folder_level4 = file_info[42:44]
            hour = file_info[44:46]
            extension = file_info[-3:]
            # 3.先将文件下载到本地
            local_path_dir = os.path.join('E:', r'\projects\pycharm\NearGoos\webclient\public', folder_level1,
                                          extension, folder_level2, folder_level3,
                                          folder_level4, "")
            # remote_path_dir = file_info[0:10]

            is_success = self.ftp_Manager.download_file(local_path_dir + file_info[10:], file_info,
                                                        local_path_dir)

            if is_success:
                num_savefile += 1

                date_str = folder_level2 + '-' + folder_level3 + '-' + folder_level4 + ' ' + hour + ':' + '00' + ':' + '00'
                url = os.path.join(folder_level1, extension, folder_level2, folder_level3,
                                   folder_level4, file_info[10:])
                # 4.封装实体
                result = self.dao.find_by_url(DataModel.DataDataInfo, url)
                if result is None:
                    self.insert_data_info(file_info[10:], folder_level1, extension, date_str, url, area_result,
                                          dict_category, source_result, local_path_dir + file_info[10:])
                    print('向数据库存储成功')
                    num_database += 1

                else:
                    print('!!!!!!该文件信息已在数据库中存在')
        self.ftp_Manager.close_connect()
        print('静态文件总共存储了' + str(num_savefile))
        print('数据库总共存储了' + str(num_database))

    def insert_data_info(self, file_info, folder_level1, extension, date_str, url, area_result, dict_category,
                         source_result, local_dir):
        """
        封装实体
        :param file_info:
        :param extension:
        :param date_str:
        :param location:
        :param url:
        :return:
        """
        # # 4.1 [to-do]得到对应的海区ID（暂时均为默认海区）
        # area_result = self.dao.find_by_name(DataArea, 'China Sea')
        # # 4.2 [to-do]得到数据源的ID（暂时均为中国）
        # source_result = self.dao.find_by_name(DataSource, 'China')
        # # 4.3 [to-do]得到数据类型的ID（暂时均为中国）
        # category_result = self.dao.find_by_name(DataCategory, folder_level1)
        datainfoModel = DataModel.DataDataInfo()
        info = DataModel.DataDataInfo()
        info.is_delete = 0
        info.gmt_create = datetime.datetime.now()
        info.gmt_modified = datetime.datetime.now()
        info.name = file_info
        info.extensions = extension
        # [to-do]暂时没有备注
        # 使用date类型传入MySQL数据库
        info.date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        info.area_id = area_result.id
        info.category_id = dict_category.get(folder_level1)
        info.source_id = source_result.id
        info.url = url
        # [to-do]暂时没有FTP文件的最新修改日期
        info.size = os.path.getsize(local_dir)
        info.location = extension
        self.dao.insert_one(info)

    def getTime(self):
        self.ftp_Manager.getCreateTime(self)


# TODO(QY) 主任务加入定时任务
def task():
    data_service = ShipDataService(config_path, section_neargoos, section_mysql)
    file_names_list = data_service.get_file_info()

    print(file_names_list)
    data_service.save_files(file_names_list)

# 定时任务
# def scheduleTask():
#     times = 0;
#     # 创建调度器：BlockingScheduler
#     scheduler = BlockingScheduler()
#     scheduler.add_job(task, 'interval', seconds=3600, id='task1')
#     scheduler.start()

if __name__ == "__main__":
    task()
