# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.utils.project import get_project_settings

import logging
import mysql.connector
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from PoleEmploi_API import utils


class PoleemploiApiPipeline:
    collection_name = 'scrapy_items'
    logger = logging.getLogger()   

    def open_spider(self, spider):
        
        #Trying to connect to db 
        try:
            self.mydb = mysql.connector.connect(
                host="localhost",
                database="datajobsearch",  
                user="root",
                password="root"
                )
            self.mycursor = self.mydb.cursor()
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            self.mycursor.close()
            self.mydb.close()
        
        #Retrieve the value of setting variable
        settings=get_project_settings()
        
        self.MODE = settings.get('CUSTO_MODE')
        self.DEBUG = settings.get('CUSTO_DEBUG')
        self.DEBUG_SUB_LEVEL = settings.get('CUSTO_DEBUG_SUB_LEVEL')
        
        self.sql = f"INSERT INTO emploi_{self.MODE} (insertion_date, data_job_type, confidence_interval, origin_offer, offer_id, title,\
            description, description_en, creation_date, update_date, pow_town,\
            pow_postal_code,  rome_code, rome_job_label, rome_name_label, company_name, company_description,\
            contract_type_code, contract_type_label, nature_contract_label, partners_name_of_offer, experience_required,\
            experience_label, experience_comment, list_of_trainings, list_of_languages, list_of_permits,\
            list_of_office_tools_used, list_of_competencies, salary, working_time, commentary_on_the_conditions_of_exercise,\
            conditions_of_exercise, alternation, list_of_contacts, number_of_positions, handi_friendly, trip_code, name_of_trip,\
            qualification_label, activity_area, activity_area_label, list_of_professional_skills)\
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s)"

    def close_spider(self, spider):
        self.mycursor.close()
        self.mydb.close()

    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()
        dataKeys = list(data.keys())
        dataTemp = []
        
        if self.DEBUG: utils.my_logger_debug(self.logger.info, 'Data : ' + str(data), self.DEBUG_SUB_LEVEL)
        if self.DEBUG: utils.my_logger_debug(self.logger.info, 'DataKeys : ' + str(dataKeys), self.DEBUG_SUB_LEVEL,1)
        
        if len(data) > 0:
            for i in range(0,len(data[dataKeys[0]])):
                dataTemp.clear()
                
                if self.DEBUG: utils.my_logger_debug(self.logger.info,'len(data[dataKeys[0]]) : ' + str(len(data[dataKeys[0]])), self.DEBUG_SUB_LEVEL,1)
                
                dataTemp.append(data[dataKeys[0]][i])
                dataTemp.append(data[dataKeys[1]][i])
                dataTemp.append(data[dataKeys[2]][i])
                dataTemp.append(data[dataKeys[3]][i])
                dataTemp.append(data[dataKeys[4]][i])
                dataTemp.append(data[dataKeys[5]][i])
                dataTemp.append(data[dataKeys[6]][i])
                dataTemp.append(data[dataKeys[7]][i])
                dataTemp.append(data[dataKeys[8]][i])
                dataTemp.append(data[dataKeys[9]][i])
                dataTemp.append(data[dataKeys[10]][i])
                dataTemp.append(data[dataKeys[11]][i])
                dataTemp.append(data[dataKeys[12]][i])
                dataTemp.append(data[dataKeys[13]][i])
                dataTemp.append(data[dataKeys[14]][i])
                dataTemp.append(data[dataKeys[15]][i])
                dataTemp.append(data[dataKeys[16]][i])
                dataTemp.append(data[dataKeys[17]][i])
                dataTemp.append(data[dataKeys[18]][i])
                dataTemp.append(data[dataKeys[19]][i])
                dataTemp.append(data[dataKeys[20]][i])
                dataTemp.append(data[dataKeys[21]][i])
                dataTemp.append(data[dataKeys[22]][i])
                dataTemp.append(data[dataKeys[23]][i])
                dataTemp.append(data[dataKeys[24]][i])
                dataTemp.append(data[dataKeys[25]][i])
                dataTemp.append(data[dataKeys[26]][i])
                dataTemp.append(data[dataKeys[27]][i])
                dataTemp.append(data[dataKeys[28]][i])
                dataTemp.append(data[dataKeys[29]][i])
                dataTemp.append(data[dataKeys[30]][i])
                dataTemp.append(data[dataKeys[31]][i])
                dataTemp.append(data[dataKeys[32]][i])
                dataTemp.append(data[dataKeys[33]][i])
                dataTemp.append(data[dataKeys[34]][i])
                dataTemp.append(data[dataKeys[35]][i])
                dataTemp.append(data[dataKeys[36]][i])
                dataTemp.append(data[dataKeys[37]][i])
                dataTemp.append(data[dataKeys[38]][i])
                dataTemp.append(data[dataKeys[39]][i])
                dataTemp.append(data[dataKeys[40]][i])
                dataTemp.append(data[dataKeys[41]][i])

                if self.DEBUG: utils.my_logger_debug(self.logger.info,'DataTemp : ' + str(dataTemp), self.DEBUG_SUB_LEVEL,1)
                                
                try:
                    self.mycursor.execute(self.sql, dataTemp)
                    self.mydb.commit()

                except mysql.connector.Error as err:
                    print(err)
                    print("Error Code:", err.errno)
                    print("SQLSTATE", err.sqlstate)
                    print("Message", err.msg)
                    if self.DEBUG: utils.my_logger_debug(self.logger.info,'On wich dataTemp : ' + str(dataTemp), self.DEBUG_SUB_LEVEL,1)
                                                      
        return item
