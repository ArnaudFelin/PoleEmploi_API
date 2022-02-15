import scrapy
from scrapy.loader import ItemLoader

import mysql.connector
import random 

from datetime import datetime
from PoleEmploi_API import utils
from PoleEmploi_API.items import PoleemploiApiItem
from twisted.internet import reactor, defer


#======================================================================================================================================
# scrapy class
#======================================================================================================================================
class DatajobsearchSpider(scrapy.Spider):
    name = 'dataJobSearch'
    
    #====================================================================================================================================== 
    #My global variables
    #====================================================================================================================================== 
    all_lists_of_offers = []
    access_token = ''
    number_of_offers = 0
    offers_Description_In_fr = {}
    offers_Description_In_En = {}
    step = ''
    
    #======================================================================================================================================        
    #Used for test / dev / prod and will be retrieve from the settings
    #======================================================================================================================================
    ALL_OFFERS = False
    NUMBER_OF_OFFERS = 0
    MODE = ''
    CHECK_DB = False
    TRANSLATION = False

    DEBUG = False
    DEBUG_SUB_LEVEL = 1
    
    #======================================================================================================================================        
    #Used to listen signals
    #======================================================================================================================================
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DatajobsearchSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=scrapy.signals.spider_idle)
        return spider
    
    #======================================================================================================================================        
    #Used to wait the end of certains steps before others traitements
    #
    #At the end of all the traitements the spider will close
    #======================================================================================================================================
    def spider_idle(self, spider):
        utils.my_logger(self.logger.info,f'Spider idle - Step : {self.step}', log_type = 'title', log_title = '*Idle*')
           
        #In order to not finish the spider we have to crawl a default web page, use of the call back function to step foward in the steps
        if self.step == 'save_response':
            if self.CHECK_DB:
                self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.check_in_db, dont_filter=True), spider)
            elif self.TRANSLATION:
                self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.request_all_translations, dont_filter=True), spider)
            else:
                self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.classification_and_parse_offer_details, dont_filter=True), spider)
                
        elif self.step == 'check_in_db':
            if self.TRANSLATION:
                self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.request_all_translations, dont_filter=True), spider)
            else:
                self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.classification_and_parse_offer_details, dont_filter=True), spider)
                
        elif self.step == 'save_translation':
            self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.classification_and_parse_offer_details, dont_filter=True), spider)

        pass
    
    #======================================================================================================================================        
    #Step 1 - get 'pole emploi api' access_token
    #======================================================================================================================================
    def start_requests(self):       
        utils.my_logger(self.logger.info,"Get 'pole emploi api' access_token", log_type = 'title', log_title = 'Step 1')
        
        #Retrieve the value of custo setting variable
        self.ALL_OFFERS = self.settings.get('CUSTO_ALL_OFFERS')
        self.NUMBER_OF_OFFERS = self.settings.get('CUSTO_NUMBER_OF_OFFERS')
        self.MODE = self.settings.get('CUSTO_MODE')
        self.CHECK_DB = self.settings.get('CUSTO_CHECK_DB')
        self.TRANSLATION = self.settings.get('CUSTO_TRANSLATION')
        self.DEBUG = self.settings.get('CUSTO_DEBUG')
        self.DEBUG_SUB_LEVEL = self.settings.get('CUSTO_DEBUG_SUB_LEVEL')
              
        url = 'https://entreprise.pole-emploi.fr/connexion/oauth2/access_token?realm=%2Fpartenaire'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        datas = {'grant_type': 'client_credentials',
        'client_id':'YOUR client_id',
        'client_secret':'YOUR client_secret',
        'scope':'YOUR scope'
        }
                
        yield scrapy.FormRequest(url=url, headers=headers, formdata=datas, callback=self.get_limit_of_offers)
        self.step = 'start_requests'
        pass
    
    #======================================================================================================================================
    #Step 2 - get the number of offers in order to "calculate" the number of page to request (not via the api but via the web site ? not found 
    #this information in the api ?)
    #======================================================================================================================================
    def get_limit_of_offers(self, response):
        utils.my_logger(self.logger.info,'Get the number of offers', log_type = 'title', log_title = 'Step 2')
        
        if self.DEBUG: utils.my_logger_debug(self.logger.info, response.text, self.DEBUG_SUB_LEVEL)
        
        url = 'https://candidat.pole-emploi.fr/offres/recherche?motsCles=data&offresPartenaires=true&range=0-19&tri=0'
        
        self.access_token = response.json()['access_token']
               
        yield scrapy.Request(url=url, callback=self.get_all_offers)
        
        self.step = 'get_limit_of_offers'
        pass

    #======================================================================================================================================
    #Step 3 - get all the offers
    #======================================================================================================================================
    def get_all_offers(self, response):
        utils.my_logger(self.logger.info,'Get all the offers', log_type = 'title', log_title = 'Step 3')
        if self.DEBUG: utils.my_logger_debug(self.logger.info,response.text, self.DEBUG_SUB_LEVEL, 1)
        
        #retrieve the total number of offers
        elem = response.xpath('//div[@id=$val]//h1/text()', val = 'zoneAfficherListeOffres').get()
        
        if self.ALL_OFFERS:
            self.number_of_offers = int(elem.strip().split(' ')[0])
        else:
            self.number_of_offers = self.NUMBER_OF_OFFERS
        
        #url of the api                         
        url = 'https://api.emploi-store.fr/partenaire/offresdemploi/v2/offres/search?'
        headers = {'Authorization': 'Bearer ' + self.access_token}
        datas = {'motsCles':'data',
           'range':''
        }
                       
        #Set a list of 100 request each in order to not over-requested the api
        range_offers = []
                
        if self.number_of_offers > 100:
            number_of_pages_to_request = self.number_of_offers//100
            rest = self.number_of_offers%100
                                   
            for i in range(0, number_of_pages_to_request):
                range_offers.append(f'{i*100}-{(i+1)*100-1}')    
            
            range_offers.append(f'{number_of_pages_to_request*100}-{number_of_pages_to_request*100+rest}')
            
        else:
            range_offers.append(f'{0}-{self.number_of_offers}')
            
        #Request the API for data in batches of 100    
        for i in range_offers:
            datas['range'] = i
            yield scrapy.FormRequest(url=url, headers=headers, method='GET', formdata=datas, callback=self.save_response) 
        
        self.step = 'get_all_offers'
        pass
    
    #======================================================================================================================================
    #Step 4 - save all the responses in a single variable
    #======================================================================================================================================
    def save_response(self,response):
        utils.my_logger(self.logger.info,'Step 4 - Save all the responses')
        
        if response.status == 200 or response.status == 206:
            if self.DEBUG: utils.my_logger_debug(self.logger.info,'Response status ok :\n' + response.text, self.DEBUG_SUB_LEVEL)
            self.all_lists_of_offers.append(response.json()['resultats']) 
        else:
            if self.DEBUG: utils.my_logger_debug(self.logger.info,'No data in the response :\n' + response.text, self.DEBUG_SUB_LEVEL)
                   
        self.step = 'save_response'
        pass
    
    #======================================================================================================================================
    #Step 5 - Check witch offers are already in DB
    #======================================================================================================================================
    def check_in_db(self,response):
        utils.my_logger(self.logger.info,'Check witch offers are already in DB', log_type = 'title', log_title = 'Step 5')
                
        offer_id_in_db = set()
        list_of_offers_to_delete = []
        nb_of_offers = 0
        
        #Trying to connect to db 
        try:
            mydb = mysql.connector.connect(
                host="localhost",
                database="datajobsearch",  
                user="root",
                password="root"
                )
            mycursor = mydb.cursor()
            
            mycursor.execute(f'SELECT offer_id FROM emploi_{self.MODE}')
            result = mycursor.fetchall()

            mycursor.close()
            mydb.close()

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            mycursor.close()
            mydb.close()
            
        if self.DEBUG: 
            utils.my_logger_debug(self.logger.info,'Fetch all offer_id already in db :\n' + str(result), self.DEBUG_SUB_LEVEL)
            utils.my_logger_debug(self.logger.info,'Offers we want to check :\n' + str(self.all_lists_of_offers), self.DEBUG_SUB_LEVEL)
        
        #Build the list of all offer_id already in db
        for i in result:
            for j in i:
                offer_id_in_db.add(j)
        
        if self.DEBUG:             
            for i in self.all_lists_of_offers:
                utils.my_logger_debug(self.logger.info,f'self.all_lists_of_offers contains : {len(self.all_lists_of_offers)} lists', self.DEBUG_SUB_LEVEL)
                utils.my_logger_debug(self.logger.info,f'This list contains : {len(i)} offers', self.DEBUG_SUB_LEVEL)
                 
        #################################################################################
        #With all the offer_id retrieved from the db we build a list of offer_id to delete, then we delete them from the list self.all_lists_of_offers
        #################################################################################
        for i in enumerate(self.all_lists_of_offers):
           for j in i:
               if isinstance(j,int):
                   on_wich_list = j
               if isinstance(j,list):
                   nb_of_offers+=len(j)
                   for k in enumerate(j):
                       for l in k:
                           if isinstance(l,int):
                               offer_to_delete = l
                           if isinstance(l,dict):
                               for m in l:
                                   if m == 'id':
                                       if l[m].strip() in offer_id_in_db:
                                           if self.DEBUG: utils.my_logger_debug(self.logger.info,'This offer_id is already in DB, so delete it :' + l[m].strip(), self.DEBUG_SUB_LEVEL)
                                           list_of_offers_to_delete.append((on_wich_list,offer_to_delete))
                                                

        nb = 0 
        num_list_hist = 0             
        for i in list_of_offers_to_delete:
            num_list = i[0]
            
            if num_list_hist != num_list:
                num_list_hist = num_list
                nb = 0
                      
            del self.all_lists_of_offers[i[0]][i[1]-nb]
            nb+=1                                                                                    
        #################################################################################
        
        if self.DEBUG:             
            for i in self.all_lists_of_offers:
                utils.my_logger_debug(self.logger.info,f'self.all_lists_of_offers contains : {len(self.all_lists_of_offers)} lists', self.DEBUG_SUB_LEVEL)
                utils.my_logger_debug(self.logger.info,f'This list contains : {len(i)} offers', self.DEBUG_SUB_LEVEL)
                
        
        
        utils.my_logger(self.logger.info, f'Already in db : {len(list_of_offers_to_delete)} offers on {nb_of_offers} - {nb_of_offers - len(list_of_offers_to_delete)} will be added to the DB')
        
        self.step = 'check_in_db'
        pass    
    
    #======================================================================================================================================
    #Step 6 - used to set up all the requests for the translation of all the descriptions of the offers
    #======================================================================================================================================
    def request_all_translations(self,response):
        utils.my_logger(self.logger.info,'Set up for the tranbslation of the descrition in english', log_type = 'title', log_title = 'Step 6')
        
        #Creation of a dict of offer_id and description
        for i in self.all_lists_of_offers:                
            for j in i:    
                for k in j:
                    
                    if k == 'id':
                        offer_id = j[k].strip()
                    
                    if k == 'description': 
                        #strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';') is necessary in order to save the result also in csv file                                       
                        self.offers_Description_In_fr[offer_id] = j[k].strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
        
        if self.DEBUG: utils.my_logger_debug(self.logger.info,str(self.offers_Description_In_fr), self.DEBUG_SUB_LEVEL,1)                              
        
        #################################################################################
        #In order not to be banned for a too large number of requests, the following formula limits to 3 requests per min the API, 
        #a delay mechanism in scrapy requests is used for this purpose
        #################################################################################
        j = 1
        
        for i in self.offers_Description_In_fr:
            
       
            if len(self.offers_Description_In_fr)>=3:
                time_delay = random.randint(1,60) 
                time_delay += (j%int(len(self.offers_Description_In_fr)/3))*60
                j+=1
            else:
                time_delay = random.randint(1,60)

            
            text_to_translate = self.offers_Description_In_fr[i]
            
            if self.DEBUG: utils.my_logger_debug(self.logger.info,str(time_delay), self.DEBUG_SUB_LEVEL,1)
           
            yield scrapy.Request('https://example.com/', meta={
                            'time': time_delay,  
                            'url': 'https://translate.googleapis.com/translate_a/single',
                            'offer_id':i,
                            'text_to_translate': text_to_translate
                            }, callback=self.translate_description_with_pause, dont_filter=True)
        #################################################################################
               
        self.step = 'request_all_translations'
        pass
    
    #======================================================================================================================================
    #Step 7 - used to request the translation, will be call with time pause for each request
    #======================================================================================================================================
    def translate_description_with_pause(self,response):
               
        #################################################################################
        #Call the google translate api with the delay
        #################################################################################
        utils.my_logger(self.logger.info,f"Step 7 - Request the translation for the offer ID : {response.meta['offer_id']}")
        params = build_params(client='gtx', text_to_translate=response.meta['text_to_translate'], src='fr', dest='en',token='xxxx')
                
        d = defer.Deferred()
        reactor.callLater(response.meta['time'], d.callback, scrapy.FormRequest(
                response.meta['url'], method='GET', formdata=params,
                callback=self.save_translation,
                meta={'offer_id':response.meta['offer_id']}, dont_filter=True))
        #################################################################################
        
        self.step = 'translate_description_with_pause'
        return d
    
    #======================================================================================================================================
    #Step 8 - Save all the translations in a single variable
    #======================================================================================================================================
    def save_translation(self,response):
        utils.my_logger(self.logger.info,'Step 8 - Save all the translations in a single variable')
            
        text = utils.format_json(response.text)        
        #This code will be updated when the format is changed.
        translated = ''.join([t[0] if t[0] else '' for t in text[0]])
        
        self.offers_Description_In_En[response.meta['offer_id']] = translated
        
        self.step = 'save_translation'
        pass    
    
    #======================================================================================================================================
    #Step 9 - Parse the response of each offer details and set the scrapy "items" for save it in DB in the corresponding scrapy "pipelines"
    #======================================================================================================================================
    def classification_and_parse_offer_details(self, response):
        utils.my_logger(self.logger.info,'Parse the response of each offer details', log_type = 'title', log_title = 'Step 9')

        #Load the corresponding items
        itLoader = ItemLoader(item=PoleemploiApiItem(), response=response)
   
        #All the offers are also saved in a new csv file each time the spider is called 
        #This could also have been done on the pipelien instead of here
        filename = str(datetime.now()).split('.')[0].replace(' ','_').replace(':','-')+ '_result.csv'
        data_to_save = {'data_job_type':'', 'confidence_interval':'','origine_offre':'', 'id':'', 'intitule':'',\
                        'description':'', 'description_en':'', 'dateCreation':'', 'dateActualisation':'','lieuTravail_libelle':'',\
                        'lieuTravail_codePostal':'', 'romeCode':'', 'romeLibelle':'', 'appellationlibelle':'', 'entreprise_nom':'', 'entreprise_description':'',\
                        'typeContrat':'', 'typeContratLibelle':'', 'natureContrat':'', 'origineOffre_partenaires':'', 'experienceExige':'', 'experienceLibelle':'',\
                        'experienceCommentaire':'', 'formations':'', 'langues':'', 'permis':'', 'outilsBureautiques':'', 'competences':'', 'salaire':'',\
                        'dureeTravailLibelle':'', 'complementExercice':'', 'conditionExercice':'', 'alternance':'', 'contact':'', 'nombrePostes':'',\
                        'accessibleTH':'', 'deplacementCode':'', 'deplacementLibelle':'', 'qualificationLibelle':'', 'secteurActivite':'',\
                        'secteurActiviteLibelle':'', 'qualitesProfessionnelles':'' }
                
        with open(filename, 'w', encoding='utf8') as f:
            
            header_line = 'data_job_type\;confidence_interval\;origin_offer\;offer_id\;title\;description\;description_en\;creation_date\;update_date\;pow_town\;\
                    pow_postal_code\;rome_code\;rome_job_label\;rome_name_label\;company_name\;company_description\;contract_type_code\;\
                    contract_type_label\;nature_contract_label\;partners_name_of_offer\;experience_required\;experience_label\;experience_comment\;\
                    list_of_trainings\;list_of_languages\;list_of_permits\;list_of_office_tools_used\;list_of_competencies\;salary\;working_time\;\
                    commentary_on_the_conditions_of_exercise\;conditions_of_exercise\;alternation\;list_of_contacts\;number_of_positions\;handi_friendly\;\
                    trip_code\;name_of_trip\;qualification_label\;activity_area\;activity_area_label\;list_of_professional_skills\;\n'
            
            f.write(header_line.replace(' ',''))
            
            classification = []
            separator = ' / '
            temp = []
            
            #################################################################################
            #Browse the list self.all_lists_of_offers and :
            #- classify the offer
            #- save each interesting value in a temporary list which will be used to : 
            #   - save all the values ​​in the items 
            #   - then to create a line of the csv file
            #################################################################################
            if self.DEBUG: utils.my_logger_debug(self.logger.info,str(self.all_lists_of_offers), self.DEBUG_SUB_LEVEL)
            
            for i in self.all_lists_of_offers:
                for j in i:
                    #Reset dataToSave :
                    for data_reset in data_to_save:
                        data_to_save[data_reset]=''
                    #Classification of the offer
                    classification = classify_the_offers(j['intitule'].lower().strip().translate(str.maketrans("\n\t\r", "   ")),\
                                                         j['description'].lower().strip().translate(str.maketrans("\n\t\r", "   ")),\
                                                         j['appellationlibelle'].lower().strip().translate(str.maketrans("\n\t\r", "   ")) )
                    
                    data_to_save['data_job_type'] = classification[0]
                    data_to_save['confidence_interval'] = classification[1]
                    data_to_save['origine_offre'] = 'Pole emploi'
                    
                    for k in j:
                        if k == 'id' or k == 'intitule' or k == 'description' or k == 'romeCode'\
                            or k == 'romeLibelle' or k == 'appellationlibelle' or k == 'typeContrat' or k == 'typeContratLibelle'\
                            or k == 'natureContrat' or k == 'experienceExige' or k == 'experienceLibelle' or k =='experienceCommentaire'\
                            or k == 'outilsBureautiques' or k == 'dureeTravailLibelle' or k == 'complementExercice' or k == 'conditionExercice'\
                            or k == 'deplacementCode' or k == 'deplacementLibelle' or k == 'qualificationLibelle'\
                            or k == 'secteurActivite' or k == 'secteurActiviteLibelle': 
                                
                                data_to_save[k] = j[k].strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                                
                                if self.TRANSLATION:
                                    if k == 'id':
                                        id_offer = j[k].strip()
                                
                                    if k == 'description':
                                        data_to_save[k+'_en'] = self.offers_Description_In_En[id_offer].replace(',',';')
                                else:
                                    data_to_save[k+'_en'] = ''
                            
                        elif k == 'dateCreation' or k == 'dateActualisation':
                           data_to_save[k] = str(j[k]).split('T')[0] 
                       
                        elif k == 'lieuTravail':
                            
                            for l in j[k]:
                                
                                if l == 'libelle' or l == 'codePostal':
                                    data_to_save[k+'_'+l] = j[k][l].strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                                    
                        elif k == 'entreprise':
                            
                            for l in j[k]:
                                
                                if l == 'nom' or l == 'description':
                                    data_to_save[k+'_'+l] = j[k][l].strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                                    
                        elif k == 'origineOffre':
                            
                            for l in j[k]:
                                
                                if l == 'partenaires':                                 
                                    temp.clear()
                                    
                                    for i in j[k][l]:
                                        if 'nom' in i:
                                            temp.append(i['nom'])                                    
                                    
                                    data_to_save[k+'_'+l] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                                    
                        elif k == 'formations':                            
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'domaineLibelle' in i:
                                    temp.append(i['domaineLibelle'])
                                if 'niveauLibelle' in i:
                                    temp.append(i['niveauLibelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                     
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                            
                        elif k == 'langues':                            
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                            
                        elif k == 'permis':                                                        
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                        
                        elif k == 'competences':                                                       
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                        
                        elif k == 'salaire': 
                            temp.clear()
                                                             
                            if 'libelle' in j[k]:
                                temp.append(j[k]['libelle'])
                            if 'commentaire' in j[k]:
                                temp.append(j[k]['commentaire']) 
                            if 'complement1' in j[k]:
                                temp.append(j[k]['complement1']) 
                            
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')                       
                        
                        elif k == 'alternance' or k == 'accessibleTH' or k == 'nombrePostes':                            
                            data_to_save[k] = str(j[k]).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                        
                        elif k == 'contact':                            
                            temp.clear()

                            if 'nom' in j[k]:
                                temp.append(j[k]['nom'])
                            if 'coordonnees1' in j[k]:
                                temp.append(j[k]['coordonnees1']) 
                            if 'coordonnees2' in j[k]:
                                temp.append(j[k]['coordonnees2'])
                            if 'coordonnees3' in j[k]:
                                temp.append(j[k]['coordonnees3'])
                            if 'urlPostulation' in j[k]:
                                temp.append(j[k]['urlPostulation'])
                            
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                            
                        elif k == 'qualitesProfessionnelles':                                                       
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'description' in i:
                                    temp.append(i['description'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                    #################################################################################                        
             
                    #Add the items in order to save it in DB
                    itLoader.add_value('data_job_type', data_to_save['data_job_type'])
                    itLoader.add_value('confidence_interval', data_to_save['confidence_interval'])
                    itLoader.add_value('origin_offer', data_to_save['origine_offre'])
                    itLoader.add_value('offer_id', data_to_save['id'])
                    itLoader.add_value('title', data_to_save['intitule'])
                    itLoader.add_value('description', data_to_save['description'])
                    itLoader.add_value('description_en', data_to_save['description_en'])
                    itLoader.add_value('creation_date', data_to_save['dateCreation'])
                    itLoader.add_value('update_date', data_to_save['dateActualisation'])  
                    itLoader.add_value('pow_town', data_to_save['lieuTravail_libelle'])
                    itLoader.add_value('pow_postal_code', data_to_save['lieuTravail_codePostal'])
                    itLoader.add_value('rome_code', data_to_save['romeCode'])
                    itLoader.add_value('rome_job_label', data_to_save['romeLibelle'])
                    itLoader.add_value('rome_name_label', data_to_save['appellationlibelle'])
                    itLoader.add_value('company_name', data_to_save['entreprise_nom'])
                    itLoader.add_value('company_description', data_to_save['entreprise_description'])
                    itLoader.add_value('contract_type_code', data_to_save['typeContrat'])
                    itLoader.add_value('contract_type_label', data_to_save['typeContratLibelle'])
                    itLoader.add_value('nature_contract_label', data_to_save['natureContrat'])
                    itLoader.add_value('partners_name_of_offer', data_to_save['origineOffre_partenaires'])
                    itLoader.add_value('experience_required', data_to_save['experienceExige'])
                    itLoader.add_value('experience_label', data_to_save['experienceLibelle'])
                    itLoader.add_value('experience_comment', data_to_save['experienceCommentaire'])
                    itLoader.add_value('list_of_trainings', data_to_save['formations'])
                    itLoader.add_value('list_of_languages', data_to_save['langues'])
                    itLoader.add_value('list_of_permits', data_to_save['permis'])
                    itLoader.add_value('list_of_office_tools_used', data_to_save['outilsBureautiques'])
                    itLoader.add_value('list_of_competencies', data_to_save['competences'])
                    itLoader.add_value('salary', data_to_save['salaire'])
                    itLoader.add_value('working_time', data_to_save['dureeTravailLibelle'])
                    itLoader.add_value('commentary_on_the_conditions_of_exercise', data_to_save['complementExercice'])
                    itLoader.add_value('conditions_of_exercise', data_to_save['conditionExercice'])
                    itLoader.add_value('alternation', data_to_save['alternance'])
                    itLoader.add_value('list_of_contacts', data_to_save['contact'])
                    itLoader.add_value('number_of_positions', data_to_save['nombrePostes'])
                    itLoader.add_value('handi_friendly', data_to_save['accessibleTH'])
                    itLoader.add_value('trip_code', data_to_save['deplacementCode'])
                    itLoader.add_value('name_of_trip', data_to_save['deplacementLibelle'])
                    itLoader.add_value('qualification_label', data_to_save['qualificationLibelle'])
                    itLoader.add_value('activity_area', data_to_save['secteurActivite'])
                    itLoader.add_value('activity_area_label', data_to_save['secteurActiviteLibelle'])
                    itLoader.add_value('list_of_professional_skills', data_to_save['qualitesProfessionnelles'])
                    
                    #Write the data in the csv file   
                    for data_to_write in data_to_save:
                        f.write(data_to_save[data_to_write])
                        f.write('\;')
                        
                    f.write('\n')
                            
        self.step = 'classification_and_parse_offer_details'
        return itLoader.load_item()
    
    #======================================================================================================================================
    #Default
    #======================================================================================================================================
    def parse(self, response):
        pass

#======================================================================================================================================
# my functions : not in the class  
#======================================================================================================================================
 
def classify_the_offers(title, description, rome_name_label):
           
    classification = []               
          
    #DA
    if is_present(title,'analyst','analyste') and not is_present(title,'scientist'):
            
       classification.append('DA')
            
       if is_present(rome_name_label,'data analyst'):
          classification.append('A')
       else:
          classification.append('B')
            
       return classification               
        
    #DS
    if is_present(title,'scientist','data science','intelligence artificielle',\
                       'data-science','ingénieur data science','machine learning') and not\
       is_present(description,'data ingénieur','data engineer'):
            
       if is_present(rome_name_label,'data scientist','data analyst'):
           classification.append('DS')
           classification.append('A')
       elif is_present(rome_name_label,'data manager'):
            classification.append('DS Manager')
            classification.append('A')
       else:
            classification.append('DS')
            classification.append('B')
            
       return classification
        
    #DE
    if is_present(title,'développeur big data','data software engineer','ingénieur big data',\
                      'data ingénieur','ingénieur des données') or\
                      are_present(description,'data','engineer'):
            
       if is_present(rome_name_label,'développeur /','ingénieur /','ingénieur concepteur /'):
           classification.append('DE')
           classification.append('A')
       elif is_present(rome_name_label,'data manager'):
            classification.append('DE Manager')
            classification.append('A')
       else:
            classification.append('DE')
            classification.append('B')
            
            return classification
        
    #Data Miner
    if is_present(title,'data miner') and is_present(rome_name_label,'data miner'):
                  
        classification.append('Data Miner')
        classification.append('A')
            
        return classification

    #Consultant
    if is_present(title,'consultant'):
            
       if is_present(rome_name_label,'consultant'):
           classification.append('Consultant - Data')
           classification.append('A')
       elif is_present(rome_name_label,'data manager'):
            classification.append('Consultant - Data Manager')
            classification.append('A')
       else:
            classification.append('Consultant - Data')
            classification.append('B')
            
            return classification

    #Data Manager
    if is_present(title,'data manager') or are_present(title, 'chef de projet', 'data') or are_present(title, 'data', 'quality') :
            
       if is_present(rome_name_label,'data manager','chef de projet'):
           classification.append('Data Manager')
           classification.append('A')
                  
           return classification
      
    #Data Architecte
    if is_present(title,'architecte') or (is_present(title, 'architecte', 'architect') and is_present(title, 'data')) or are_present(title, 'tech lead', 'data') :
       
       classification.append('Data Architecte')
        
       if is_present(rome_name_label,'architecte'):
          classification.append('A')
       else:
          classification.append('B')
             
          return classification
  
    #Data Protection Officer
    if is_present(title,'data protection officer', 'protection des données'):
       
       classification.append('Data Protection Officer')
        
       if is_present(rome_name_label,'délégué(e) à la protection des données - DPO'):
          classification.append('A')
       else:
          classification.append('B')
             
          return classification

    #Data Developer
    if are_present(title,'développeur','data'):
       
       if is_present(rome_name_label,'développeur'):
          classification.append('Data Developer')
          classification.append('A')
             
          return classification

    #Data Product Owner 
    if are_present(title, 'owner', 'data'):
       
       classification.append('Data Product Owner')
        
       if is_present(rome_name_label,'product owner'):
          classification.append('A')
       else:
          classification.append('B')
             
          return classification

    #In case of no classification we have to return something
    if len(classification) == 0:
        classification.append('')
        classification.append('')

    return classification

#======================================================================================================================================

def is_present(text, *argv):
    nb_of_find = 0
            
    for arg in argv:
        if text.find(arg) == -1:
           nb_of_find+= 0
        else:
           nb_of_find+= 1
        
    if nb_of_find == 0:
       return 0
    else:
       return 1

#======================================================================================================================================
    
def are_present(text, *argv):
    
    for arg in argv:
   
        if text.find(arg) == -1:
           return 0

    return 1

#======================================================================================================================================

def build_params(client, text_to_translate, src, dest, token):
    params = {
        'client': client,
        'sl': src,
        'tl': dest,
        'hl': dest,
        'dt': ['at', 'bd', 'ex', 'ld', 'md', 'qca', 'rw', 'rm', 'ss', 't'],
        'ie': 'UTF-8',
        'oe': 'UTF-8',
        'otf': '1',
        'ssel': '0',
        'tsel': '0',
        'tk': token,
        'q': text_to_translate,
    }
    return params
