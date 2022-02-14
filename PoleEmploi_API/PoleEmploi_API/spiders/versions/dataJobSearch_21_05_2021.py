import scrapy
#import time
#from googletrans import Translator
from twisted.internet import reactor, defer
import random 

from PoleEmploi_API import utils

from scrapy.loader import ItemLoader
from PoleEmploi_API.items import PoleemploiApiItem

import mysql.connector

from datetime import datetime

#======================================================================================================================================
# my class
#======================================================================================================================================
#class Translate:

#    def __init__(self):
#       self.translator = Translator()
        
#    def do_translation(self, to_translate):
#        self.translation = self.translator.translate(to_translate, dest='en')
#        return self.translation.text

#======================================================================================================================================
# scrapy class
#======================================================================================================================================
class DatajobsearchSpider(scrapy.Spider):
    name = 'dataJobSearch' 
    access_token = ''
    step = ''
    all_lists_of_offers = []
    offers_Description_In_fr = {}
    offers_Description_In_En = {}
    
      
    
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
        print('function spider_idle ###########################################################################', self.step)
        self.logger.info('spider_idle,  step : %s', self.step)
           
        
        if self.step == 'save_response':
            self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.check_in_db, dont_filter=True), spider)
        elif self.step == 'check_in_db':
            self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.request_all_translations, dont_filter=True), spider)
        elif self.step == 'save_translation':
            self.crawler.engine.crawl(scrapy.Request(url='https://example.com/', callback=self.classification_and_parse_offer_details, dont_filter=True), spider)
            

        pass
    
    #======================================================================================================================================        
    #Step 1 - get the api access_token
    #======================================================================================================================================
    def start_requests(self):
        self.logger.info('\n###################################################################\
                          \nStep 1 - get the api access_token\
                          \n###################################################################\n')
                          
        url = 'https://entreprise.pole-emploi.fr/connexion/oauth2/access_token?realm=%2Fpartenaire'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        datas = {'grant_type': 'client_credentials',
        'client_id':'PAR_datajobsearch_553c8b452f8d302abafa8614d4dd04b792a31ca1c4f0797432e99de21981d043',
        'client_secret':'a3b07264d2f0e48c3c20ca671d86639ed6d178a844d6fadc073ee4c289f3ce3e',
        'scope':'api_offresdemploiv2 application_PAR_datajobsearch_553c8b452f8d302abafa8614d4dd04b792a31ca1c4f0797432e99de21981d043 o2dsoffre'
        }
                
        yield scrapy.FormRequest(url=url, headers=headers, formdata=datas, callback=self.get_limit_of_offers) 
        pass
    
    #======================================================================================================================================
    #Step 2 - get the number of offer in order to "calculate" the number of page to request (not via the api but via the web site ? not found 
    #this information in the api ?)
    #======================================================================================================================================
    def get_limit_of_offers(self, response):
        url = 'https://candidat.pole-emploi.fr/offres/recherche?motsCles=data&offresPartenaires=true&range=0-19&tri=0'
        
        self.access_token = response.json()['access_token']
               
        yield scrapy.Request(url=url, callback=self.get_all_offers)
        
        self.step = 'get_limit_of_offers'
        pass

    #======================================================================================================================================
    #Step 3 - get all the short offers
    #======================================================================================================================================
    def get_all_offers(self, response):
        
        elem = response.xpath('//div[@id=$val]//h1/text()', val = 'zoneAfficherListeOffres').get()
        #self.number_of_offer = int(elem.strip().split(' ')[0])
        self.number_of_offer = 4
                                 
        url = 'https://api.emploi-store.fr/partenaire/offresdemploi/v2/offres/search?'
        headers = {'Authorization': ''}
        datas = {'motsCles':'data',
           'range':''
        }
        range_offers = []
               
        headers['Authorization'] =  'Bearer ' + self.access_token
        
        
        if self.number_of_offer > 100:
            number_of_page_to_request = self.number_of_offer//100
            rest = self.number_of_offer%100
                                   
            for i in range(0, number_of_page_to_request):
                range_offers.append(f'{i*100}-{(i+1)*100-1}')    
            
            range_offers.append(f'{number_of_page_to_request*100}-{number_of_page_to_request*100+rest}')
            
        else:
            range_offers.append(f'{0}-{self.number_of_offer}')
            
              
        for i in range_offers:
            datas['range'] = i
            yield scrapy.FormRequest(url=url, headers=headers, method='GET', formdata=datas, callback=self.save_response) 
        
        self.step = 'get_all_offers'
        pass
    
    #======================================================================================================================================
    #Step 4 - used to save all the response in a variable
    #======================================================================================================================================
    def save_response(self,response):
        
        if response.status == 200 or response.status == 206:
            print('save_response -> Status 200 or 206 ###########################################################################', response.status)
            #print()
            #print('save_response -> Status 200 or 206 ########################################################################### body', response.body)
            #print()
            #print('save_response -> Status 200 or 206 ########################################################################### text', response.text)
            #print()
            #print('save_response -> Status 200 or 206 ########################################################################### json()', response.json())
            #self.all_lists_of_offers.append(response.json()['resultats']) 
            
            self.all_lists_of_offers.append(response.json()['resultats']) 
        else:
            print('save_response -> No data in the response ###########################################################################', response.status)
        
        self.step = 'save_response'
        pass
    
    #======================================================================================================================================
    #Step 5 - used to check witch offers are already in DB
    #======================================================================================================================================
    def check_in_db(self,response):
                
        print('check_in_db ###########################################################################')
        offer_id_in_db = set()
        nb_of_offers_already_in_db = 0
        list_of_offers_to_delete = []
        
        
        
        
        try:
            
            mydb = mysql.connector.connect(
                host="localhost",
                database="datajobsearch",  
                user="root",
                password="root"
                )
            mycursor = mydb.cursor()
            
            mycursor.execute('SELECT offer_id FROM pole_emploi')
            result = mycursor.fetchall()
            #print(result)
            #print(self.all_lists_of_offers)
            mycursor.close()
            mydb.close()

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            mycursor.close()
            mydb.close()
            
            
            
        for i in result:
            for j in i:
                offer_id_in_db.add(j)
        

        
        for i in self.all_lists_of_offers:
            print()
            print(f'check_in_db ########################################################################### self.all_lists_of_offers contains : {len(self.all_lists_of_offers)} lists')
            print(f'check_in_db ########################################################################### this list contains : {len(i)} offers')
            print()
            
        
        for i in enumerate(self.all_lists_of_offers):
           for j in i:
               if isinstance(j,int):
                   on_wich_list = j
               if isinstance(j,list):
                   for k in enumerate(j):
                       for l in k:
                           if isinstance(l,int):
                               offer_to_delete = l
                           if isinstance(l,dict):
                               for m in l:
                                   if m == 'id':
                                       if l[m].strip() in offer_id_in_db:
                                           print('This offer_id is already in DB, so delete it : ', l[m].strip())
                                           list_of_offers_to_delete.append((on_wich_list,offer_to_delete))
                                           nb_of_offers_already_in_db+=1
                                           
        

        nb = 0 
        num_list_hist = 0             
        for i in list_of_offers_to_delete:
            num_list = i[0]
            
            if num_list_hist != num_list:
                num_list_hist = num_list
                nb = 0
                      
            del self.all_lists_of_offers[i[0]][i[1]-nb]
            nb+=1                                           
                                           
        
        for i in self.all_lists_of_offers:
            print()
            print(f'check_in_db ########################################################################### self.all_lists_of_offers contains : {len(self.all_lists_of_offers)} lists')
            print(f'check_in_db ########################################################################### this list contains : {len(i)} offers')
            print()
        
        print(f'check_in_db ########################################################################### already in db : {nb_of_offers_already_in_db} offers')
        print()
        
        self.step = 'check_in_db'
        pass    
        
       
 
    
    #======================================================================================================================================
    #Step 6 - used to set up all the requests for the translation of all the descriptions of the offers
    #======================================================================================================================================
    def request_all_translations(self,response):
        print('request_all_translations ###########################################################################')
        
        for i in self.all_lists_of_offers:                
            for j in i:    
                for k in j:
                    
                    if k == 'id':
                        id_offer = j[k].strip()
                    
                    if k == 'description': 
                        self.offers_Description_In_fr[id_offer] = j[k].strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                                       
        
        
        j = 1
        
        for i in self.offers_Description_In_fr:
            
       
            if len(self.offers_Description_In_fr)>=3:
                time_delay = random.randint(1,60) 
                time_delay += (j%int(len(self.offers_Description_In_fr)/3))*60
                j+=1
            else:
                time_delay = random.randint(1,60)

            
            text_to_translate = self.offers_Description_In_fr[i]
            
            
            print('request_all_translations ###########################################################################', time_delay)
            yield scrapy.Request('https://example.com/', meta={
                            'time': time_delay,  
                            'url': 'https://translate.googleapis.com/translate_a/single',
                            'id_offer':i,
                            'text_to_translate': text_to_translate
                            }, callback=self.translate_description_with_pause, dont_filter=True)
        
        
        self.step = 'request_all_translations'
        pass
    
    #======================================================================================================================================
    #Step 7 - used to request the translation, will be call with time pause for each request
    #======================================================================================================================================
    def translate_description_with_pause(self,response):
        print('translate_description_with_pause ###########################################################################')
        params = build_params(client='gtx', text_to_translate=response.meta['text_to_translate'], src='fr', dest='en',token='xxxx')
                
        d = defer.Deferred()
        reactor.callLater(response.meta['time'], d.callback, scrapy.FormRequest(
                response.meta['url'], method='GET', formdata=params,
                callback=self.save_translation,
                meta={'id_offer':response.meta['id_offer']}, dont_filter=True))
        
        self.step = 'translate_description_with_pause'
        return d
    
    #======================================================================================================================================
    #Step 8 - used to save all the response in a variable
    #======================================================================================================================================
    def save_translation(self,response):
        print('save_translation ###########################################################################')
        text = utils.format_json(response.text)
        
        # this code will be updated when the format is changed.
        translated = ''.join([t[0] if t[0] else '' for t in text[0]])
        
        self.offers_Description_In_En[response.meta['id_offer']] = translated
        
        
        self.step = 'save_translation'
        pass    
    
    #======================================================================================================================================
    #Step 9 - parse the response of each offer detail and set the scrapy "items" for save it in DB in the corresponding scrapy "pipelines"
    #======================================================================================================================================
    def classification_and_parse_offer_details(self, response):
        
        print('classification_and_parse_offer_details ###########################################################################')
        
        #for i in self.all_lists_of_offers:
        #    for j in i:
        #        print('################')
        #        print(j)
        #        print('################')
        #        print('################')
        #        print('################')
        #        print()
        #        print()
             
    
    #TODO : contrairement au site l'api me donne déjà toutes les infos des annonces !! je n'ai pas à demander plus d'infos annonce par annonce par la suite !
    #
    #j'ai juste un tri a effectuer suivants mes critères dans all_lists_of_offers et supprimer ce qui ne m'interesse pas
    #penser a ajouter 2 critères - obtenu sur quel site - categorie data (data science - analyste - ingnieur - machine learning ing) à ce sujet voir comment faire une 
    #boucle pour rechercher plussieurs critères comme data - machine learning ing ect... c'est à faire très certainement plus haut dans le code
    #regarder ce qu'il y a déjà en db pour ne sauvegader que ce qu'il faut - puis suivre le process avec les items ect...dans l'étape suivante parse_offer_details
    
    #Exemple de résultat à prendre en compte pour la DB
    #{'resultats': [{'id': '4353214', 'intitule': 'Chef de projet data junior (H/F)', 'description': "Axysweb est une agence spécialisée depuis 15 ans dans l'intégration de solutions de gestion des données. Grâce à notre niveau d'expertise, nous permettons à nos clients d'avoir une pleine confiance dans leurs données et d'en tirer le meilleur parti. Notre équipe qui porte nos valeurs d'expertise, de rigueur et d'intégrité, intervient sur des problématiques telles que la qualification, l'exploitation ou l'intégration des données d'entreprise.\n Bon maintenant que c'est dit, ont te propose quoi ?\n Pour devenir leader de notre marché on n'y va pas par 4 chemins, on recrute des bons ! Donc on recrute un Chef de projet data junior (H/F)\n Ta mission si tu l'acceptes : renforcer la qualité des relations avec nos clients !\n Après une période de formation à nos méthodes et à notre culture, tu interviendras en support de nos projets et seras l'interlocuteur privilégié de nos clients.\n Et tu vas faire quoi ?\n Missions principales :\n * Tenir le budget des projets\n * Réceptionner les retours du client (demandes de modifications/adaptations, nouvelles demandes)\n * Gérer le planning entre le client et Axysweb au niveau fonctionnel\n * Etre l'interface entre le client et la technique\n * Contrôler le respect du cahier des charges au niveau fonctionnel\n * Faire le reporting auprès de la direction\n Missions secondaires :\n * Réceptionner et étudier le besoin de nos clients\n * Formaliser le besoin (écriture exacte de ce que veut le client)\n * Transmettre le besoin à l'équipe technique\n * Contrôler le respect du cahier des charges au niveau fonctionnel\n * Conseiller les clients\n Si tu veux postuler tu nous envoies ton CV, puis on te contacte pour un échange téléphonique, puis un entretien et enfin un échange avec toute l'équipe.\n Tu as déjà une expérience dans la gestion de projets et/ou la relation client (2 à 3 ans y compris une alternance). Ton intérêt pour la business intelligence et les bases de données seront des atouts essentiels.\n Tu souhaites intégrer une équipe jeune et dynamique, bénéficier d'une organisation du travail flexible (principalement télétravail), dans une atmosphère conviviale, tu es doté.e d'un excellent relationnel, d'une nature conviviale et bienveillante, et tu es autonome tout en appréciant le travail en équipe, nous t'attendons !\n Type d'emploi : Temps plein, CDI\n Salaire\xa0: à partir de 29\xa0000,00€ par an\n Avantages\xa0:\n * Horaires flexibles\n * Travail à Distance\n Horaires\xa0:\n * Du Lundi au Vendredi\n Télétravail:\n * Oui\n", 'dateCreation': '2021-05-08T11:42:10.000Z', 'dateActualisation': '2021-05-08T11:42:10.000Z', 'lieuTravail': {'libelle': '33 - BORDEAUX', 'latitude': 44.851895, 'longitude': -0.587877, 'codePostal': '33000', 'commune': '33063'}, 'romeCode': 'M1804', 'romeLibelle': 'Études et développement de réseaux de télécoms', 'appellationlibelle': 'Chef de projet télécoms', 'entreprise': {}, 'typeContrat': 'CDI', 'typeContratLibelle': 'Contrat à durée indéterminée', 'natureContrat': 'Contrat travail', 'experienceExige': 'E', 'experienceLibelle': 'Expérience exigée de 2 An(s)', 'salaire': {}, 'dureeTravailLibelleConverti': 'Temps plein', 'alternance': False, 'nombrePostes': 1, 'accessibleTH': False, 'origineOffre': {'origine': '2', 'urlOrigine': 'https://candidat.pole-emploi.fr/offres/recherche/detail/4353214', 'partenaires': [{'nom': 'INDEED', 'url': 'https://fr.indeed.com/job/chef-de-projet-data-junior-37ac93b30d50c2d4?from=poleemploi', 'logo': 'https://www.pole-emploi.fr/static/img/partenaires/indeed80.png'}]}}, {'id': '4351779', 'intitule': 'Data Engineer H/F', 'description': "Envie d'un nouveau challenge ?\n Voulez-vous exercer un travail dans un secteur à la pointe de la technologie ?\n ENTREPRISE:\n Groupe dans le secteur pharmaceutique.\n PROFIL : \n Ecole d'ingénieur ou équivalent universitaire en informatique\n 3 ans + d'expérience sur un poste similaireExpertise sur l'architecture DATA (création des systèmes, développent, teste mise en service)\n Maitrise de toute la chaine de gestion de la DATA\n Expertise en Phyton R, environnement AWS, JavaScriptConnaissance en Apache (Spark, Cassandra et Talend)\n Maitrise des environnements Google (GCP) et Amazon (AWS S3)Anglais bon niveau\n OFFRE:\n Une entreprise internationale offrant des perspectives de carrière\n Un contrat CDIUn salaire attractif.\n Type d'emploi : Temps plein, CDI\n Salaire\xa0: 40\xa0000,00€\xa0à\xa060\xa0000,00€\xa0par mois\n Horaires\xa0:\n * Travail en journée\n Télétravail:\n * Non\n", 'dateCreation': '2021-05-08T11:09:47.000Z', 'dateActualisation': '2021-05-10T05:37:58.000Z', 'lieuTravail': {'libelle': '69 - LYON 01', 'latitude': 45.758, 'longitude': 4.835, 'codePostal': '69001', 'commune': '69381'}, 'romeCode': 'M1403', 'romeLibelle': 'Études et prospectives socio-économiques', 'appellationlibelle': 'Data analyst', 'entreprise': {}, 'typeContrat': 'CDI', 'typeContratLibelle': 'Contrat à durée indéterminée', 'natureContrat': 'Contrat travail', 'experienceExige': 'E', 'experienceLibelle': 'Expérience exigée de 3 An(s)', 'salaire': {}, 'dureeTravailLibelleConverti': 'Temps plein', 'alternance': False, 'nombrePostes': 1, 'accessibleTH': False, 'origineOffre': {'origine': '2', 'urlOrigine': 'https://candidat.pole-emploi.fr/offres/recherche/detail/4351779', 'partenaires': [{'nom': 'INDEED', 'url': 'https://fr.indeed.com/job/data-engineer-hf-7f348cf7be766c59?from=poleemploi', 'logo': 'https://www.pole-emploi.fr/static/img/partenaires/indeed80.png'}]}}], 'filtresPossibles': [{'filtre': 'typeContrat', 'agregation': [{'valeurPossible': 'CDD', 'nbResultats': 111}, {'valeurPossible': 'CDI', 'nbResultats': 827}, {'valeurPossible': 'LIB', 'nbResultats': 2}, {'valeurPossible': 'MIS', 'nbResultats': 51}]}, {'filtre': 'experience', 'agregation': [{'valeurPossible': '0', 'nbResultats': 179}, {'valeurPossible': '1', 'nbResultats': 277}, {'valeurPossible': '2', 'nbResultats': 375}, {'valeurPossible': '3', 'nbResultats': 160}]}, {'filtre': 'qualification', 'agregation': [{'valeurPossible': '0', 'nbResultats': 103}, {'valeurPossible': '9', 'nbResultats': 123}, {'valeurPossible': 'X', 'nbResultats': 765}]}, {'filtre': 'natureContrat', 'agregation': [{'valeurPossible': 'E1', 'nbResultats': 920}, {'valeurPossible': 'E2', 'nbResultats': 65}, {'valeurPossible': 'FS', 'nbResultats': 3}, {'valeurPossible': 'FV', 'nbResultats': 1}, {'valeurPossible': 'NS', 'nbResultats': 2}]}]}
        
        
        filename = str(datetime.now()).split('.')[0].replace(' ','_').replace(':','-')+ '_result.csv'
        data_to_save = {'data_job_type':'', 'confidence_interval':'','origine_offre':'', 'id':'', 'intitule':'',\
                        'description':'', 'description_en':'', 'dateCreation':'', 'dateActualisation':'','lieuTravail_libelle':'',\
                        'lieuTravail_codePostal':'', 'romeCode':'', 'romeLibelle':'', 'appellationlibelle':'', 'entreprise_nom':'', 'entreprise_description':'',\
                        'typeContrat':'', 'typeContratLibelle':'', 'natureContrat':'', 'origineOffre_partenaires':'', 'experienceExige':'', 'experienceLibelle':'',\
                        'experienceCommentaire':'', 'formations':'', 'langues':'', 'permis':'', 'outilsBureautiques':'', 'competences':'', 'salaire':'',\
                        'dureeTravailLibelle':'', 'complementExercice':'', 'conditionExercice':'', 'alternance':'', 'contact':'', 'nombrePostes':'',\
                        'accessibleTH':'', 'deplacementCode':'', 'deplacementLibelle':'', 'qualificationLibelle':'', 'secteurActivite':'',\
                        'secteurActiviteLibelle':'', 'qualitesProfessionnelles':'' }
        
        classification = []
        
        itLoader = ItemLoader(item=PoleemploiApiItem(), response=response)
                
        with open(filename, 'w', encoding='utf8') as f:
            
            header_line = 'data_job_type\;confidence_interval\;origin_offer\;offer_id\;title\;description\;description_en\;creation_date\;update_date\;pow_town\;\
                    pow_postal_code\;rome_code\;rome_job_label\;rome_name_label\;company_name\;company_description\;contract_type_code\;\
                    contract_type_label\;nature_contract_label\;partners_name_of_offer\;experience_required\;experience_label\;experience_comment\;\
                    list_of_trainings\;list_of_languages\;list_of_permits\;list_of_office_tools_used\;list_of_competencies\;salary\;working_time\;\
                    commentary_on_the_conditions_of_exercise\;conditions_of_exercise\;alternation\;list_of_contacts\;number_of_positions\;handi_friendly\;\
                    trip_code\;name_of_trip\;qualification_label\;activity_area\;activity_area_label\;list_of_professional_skills\;\n'
            
            f.write(header_line.replace(' ',''))
            
            
            separator = ' / '
            temp = []

            for i in self.all_lists_of_offers:
                
                for j in i:
                    
                    #Reset dataToSave :
                    for data_reset in data_to_save:
                        data_to_save[data_reset]=''
                    
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
                                
                                if k == 'id':
                                    id_offer = j[k].strip()
                                
                                if k == 'description':
                                    data_to_save[k+'_en'] = self.offers_Description_In_En[id_offer].replace(',',';')
                                    #data_to_save[k+'_en'] = ''
                                    
                            
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
                                    print('partenaires',j[k][l])                                  
                                    temp.clear()
                                    
                                    for i in j[k][l]:
                                        if 'nom' in i:
                                            temp.append(i['nom'])                                    
                                    
                                    data_to_save[k+'_'+l] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                                    
                        elif k == 'formations':                            
                            print('formations',j[k])
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
                            print('langues',j[k])                            
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                            
                        elif k == 'permis':                            
                            print('permis',j[k])                            
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                        
                        elif k == 'competences':                            
                            print('competences',j[k])                            
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'exigence' in i:
                                    temp.append(i['exigence'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                        
                        elif k == 'salaire':
                            print('salaire',j[k]) 
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
                            print('alternance',j[k])
                        
                        elif k == 'contact':                            
                            print('contact',j[k]) 
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
                            print('qualitesProfessionnelles',j[k])                            
                            temp.clear()
                                    
                            for i in j[k]:
                                if 'libelle' in i:
                                    temp.append(i['libelle'])
                                if 'description' in i:
                                    temp.append(i['description'])                                    
                                    
                            data_to_save[k] = separator.join(temp).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';')
                        
                    
                        
                     
                        
                    #TODO : ce code est a bouger de place normalement :
                                       
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
                    
                    
                        
                    for data_to_write in data_to_save:
                        f.write(data_to_save[data_to_write])
                        f.write('\;')
                        
                    f.write('\n')
                            
                            
                            
                            
                        
                        #if isinstance(j[k], str):                        
                        #    f.write(j[k].strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';'))
                        #else:
                        #    f.write(str(j[k]).strip().translate(str.maketrans("\n\t\r", "   ")).replace(',',';'))
                        #    
                    
                        #if str(k) == 'salaire':
                        #    print(f'################ annonce numéro{nb}')
                        #    nb+=1
                        #    f.write('\n')
                        #    break
                        #else:
                        #    f.write('\;')
                            
    

        
        print('classification_and_parse_offer_details ########################################################################### ENNNNNNNNNDDDD')
        self.step = 'classification_and_parse_offer_details'
        #pass
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



    
    if len(classification) == 0:
        classification.append('')
        classification.append('')
    #elif len(classification) == 1:
    #    classification.append('')
    
    return classification


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
    
def are_present(text, *argv):
    
    for arg in argv:
   
        if text.find(arg) == -1:
           return 0

    return 1


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