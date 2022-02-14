# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PoleemploiApiItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    
    data_job_type = scrapy.Field()
    confidence_interval = scrapy.Field()
    origin_offer = scrapy.Field()
    offer_id = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    description_en = scrapy.Field()
    creation_date = scrapy.Field()
    update_date = scrapy.Field()
    pow_postal_code = scrapy.Field()
    pow_town = scrapy.Field()
    rome_code = scrapy.Field()
    rome_job_label = scrapy.Field()
    rome_name_label = scrapy.Field()
    company_name = scrapy.Field()
    company_description = scrapy.Field()
    contract_type_code = scrapy.Field()
    contract_type_label = scrapy.Field()
    nature_contract_label = scrapy.Field()
    partners_name_of_offer = scrapy.Field()
    experience_required = scrapy.Field()
    experience_label = scrapy.Field()
    experience_comment = scrapy.Field()
    list_of_trainings = scrapy.Field()
    list_of_languages = scrapy.Field()
    list_of_permits = scrapy.Field()
    list_of_office_tools_used = scrapy.Field()
    list_of_competencies = scrapy.Field()
    salary = scrapy.Field()
    working_time = scrapy.Field()
    commentary_on_the_conditions_of_exercise = scrapy.Field()
    conditions_of_exercise = scrapy.Field()
    alternation = scrapy.Field()
    list_of_contacts = scrapy.Field()
    number_of_positions = scrapy.Field()
    handi_friendly = scrapy.Field()
    trip_code = scrapy.Field()
    name_of_trip = scrapy.Field()
    qualification_label = scrapy.Field()
    activity_area = scrapy.Field()
    activity_area_label = scrapy.Field()
    list_of_professional_skills = scrapy.Field()
   
    pass
