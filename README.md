## PoleEmploi_API
Pole Emploi API scraper with scrapy

The purpose of this project is to request the API of pole emploi which is an aggregator of job offers in France. The pole emploi API is used to search for all job offers containing the keyword data.

The database obtained will provide a history of all the jobs, which will make it possible to solicit spontaneously the companies / types of positions that we would wish.
To do this I used a well-known and python-based scraper: scrapy (although we could have done it completely differently, it would probably have been easier!). Scrapy is a complete and very powerful scraper which makes it a little difficult to learn and to customize if necessary!

The advantage of Scrapy is that once you have mastered it a bit you can do the same on sites that don't have an API like Welcome to the jungle for example. 

Twice a month or so I run the scripts that get all of the announcements and store them in the Maria DB database.

## Run the script

In order the run the script you have to create a new python env and install the libraries :
- Scrapy
- mysql.connector

You have to install also the sofware MariaDB and create an appropriate DB

Copy all the files on your computer, activate your python env, select the path to the folder PoleEmploi_API\PoleEmploi_API and then run : 

```python
scrapy crawl dataJobSearch
```
