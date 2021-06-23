import os
import pandas as pd, csv, sqlite3
import numpy as np
# names of databases accidents
# names of the tables we collect from the site
names = ['caracteristiques', 'lieux', 'vehicules', 'usagers']

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq

from random import *
from datetime import datetime


# function to get page soup from html page
def get_page_soup(url):

    # opening connection
    uClient = uReq(url)
    page_html = uClient.read()
    uClient.close()
    #html parser
    return soup(page_html, "html.parser")


# function returning the list of liens to the tables with data
def liens_list():
    #set the url for site gouv.fr
    url = "https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2019/#_"

    page = get_page_soup(url)
    articles = page.findAll("article")
    liens = [[], [], [], []]
    for a in articles:
        if a.div.h4 != None:
            name = a.div.h4.text.split('-')[0].split('_')[0]
        for i, n in enumerate(names):
            if name == n:
                lien = a.footer.find("div",
                                     {"resource-card-actions btn-toolbar"
                                      }).findAll("a")[1]['href']
                liens[i].append(lien)
    return liens


class AccidentsDataBase:
    def __init__(self, name='accidents.sqlite3'):
        self.name_db = name

        if os.path.isfile(
                self.name_db):  # if sqlite file exist create a connection
            self.connection = self.__connect_db()
            print(self.connection)

        else:  # if file sqlite doesn't exist create file and then create sql

            open(self.name_db, 'x')
            self.connection = self.__connect_db()
            print(self.connection)
            self.__create_db()
            self.prepare_db()

        self.cur = self.connection.cursor()

    def get_tables_names(self):
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")

        #store all names of the tables/chemas in var tables
        tables0 = self.cur.fetchall()

        tables = []
        for tab in tables0:
            tables.append(tab[0])

        return tables
    
    def get_merged_df(self):
        lieux = self.get_df('lieux')
        caracteristiques = self.get_df('caracteristiques')
        caracteristiques_lieux = lieux.join(caracteristiques.set_index('Num_Acc'), on = 'Num_Acc')
        
        
        #usagers_vehichiles = usagers.join(vehicules.set_index('Num_Veh'), on = 'Num_Veh')
        return caracteristiques_lieux

    def get_df(self, table_name):
        return pd.read_sql_query(f"SELECT * FROM {table_name}",
                                 self.connection)

    def __connect_db(self):
        return sqlite3.connect(self.name_db)

    # to create data base on your laptop, call function create_data_base,
    # do not forget to create empty file with name accidents.sqlite3 in the same directory your notebook is.
    def __create_db(self):
        #Import libraries
        import pandas as pd, csv, sqlite3

        # Create sqlite database and cursor
        c = self.connection.cursor()

        #c.execute("""DROP TABLE IF EXISTS caracteristiques, lieux, vehicules, usagers;""")
        c.execute(
            """CREATE TABLE IF NOT EXISTS caracteristiques (Num_Acc INTEGER, jour INTEGER, mois INTEGER, an INTEGER, hrmn TEXT, lum INTEGER, dep TEXT, com TEXT, agg INTEGER, int INTEGER, atm INTEGER, col INTEGER, adr TEXT, gps TEXT, lat TEXT, long TEXT)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS lieux (Num_Acc INTEGER, catr INTEGER, voie REAL, v1 REAL, v2 TEXT, circ REAL, nbv REAL, pr REAL, pr1 REAL, vosp REAL, prof REAL, plan REAL, lartpc REAL, larrout REAL, surf REAL, infra REAL, situ REAL, env1 REAL, vma INTEGER)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS vehicules (Num_Acc INTEGER, id_vehicule TEXT, senc REAL, catv INTEGER, occutc INTEGER, obs REAL, obsm REAL, choc REAL, manv REAL, num_veh TEXT, motor INTEGER)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS usagers (Num_Acc INTEGER, id_vehicule TEXT, place REAL, catu INTEGER, grav INTEGER, sexe INTEGER, trajet REAL, secu REAL, secu1 REAL, secu2 REAL, secu3 REAL, locp REAL, actp REAL, etatp REAL, an_nais REAL, num_veh TEXT)"""
        )
        self.connection.commit()

        for i, lien in enumerate(liens_list()):  #names 0,1,2,3
            for j, l in enumerate(lien):  #years 0-15
                print("fichier {} - {}".format(names[i], 2019 - j))
                #print(l)
                try:
                    df = pd.read_csv(
                        l, sep=';', low_memory=False
                    )  #, encoding = 'ISO-8859-1', error_bad_lines=False, quotechar='"'

                    if (len(df.columns) < 2):
                        df = pd.read_csv(l, sep=',', low_memory=False)
                except:
                    try:
                        df = pd.read_csv(l,
                                         sep=',',
                                         encoding='ISO-8859-1',
                                         engine='python')
                        print('exeption1')
                    except:
                        df = pd.read_csv(l,
                                         sep='\t',
                                         encoding='ISO-8859-1',
                                         engine='python')
                        print('exeption2')

                df.to_sql(names[i],
                          self.connection,
                          if_exists='append',
                          index=False)

        print(f'Data base {self.name_db} was created')


    def prepare_db(self):
        #do if your data base is not clean yet
        try:
            self.clean_caracteristiques()
        except:
            print('Error to clean caracteristiques table')
            
        try:
            self.clean_lieux()
        except:
            print('Error to clean lieux table')
            
        ### do clean of the rest tables here
        
    
    def clean_caracteristiques(self):
        caracteristiques = self.get_df('caracteristiques')
        
        drop_caracteristiques = ['gps', 'adr', 'com']

        new_values = \
    ['01', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
        '02', '21', '22', '23', '24', '25', '26', '27', '28', '29',
        '2A', '2B',
        '03', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
        '04', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
        '05', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59',
        '06', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
        '07', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79',
        '08', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
        '09', '90', '91', '92', '93', '94', '95',
        'QP', 'MQ', 'GF', 'RE', 'YT']

        old_values = \
    ['10', '100', '110', '120', '130', '140', '150', '160', '170', '180', '190',
        '20', '210', '220', '230', '240', '250', '260', '270', '280', '290',
        '201', '202',
        '30', '300', '310', '320', '330', '340', '350', '360', '370', '380', '390',
        '40', '400', '410', '420', '430', '440', '450', '460', '470', '480', '490',
        '50', '500', '510', '520', '530', '540', '550', '560', '570', '580', '590',
        '60', '600', '610', '620', '630', '640', '650', '660', '670', '680', '690',
        '70', '700', '710', '720', '730', '740', '750', '760', '770', '780', '790',
        '80', '800', '810', '820', '830', '840', '850', '860', '870', '880', '890',
        '90', '900', '910', '920', '930', '940', '950',
        '971', '972', '973', '974', '976']

        # drop columns
        caracteristiques = caracteristiques.drop(drop_caracteristiques, axis=1)
        # clean dep
        caracteristiques.dep = caracteristiques.dep.replace(old_values, new_values)
        # clean an
        caracteristiques.loc[(caracteristiques.an < 1000),
                             'an'] = caracteristiques.an + 2000
        caracteristiques.an = caracteristiques.an.astype('int')

        # hrmn
        for ind in caracteristiques[(caracteristiques.hrmn.str.len() < 3)].index:
            #print(len(caracteristiques[(caracteristiques.hrmn.str.len() < 3)].index))
            #print(ind)
            if caracteristiques.loc[ind, 'lum'] == 1:
                hm = str(choice([9, 10, 11, 12, 13, 14, 15, 16, 17
                                 ])).zfill(2) + ':' + str(randint(0, 59)).zfill(2)
            if caracteristiques.loc[ind, 'lum'] == 2:
                hm = str(choice([6, 7, 19, 20])).zfill(2) + ':' + str(
                    randint(0, 59)).zfill(2)
            if caracteristiques.loc[ind, 'lum'] in [3, 4, 5]:
                hm = str(choice([23, 0, 1, 2, 3, 4])).zfill(2) + ':' + str(
                    randint(0, 59)).zfill(2)

            caracteristiques.loc[ind, 'hrmn'] = hm


        # separate numbers with : for time format
        caracteristiques.hrmn = [
            hm[:-2] + ':' + hm[-2:] if ':' not in hm else hm
            for hm in caracteristiques.hrmn
        ]

        ## change format str to time object

        caracteristiques.hrmn = [
            datetime.strptime(hm, '%H:%M').time()
            if type(hm) != type(datetime.now().time()) else hm
            for hm in caracteristiques.hrmn
        ]

        # weekday new columns
        caracteristiques['weekday'] = [datetime(an, mois, jour).weekday() for an,mois,jour in zip(caracteristiques.an, caracteristiques.mois, caracteristiques.jour)]

        #lat
        caracteristiques['lat'] = caracteristiques['lat'].str.replace(',','.')
        caracteristiques.lat = caracteristiques.lat.astype(float)
        caracteristiques.loc[(caracteristiques.lat > 10000),'lat'] = caracteristiques.lat/100000

        #long   
        caracteristiques['long'] = caracteristiques['long'].str.replace(',','.')
        caracteristiques['long'] = [np.nan if l == '-' else l for l in caracteristiques['long']]
        caracteristiques['long'] = caracteristiques['long'].astype(float)
        caracteristiques.loc[(caracteristiques['an'] != 2019),'long'] = caracteristiques['long']/100000

        #atm
        caracteristiques.atm = caracteristiques.atm.replace([np.nan,-1], 1)
        
        self.cur.execute("""DROP TABLE IF EXISTS caracteristiques;""")    
        caracteristiques.to_sql('caracteristiques',
                          self.connection, if_exists = 'replace',
                          index=False)
        
        
    def clean_lieux(self):
        
        lieux = self.get_df('lieux')
        
        # do cleaning here
        lieux = lieux.drop(['v1','v2','vma', 'env1'], axis = 1)
        
        lieux.catr = lieux.catr.replace(np.nan, 4)
        
        lieux.circ = lieux.circ.replace([np.nan, -1], 0)
        
        lieux.nbv = lieux.nbv.replace([-1.,np.nan], 0.)
        
        ## Assumtion 1
        ## Important assumtion cut all accidents where number of roads >10 or negative! Delete around 800 cases
        lieux = lieux[(lieux.nbv <= 10) | (lieux.nbv<0)]
        
        lieux.vosp = lieux.vosp.replace([-1., np.nan], 0.)
        lieux.vosp = lieux.vosp.astype(int)
        
        lieux.prof = lieux.prof.replace([np.nan,-1.], 1.)
        
        lieux.pr = lieux.pr.replace([np.nan,'(1)', ''], -1.0)
        lieux.pr1 = lieux.pr1.replace([np.nan,'(1)', ''], -1.0)
        
        lieux.plan= lieux.plan.replace([-1.,np.nan],0.)
        
        lieux.surf = lieux.surf.replace([np.nan,-1.,0.])

        lieux.infra = lieux.infra.replace([np.nan, -1.], 0.)                             
        lieux.situ = lieux.situ.replace([np.nan, -1], 1)       
                                        
        ## Assumtion 2
        #assumtion
        lieux.lartpc = lieux.lartpc.replace(np.nan, 0.)
        lieux.larrout = lieux.larrout.replace(np.nan, 0.)
        
        # drop old table
        self.cur.execute("""DROP TABLE IF EXISTS lieux;""")    
        
        #put clean table to the db
        lieux.to_sql('lieux',
                          self.connection, if_exists = 'replace',
                          index=False)
        
        
        
      
