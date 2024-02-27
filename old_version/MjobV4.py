#!/usr/bin/env python


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import date
from fake_useragent import UserAgent
from datetime import datetime
import glob, os
from selenium.webdriver.chrome.service import Service as ChromeService

path = './chromedriver'
# path = '/root/python_scripts/data_web_scrap/chromedriver'
s = Service(path)

from selenium import webdriver

options = webdriver.ChromeOptions()

# Run in headless mode
options.add_argument('--headless')

# Disable GPU (useful in headless environments)
options.add_argument('--disable-gpu')

# Set the window size
options.add_argument('window-size=1200x600')

# Open in incognito mode
options.add_argument('--incognito')

# Disable extensions
options.add_argument('--disable-extensions')

# Ignore SSL certificate errors
options.add_argument('--ignore-certificate-errors')

# Initialize the driver with these options
driver = webdriver.Chrome(options=options)




# # from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# # chrome_options = Options()
# # chrome_options.add_argument("--disable-extensions")
# # driver = webdriver.Chrome(chrome_options=chrome_options)

# options = webdriver.ChromeOptions()
# # set any options you need
# driver = webdriver.Chrome(options=options)


# driver = webdriver.Chrome(service=s,options=options)

# ofer_path = '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['

links = []
societes = [] 
postes = []
locations = []
date_pubs = []
postes_pros = []
secteurs = []
fonctions = []
exprequises = []
nivetudes = []
contrats = []
profils = []
entreprise_details = []
postes_details = []
traits_de_perso = []
adresses = []


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


def get_difference_date(file_prev,site_name):
    
    file_name = os.path.basename(file_prev)
    p = os.path.splitext(file_name)[0]
    today = date.today()
    # dd/mm/YY
    t1 = today.strftime("%Y-%m-%d")
    t2 = p.replace('Rekrute-','').split(' ')[0]
    if days_between(t1, t2)>0:
        return days_between(t1, t2)
    else:
        return days_between(t1, t2)+1
    
    
def isreplicated(df1,df2):
     
    current_row = df2.iloc[-1].tolist()[1:]

    for i in range(len(df1)):
        first_row = df1.iloc[i].tolist()[1:]
        if first_row==current_row:
            return True

    
def rekrutedata(current_working_dir):
    from datetime import datetime
    s = str(datetime.now()).split(' ')[0]
    cpt = 0
    
    import glob, os
    # current_working_dir = '/root/python_scripts/data_web_scrap/'
    # list_of_files = glob.glob(current_working_dir+'REKRUTE/*.csv') # * means all if need specific format then *.csv
    list_of_files = glob.glob('./*.csv') # * means all if need specific format then *.csv
    current = max(list_of_files, key=os.path.getctime)
    print(current)
    
    
    t = get_difference_date(current,'Rekrute')
    print(t)
    
    if t*5>158:
        ss = 158
    if t*5<158:
        ss = t*5
    
    #p from 1 to 159:
    for p in range(1,ss+1):
        print('___[','https://www.rekrute.com/offres.html?p='+str(p))
        driver.get('https://www.rekrute.com/offres.html?p='+str(p))
        
        for i in range(1,11):
            try:
                
                link =  driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[2]/div/h2/a')
                links.append(link.get_attribute('href'))
            except Exception as e:
                print(f"ERROR: {e}")
                links.append('non spécifié')
            try:
                poste = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[2]/div/h2/a')
                postes.append(poste.text.split('|')[0])
            except:
                postes.append("non spécifié")
            try:
                location  = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[2]/div/h2/a')
                locations.append(location.text.split('|')[1])
            except:
                locations.append('non spécifié')
            try:
                date_pub = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[2]/div/div/em')
                date_pubs.append(date_pub.text.split('|')[0])
            except:
                date_pubs.append('non spécifié')
            try:
                postes_pro = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[2]/div/div/em')
                postes_pros.append(postes_pro.text.split('|')[1][17:])
            except:
                postes_pros.append('non spécifié')
            try:
                secteur = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[2]/div/div/div[3]/ul')
                L = secteur.text.split('\n')
                M = []
                for x in L:
                    x = x.split(':')[1]
                    M.append(x)
                secteurs.append(M[0])
                fonctions.append(M[1])
                exprequises.append(M[2])
                nivetudes.append(M[3])
                contrats.append(M[4])

            except:
                secteurs.append('non spécifié')
                fonctions.append('non spécifié')
                exprequises.append('non spécifié')
                nivetudes.append('non spécifié')
                contrats.append('non spécifié')
            try:
                societe  = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div/div[3]/div/div[3]/ul/li['+str(i)+']/div/div[1]/a')
                societes.append(societe.get_attribute('href').split('/')[3].split('-')[0].upper())
            except:
                societes.append('non spécifié')
                # print(link)
    for link in links:
        if link!='non spécifié':
            driver.get(link)
            try:
                profil = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[3]/div[2]/div/div[5]')
                if profil.text.startswith('Profil recherché'):
                       profils.append(profil.text)
                else:
                    profil = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[3]/div[2]/div/div[6]')
                    profils.append(profil.text)

            except:
                profils.append('non spécifié')
            try:
                entreprise_detail = driver.find_element(By.XPATH, '//*[@id="recruiterDescription"]')
                entreprise_details.append(entreprise_detail.text)
            except:
                entreprise_details.append('non spécifié')
            try:
                f = driver.find_element(By.XPATH, '//*[@id="fortopscroll"]/div[3]/div[2]/div')
                if f!='':
                    a = f.text.find('Poste')
                    b = f.text.find('Profil recherché ')
                    postes_details.append(f.text[a:b-1])
                else:
                    f2 = driver.find_element(By.XPATH, '//*[@id="fortopscroll"]/div[3]/div[2]/div[2]')
                    a2 = f2.text.find('Poste')
                    b2 = f2.text.find('Profil recherché ')
                    postes_details.append(f2.text[a2:b2-1])

            except:
                postes_details.append('non spécifié')
            try:
                g = driver.find_element(By.XPATH, '//*[@id="fortopscroll"]/div[3]/div[2]/div')
                if g.text!='':
                    a = g.text.find('Traits de personnalité souhaités')
                    b  = g.text.find('Adresse')
                    traits_de_perso.append(''.join(g.text[a+len('Traits de personnalité souhaités :\n'):].split('\n')[0]))
                    adresses.append(''.join(g.text[b:a].split('\n')[1:]))
                else:
                    g2 = driver.find_element(By.XPATH, '//*[@id="fortopscroll"]/div[3]/div[2]/div[2]')
                    a2 = g2.text.find('Traits de personnalité souhaités')
                    b2  = g2.text.find('Adresse')
                    traits_de_perso.append(''.join(g2.text[a2+len('Traits de personnalité souhaités :\n'):].split('\n')[0]))
                    adresses.append(''.join(g2.text[b2:a2].split('\n')[1:]))

            except:
                traits_de_perso.append('non spécifié')
                adresses.append('non spécifié')
        else:
            profils.append('non spécifié')
            entreprise_details.append('non spécifié')
            postes_details.append('non spécifié')
            traits_de_perso.append('non spécifié')
            adresses.append('non spécifié')
            
        import pandas as pd
        jobs_list = pd.DataFrame(
            {
             'Societés' : societes[:cpt+1],
                'Postes' : postes[:cpt+1],
                'Adresses' : adresses,
                'Details de poste':postes_details,
                'Date de publication' : date_pubs[:cpt+1],
                'Location' : locations[:cpt+1],
                'Postes proposés': postes_pros[:cpt+1],
                "Secteur d'activité" : secteurs[:cpt+1],
                'Fonctions' : fonctions[:cpt+1],
                'Expérience requise' : exprequises[:cpt+1],
                'Niveau d’étude demandé' : nivetudes[:cpt+1],
                'Type de contrat proposé'  :contrats[:cpt+1],
                'Profil recherché' : profils,
                "Decsription d'entreprise" : entreprise_details,
                'Traits de personnalité' : traits_de_perso




            })

        jobs_list.replace('','non spécifié').replace('-','non spécifié')
        
        
        # excel_data = jobs_list.to_csv("/root/python_scripts/data_web_scrap/REKRUTE/"+"Rekrute-"+s+".csv", encoding='utf-8-sig')
        excel_data = jobs_list.to_csv("./"+"Rekrute-"+s+".csv", encoding='utf-8-sig')
       
        
        df1 = pd.read_csv(current, encoding='utf-8-sig')
        
        #Drop of 'non spécifié' raws:
        D = []
        for x in range(len(df1)):
            #print(x)
            m = df1.iloc[x].tolist()
            if m[1:]==['non spécifié' for i in range(len(df1.columns)-1)]:
                 D.append(m[0])
        df1 = df1.drop(D)
        
        df2 = pd.read_csv("./"+"Rekrute-"+s+".csv",encoding='utf-8-sig')
        
        
        import numpy as np
        
        df1 = df1.replace(np.nan, 'non spécifié')
        df2 = df2.replace(np.nan, 'non spécifié')
        
        columns = ["Societés","Postes","Adresses","Details de poste","Date de publication","Location","Postes proposés","Secteur d'activité","Fonctions","Expérience requise","Niveau d’étude demandé","Type de contrat proposé","Profil recherché","Decsription d'entreprise","Traits de personnalité"]
        for x in columns:
            df1[x] = df1[x].apply(str).str.lower()
            df1[x] = df1[x].apply(str).str.strip()
            df2[x] = df2[x].apply(str).str.lower()
            df2[x] = df2[x].apply(str).str.strip()
        
        current_row = df2.iloc[-1].tolist()[1:]

        if isreplicated(df1,df2):
            #print(cpt)
            break
        cpt+=1
        
        #if cpt>len(df1):
        #break
        
    print("Le nombre d offres ajouté est : "+str(cpt))
    
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
    df = spark.read.option("delimiter",",").option("encoding", "utf-8").option("parserLib", "univocity").option('escape','"').option("multiline",True).option("header",True).option("inferSchema","true").csv("./Rekrute-"+s+".csv")
    df.repartition(1).write.mode('overwrite').parquet('./Rekruteparquet-'+s)




    
    #df2.to_parquet("/root/python_scripts/data_web_scrap/REKRUTE/"+"Rekrute"+s+".csv"+s+".parquet")   
    
    #import os
    #os.remove(current)

import time
# current_working_dir = '/root/python_scripts/data_web_scrap/'
current_working_dir = './'

for i in range(1):
    rekrutedata(current_working_dir)
    print('data successfully collected at ' + time.ctime())
        

