#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from selenium import webdriver
from selenium.webdriver.chrome.service import Service

import pandas as pd
from datetime import date
from fake_useragent import UserAgent
from datetime import datetime
import glob, os
from selenium.webdriver.chrome.service import Service as ChromeService

path = '/usr/local/bin/chromedriver'
s = Service(path)
current_working_dir = './'

options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument('--headless')
options.add_argument(f'user-agent={UserAgent().random}')

driver = webdriver.Chrome(service=s,options=options)
driver.get('https://www.m-job.ma')


links_info = []
dates = []
locations = []
societes= []
names = []
contrats = []
missions = []
profils = []
descrips = []
salaires = []
secteurs = []
metiers = []
niv_exps = []
niv_etds = []
langues = []


import os
from datetime import datetime
from datetime import date

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
    t2 = p.replace('Mjob-','').split(' ')[0]
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


def mjobdata(current_working_dir):
    
    s = str(datetime.now()).split(' ')[0]
    #s = str(date.today())
    cpt = 0

    
    import glob, os
    # current_working_dir = '/root/python_scripts/data_web_scrap/'
    list_of_files = glob.glob(current_working_dir+'./*.csv')
    # * means all if need specific format then *.csv
    #print(list_of_files)
    current = max(list_of_files, key=os.path.getctime)
    print(current)
    
    
    t = get_difference_date(current,'Mjob')
    print(t)
    
    if t*5>=53:
       ss = 52
    if t*5<53:
       ss = t*5
    
    j=1
    for i in range(1,ss+1):
    
        driver.get('http://www.m-job.ma/recherche?page='+str(i))
        #links = driver.find_elements_by_tag_name('td')
        #for link in links:
        #20
        while (j<=20):
            try:
                link = driver.find_element_by_xpath('/html/body/section[2]/div[2]/div[1]/div[2]/div['+str(j)+']/div[1]/div[1]/h3/a').get_attribute('href')
                links_info.append(link)
                
            except:
                links_info.append('non spécifié')
            try:
                date = driver.find_element_by_xpath('/html/body/section[2]/div[2]/div[1]/div[2]/div['+str(j)+']/div[2]/div/span')
                dates.append(date.text)
            except:
                dates.append('non spécifié')
            try:
                location = driver.find_element_by_xpath('/html/body/section[2]/div[2]/div[1]/div[2]/div['+str(j)+']/div[1]/div[3]/div')
                locations.append(location.text)
            except:
                locations.append('non spécifié')
            j=j+1
        j=1
        
        
    for link in links_info:
        if link!='non spécifié':
            driver.get(link)
            try:
                societe =  driver.find_element_by_xpath('/html/body/section[2]/div/div/div[1]/div[2]/h1')
                societes.append(societe.text)
            except:
                societes.append('non spécifié')

            try:
                name = driver.find_element_by_xpath('/html/body/section[2]/div/div/div[1]/div[2]/ul/li[1]')
                names.append(name.text)
            except:
                names.append('non spécifié')


            try:
                contrat = driver.find_element_by_xpath('/html/body/section[2]/div/div/div[1]/div[2]/ul/li[2]')
                contrats.append(contrat.text)
            except:
                contrats.append('non spécifié')

            try:
                salaire = driver.find_element_by_xpath('/html/body/section[2]/div/div/div[1]/div[2]/ul/li[3]/h3')
                salaires.append(salaire.text)
            except:
                salaires.append('non spécifié')


            try:
                a = driver.find_element_by_xpath('/html/body/section[2]/div/div/div[2]').text
                titles =WebDriverWait(driver,20).until(EC.visibility_of_all_elements_located((By.XPATH,"//h3[contains(@class,'heading')]")))

                #Create a dictionnary that contains the infos about the offer
                F = []
                for names_ in titles:
                    #print(names.text)
                    F.append(names_.text.replace(' :','')) 
                #print(F)
                dicti= {}
                for i in range(0,len(F)-1):
                    y = a.find(F[i])
                    dicti[F[i]]=a[y+len(F[i])+2:a.find(F[i+1])].replace('\n','')
                yy = a.find(F[-1])
                z = dicti['Langue(s) exigée(s)'].find("L'offre")
                dicti['Langue(s) exigée(s)'] = dicti['Langue(s) exigée(s)'][:z]
                #dicti[F[-1]]= a[yy+len(F[-1])+1:]
                #dicti
                #['Le recruteur', 'Poste à occuper', 'Profil recherché', "Secteur(s) d'activité", 'Métier(s)', 'Langue(s) exigée(s)', 'Pièces jointes']


                if 'Le recruteur' in dicti:
                    descrips.append(dicti['Le recruteur'])
                else:
                    descrips.append('non spécifié')

                if 'Poste à occuper ' in dicti:
                    profils.append(dicti['Poste à occuper '])
                else:
                    profils.append('non spécifié')

                if 'Profil recherché' in dicti:
                    missions.append(dicti['Profil recherché'])
                else:
                    missions.append('non spécifié')

                if "Secteur(s) d'activité" in dicti:
                    secteurs.append(dicti["Secteur(s) d'activité"])
                else:
                    secteurs.append('non spécifié')

                if 'Métier(s)' in dicti:
                    metiers.append(dicti['Métier(s)'])
                else:
                    metiers.append('non spécifié')

                if 'Langue(s) exigée(s)' in dicti:
                    langues.append(dicti['Langue(s) exigée(s)'])
                else:
                    langues.append('non spécifié')

                if "Niveau d'expériences requis" in dicti:
                    niv_exps.append(dicti["Niveau d'expériences requis"])
                else:
                    niv_exps.append('non spécifié')

                if "Niveau d'études exigé" in dicti:
                    niv_etds.append(dicti["Niveau d'études exigé"])
                else:
                    niv_etds.append('non spécifié')

            except:
                descrips.append('non spécifié')
                profils.append('non spécifié')
                missions.append('non spécifié')
                metiers.append('non spécifié')
                niv_exps.append('non spécifié')
                niv_etds.append('non spécifié')
                secteurs.append('non spécifié')
                langues.append('non spécifié')
        else:
            contrats.append('non spécifié')
            societes.append('non spécifié')
            names.append('non spécifié')
            salaires.append('non spécifié')
            descrips.append('non spécifié')
            profils.append('non spécifié')
            missions.append('non spécifié')
            metiers.append('non spécifié')
            niv_exps.append('non spécifié')
            niv_etds.append('non spécifié')
            secteurs.append('non spécifié')
            langues.append('non spécifié')
            
        import pandas as pd
        jobs_list = pd.DataFrame(
            {'Postes':societes,
             "Secteur d'activité":secteurs,
             'Locations':locations[:cpt+1],
             'Dates' : dates[:cpt+1],
             'Societes' : names,
             'Contrats' : contrats,
             'Profils' : profils,
             'Missions' : missions,
             'Details entreprises':descrips,
             'Salaire' : salaires,
             'Metiers':metiers,
             "Niveau d'expérience":niv_exps,
             "Niveau d'études" : niv_etds, 
             "Langues exigées" : langues

            })
        
        
        excel_data = jobs_list.to_csv("./"+"Mjob-"+s+".csv", encoding='utf-8-sig')
        
        
        df1 = pd.read_csv(current, encoding='utf-8-sig')
        
        
        #Drop of 'non spécifié' raws:
        D = []
        for x in range(len(df1)):
            #print(x)
            m = df1.iloc[x].tolist()
            if m[1:]==['non spécifié' for i in range(len(df1.columns)-1)]:
                 D.append(m[0])
        df1 = df1.drop(D)
        
        df2 = pd.read_csv("./"+"Mjob-"+s+".csv",encoding='utf-8-sig')
        
        
        columns = ['Postes',"Secteur d'activité",'Locations','Societes',
                   'Contrats','Dates','Missions',"Profils",
                   'Details entreprises','Salaire','Metiers',
                   "Niveau d'expérience","Niveau d'études",'Langues exigées']

        for x in columns:
            df1[x] = df1[x].str.strip()
            df2[x] = df2[x].str.strip()
        
        #current_row = df2.iloc[-1].tolist()[1:]

        if isreplicated(df1,df2):
            break
            
        cpt+=1

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
    df = spark.read.option("delimiter",",").option("encoding", "utf-8").option("parserLib", "univocity").option('escape','"').option("multiline",True).option("header",True).option("inferSchema","true").csv("./Mjob-"+s+".csv")
    df.repartition(1).write.mode('overwrite').parquet('./Mjobparquet-'+s)







    #df2.to_parquet("/root/python_scripts/data_web_scrap/MJOB/"+"Mjob"+s+".parquet") 
        
        
    #import os
    #os.remove(current)
         
import time
for i in range(1):
    mjobdata()
    print('data successfully collected at ' + time.ctime())

