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

Links = []
Postes= []
Societes = []
Dates = []
salaires = []
contrat_details = []
locations = []
missions = []
profils = []
all_details = []



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
    t2 = p.replace('Optioncarriere-','').split(' ')[0]
    if days_between(t1, t2)>0:
        return days_between(t1, t2)
    else:
        return days_between(t1, t2)+1
def isreplicated(df1,df2):
     
    # current_row = df2.iloc[-1].tolist()[1:]

    if not df2.empty:
        current_row = df2.iloc[-1].tolist()[1:]

    for i in range(len(df1)):
        first_row = df1.iloc[i].tolist()[1:]
        if first_row==current_row:
            return True

def optioncarrieredata(current_working_dir):
    
    s = str(datetime.now()).split(' ')[0]
    cpt = 0
    
    import glob, os
    # current_working_dir = '/root/python_scripts/data_web_scrap/'
    current_working_dir = './'

    list_of_files = glob.glob(current_working_dir+'*.csv') # * means all if need specific format then *.csv
    current = max(list_of_files, key=os.path.getctime)
    print(current)
    
    
    t = get_difference_date(current,'Optioncarriere')
    print(t)
    
    L = []
    M = []
    
    if t*5>100:
        ss = 100
    if t*5<100:
        ss = t*5

    # p for 1 to 101
    for p in range(1,ss+1):
        driver.get('https://www.optioncarriere.ma/emplois-maroc-113535.html?p='+str(p)+'')
        for i in range(1,25):
                try:
                    link = driver.find_element(By.XPATH, '//*[@id="search-content"]/ul[2]/li['+str(i)+']/article/header/h2/a').get_attribute('href')
                    Links.append(link)
                except Exception as e:
                    print(f"ERROR: {e}")
                    Links.append('non spécifié')
    for link in Links:
        if link!='non spécifié':
            driver.get(link)
            try:
                poste = driver.find_element(By.XPATH, '//*[@id="job"]/div/header/h1').text
                Postes.append(poste)
            except Exception as e:
                print(f"ERROR: {e}")
                Postes.append('non spécifié')

            try:
                soc = driver.find_element(By.XPATH, '//*[@id="job"]/div/header/p').text
                Societes.append(soc)
            except Exception as e:
                print(f"ERROR: {e}")
                Societes.append('non spécifié')

            try:
                date = driver.find_element(By.XPATH, '//*[@id="job"]/div/header/ul[2]/li/span').text
                Dates.append(date)
            except Exception as e:
                print(f"ERROR: {e}")
                Dates.append('non spécifié')

            html_list = driver.find_element(By.CLASS_NAME, "details")
            # items = html_list.find_elements_by_tag_name("li")
            items = html_list.find_elements(By.TAG_NAME, "li")
            F = []
            for item in items:
                text = item.text
                F.append(text)
            #print(F)
            if len(F)==3:
                locations.append(F[0])
                contrat_details.append(F[1])
                salaires.append('non spécifié')
            else:
                locations.append(F[0])
                salaires.append(F[1])
                contrat_details.append(F[2])


            try:
                d = driver.find_element(By.XPATH, '//*[@id="job"]/div/section[1]')
                a1 = d.text.find('Mission')
                a2 = d.text.find('mission')
                b = d.text.find('Profil')
                c=d.text[b:].find('.')
                mission1 = d.text[a1:b]
                mission2 = d.text[a2:b]
                profil = d.text[b:][:c]
                if a1!=-1:
                    missions.append(mission1)
                else:
                    missions.append(mission2)
                profils.append(profil)
                all_details.append(d.text)
            except Exception as e:
                print(f"ERROR: {e}")
                missions.append('non spécifé')
                profils.append('non spécifié')
                all_details.append('non spécifié')
        else:
            salaires.append('non spécifié')
            contrat_details.append('non spécifié')
            locations.append('non spécifié')
            missions.append('non spécifié')
            profils.append('non spécifié')
            #details.append('non spécifié')
            Postes.append('non spécifié')
            Societes.append('non spécifié')
            Dates.append('non spécifié')
            all_details.append('non spécifié')
        import pandas as pd
        jobs_list = pd.DataFrame(
        {'Société' : Societes,
         'Postes' : Postes,
         'Regions' : locations,
         'Date de publication' : Dates,
         'Contrat' : contrat_details,
         'Salaire' : salaires,
         'Profil' : profils,
         "Mission":missions,
         'Autres details': all_details
        })

        excel_data = jobs_list.to_csv("Optioncarriere-"+s+".csv", encoding='utf-8-sig')
       
        
        df1 = pd.read_csv(current, encoding='utf-8-sig')
        #Drop of 'non spécifié' raws:
        D = []
        for x in range(len(df1)):
            #print(x)
            m = df1.iloc[x].tolist()
            if m[1:]==['non spécifié' for i in range(len(df1.columns)-1)]:
                 D.append(m[0])
        df1 = df1.drop(D)
        
        df2 = pd.read_csv("Optioncarriere-"+s+".csv",encoding='utf-8-sig')
        
        
        import numpy as np
        
        df1 = df1.replace(np.nan, 'non spécifié')
        df2 = df2.replace(np.nan, 'non spécifié')
        
        #L = []
        for i in range(len(df1)):
            a = df1.iloc[i].tolist()[1:]
            if len(set(a))==1:
                L.append(i)
        df1_ = df1.drop([x for x in L])
            
        #M = []
        for i in range(len(df2)):
            b = df2.iloc[i].tolist()[1:]
            if len(set(b))==1:
                M.append(i)
        
        df2_ = df2.drop([x for x in M])
    
                
        '''
        columns = ["Société","Postes","Regions","Date de publication","Contrat","Salaire","Profil","Mission","Autres details"]
        for x in columns:
            df1_[x] = df1_[x].apply(str).str.lower()
            df1_[x] = df1_[x].apply(str).str.strip()
            df2_[x] = df2_[x].apply(str).str.lower()
            df2_[x] = df2_[x].apply(str).str.strip()
        '''
        
        #current_row = df2.iloc[-1].tolist()[1:]

        if isreplicated(df1_,df2_):
            #print(cpt)
            break
        cpt+=1
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
    df = spark.read.option("delimiter",",").option("encoding", "utf-8").option("parserLib", "univocity").option('escape','"').option("multiline",True).option("header",True).option("inferSchema","true").csv("/root/python_scripts/data_web_scrap/OPTIONCARRIERE/Optioncarriere-"+s+".csv")
    df.repartition(1).write.mode('overwrite').parquet('./OPTIONCARRIEREparquet-'+s)




        
    #df2.to_parquet("/root/python_scripts/data_web_scrap/OPTIONCARRIERE/"+"Optioncarriere"+s+".parquet")   
    #import os
    #os.remove(current)
import time
current_working_dir = './'
# current_working_dir = '/root/python_scripts/data_web_scrap/'

for i in range(1):
    optioncarrieredata(current_working_dir)
    print('data successfully collected at ' + time.ctime())
