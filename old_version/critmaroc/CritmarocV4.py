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

driver.get('https://www.critmaroc.com/nos-offres/')
driver.implicitly_wait(10)
postes = []
secteurs =[]
localisations = []
experiences = []
postes_links =[]
date_pubs = []
mission = []
profil = []
contrats = []



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
    t2 = p.replace('Critmaroc-','').split(' ')[0]
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


def critmarocdata(current_working_dir):
    
    #s = str(datetime.now()).split(' ')[0]
    d = str(date.today())
    cpt = 0
    
    import glob, os
    current_working_dir = '/root/python_scripts/data_web_scrap/'
    list_of_files = glob.glob('./*.csv') # * means all if need specific format then *.csv
    current = max(list_of_files, key=os.path.getctime)
    print(current)
    
    
    t = get_difference_date(current,'Critmaroc')
    print(t)
    
    for i in range(2,110):
        try:
            poste = driver.find_element(By.XPATH, '//*[@id="txtHint"]/table/tbody/tr['+str(i)+']/td[2]/h5/a')
            postes.append(poste.text)
            postes_links.append(poste.get_attribute('href'))

        except Exception as e:
            print(f"Error : {e}")
            poste = "non spécifié"
            postes.append(poste)
            postes_links.append('non spécifié')
        try:
            secteur = driver.find_element(By.XPATH, '//*[@id="txtHint"]/table/tbody/tr['+str(i)+']/td[3]/span')
            secteurs.append(secteur.text)
        except Exception as e:
            print(f"Error : {e}")
            secteur = "non spécifié"
            secteurs.append(secteur)
        try:
            localisation = driver.find_element(By.XPATH, '//*[@id="txtHint"]/table/tbody/tr['+str(i)+']/td[4]/span')
            localisations.append(localisation.text)
        except Exception as e:
            print(f"Error : {e}")
            localisation = "non spécifié"
            localisations.append(localisation)
        try:
            experience = driver.find_element(By.XPATH, '//*[@id="txtHint"]/table/tbody/tr['+str(i)+']/td[5]/span')
            experiences.append(experience.text)
        except Exception as e:
            print(f"Error : {e}")
            experience="non spécifié"
            experiences.append(experience)
        try:
            date_pub = driver.find_element(By.XPATH, '//*[@id="txtHint"]/table/tbody/tr['+str(i)+']/td[2]/span')
            date_pubs.append(date_pub.text)
        except Exception as e:
            print(f"Error : {e}")
            date_pubs.append("non spécifié")
            
            
    
    for link in postes_links:
        if link!='non spécifié':
                driver.get(link)
                try:
                    alldiv =  driver.find_element(By.XPATH, '//*[@id="Content"]/div/div/div/div[3]/div/div/div/div[1]/div')
                    o = alldiv.text
                    a = o.find('Profil')
                    mission.append(o[:a])
                    profil.append(o[a:])
                except Exception as e:
                    print(f"Error : {e}")
                    mission.append('non spécifié')
                    profil.append('non spécifié')
                try:
                    contrat = driver.find_element(By.XPATH, '//*[@id="offre-contrat"]')
                    contrats.append(contrat.text)
                except Exception as e:
                    print(f"Error : {e}")
                    contrats.append('non spécifié')
        else:
            mission.append('non spécifié')
            profil.append('non spécifié')
            contrats.append('non spécifié')
            
            
        import pandas as pd
        jobs_list = pd.DataFrame(
            {
             'Poste': postes[:cpt+1],
             'Secteur': secteurs[:cpt+1],
                'Date de publication' :  date_pubs[:cpt+1],
                'Contrats':contrats,


             'Localisations': localisations[:cpt+1],
                'Experiences': experiences[:cpt+1],
                'Missions' : mission,
                'Profils' : profil





            })
        
        excel_data = jobs_list.to_csv("./"+"Critmaroc-"+d+".csv", encoding='utf-8-sig')
       
        
        df1 = pd.read_csv(current, encoding='utf-8-sig')
        #Drop of 'non spécifié' raws:
        D = []
        for x in range(len(df1)):
            #print(x)
            m = df1.iloc[x].tolist()
            if m[1:]==['non spécifié' for i in range(len(df1.columns)-1)]:
                 D.append(m[0])
        df1 = df1.drop(D)
        df2 = pd.read_csv("./"+"Critmaroc-"+d+".csv",encoding='utf-8-sig')
        
        
        import numpy as np
        
        df1 = df1.replace(np.nan, 'non spécifié')
        df2 = df2.replace(np.nan, 'non spécifié')
        
        columns = ["Poste","Secteur","Localisations","Experiences","Contrats","Missions","Profils"]
        for x in columns:
            df1[x] = df1[x].str.strip()
            df2[x] = df2[x].str.strip()
        
        current_row = df2.iloc[-1].tolist()[1:]

        if isreplicated(df1,df2):
            print(cpt)
            break
        cpt+=1
        
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
    df = spark.read.option("delimiter",",").option("encoding", "utf-8").option("parserLib", "univocity").option('escape','"').option("multiline",True).option("header",True).option("inferSchema","true").csv("./Critmaroc-"+d+".csv")
    df.repartition(1).write.mode('overwrite').parquet('./Critmarocparquet-'+d)
        
        
        
    #f2.to_parquet("/Users/macbook-pro/Desktop/oozie_f/files/"+'PLATFORMS/Critmaroc/'+"Critmaroc"+s+".parquet") 
            
        
    
    
        

import time
current_working_dir = './'
for i in range(1):
    critmarocdata(current_working_dir)
    print('data successfully collected at ' + time.ctime())


driver.close()

