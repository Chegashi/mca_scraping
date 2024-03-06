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

# from selenium import webdriver

options = webdriver.ChromeOptions()

# # Run in headless mode
# options.add_argument('--headless')

# # Disable GPU (useful in headless environments) # options.add_argument('--disable-gpu')J
# # Set the window size
# options.add_argument('window-size=1200x600')

# # Open in incognito mode
# options.add_argument('--incognito')

# # Disable extensions
# options.add_argument('--disable-extensions')

# # Ignore SSL certificate errors
# options.add_argument('--ignore-certificate-errors')

# Initialize the driver with these options
driver = webdriver.Chrome(options=options)

date_pubs = []
postes = []
entreprises = []
lieux = []
links = []
npostes = []
entreprises = []
secteurs = []
Datedebuts = []
contrats = []
lieux = []
cardepostes = []
descriptionsprofils=[]
formations=[]
expprofess = []
langues=[]
competences=[]
postes  = []
salaires = []

def cleanlanguage(s):
    
    try:
        F = ['Francais','Arabe','Anglais','Tamazight','Français']
        L = {}
        for x in F:
            a = s.find(x)
            L[x]=a
        d = {}
        for k,v in L.items():
            if v!=-1:
                d[k]=v
        M = list({k: v for k, v in sorted(d.items(), key=lambda item: item[1])}.keys())
        dicti= {}
        for i in range(1,len(M)):
            y = s.find(M[i-1])
            dicti[M[i-1]]=s[y+len(M[i-1]):s.find(M[i])].strip()
        yy = s.find(M[-1])
        dicti[M[-1]]= s[yy+len(M[-1])+1:].strip()

        R = []
        for k,v in dicti.items():
            R = R + ([k+v])


        return ', '.join(R).replace('Partager sur :','')
    except Exception as e:
        print(f"Error : {e}")
        return s
    
    
    
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
    t2 = p.replace('Anapec-','').split(' ')[0]
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


    
def anapecdata(current_working_dir):
    
    #s = str(datetime.now())
    s =str(date.today())
    compteur = 0
    current_working_dir = './'
    list_of_files = glob.glob('./*.csv') # * means all if need specific format then *.csv
    current = max(list_of_files, key=os.path.getctime)
    print(current)
    
    
    t = get_difference_date(current,'Anapec')
    print(t)
    
    if t*5>=215:
        ss = 215
    if t*5<215:
        ss = t*5
    
    
    #i from 1 to 186:
    for i in range(1,ss+1):
    
        driver.get('http://anapec.org/sigec-app-rv/chercheurs/resultat_recherche/page:'+str(i)+'/tout:all/language:fr')
        for j in range(1,16):
            try:
                nposte = driver.find_element(By.XPATH, '//*[@id="myTable"]/tbody/tr['+str(j)+']/td[5]').text
                npostes.append(nposte)
            except Exception as e:
                print(f"Error : {e}")
                npostes.append('non spécifié')
            try:
                date_pub = driver.find_element(By.XPATH, '//*[@id="myTable"]/tbody/tr['+str(j)+']/td[3]')
                date_pubs.append(date_pub.text)
            except Exception as e:
                print(f"Error : {e}")
                date_pub = "non spécifié"
                date_pubs.append(date_pub)
            '''
            try:
                lieu = driver.find_element(By.XPATH, '//*[@id="myTable"]/tbody/tr['+str(j)+']/td[7]')
                lieux.append(lieu.text)
            except Exception as e:
                print(f"Error : {e}")
                lieu="non spécifié"
                lieux.append(lieu)
            '''
            try:
                link = driver.find_element(By.XPATH, '//*[@id="myTable"]/tbody/tr['+str(j)+']/td[2]/a')
                links.append(link.get_attribute('href'))
            except Exception as e:
                print(f"Error : {e}")
                links.append('non spécifié')
                
       
    for link in links:
        print(f":=link : {link}")
        if link!='non spécifié':
            driver.get(link)
            try:
                poste = driver.find_element(By.XPATH, '//*[@id="annonce_emploi"]/h5/p/span[1]').text
                postef = poste.find(')')
                postes.append(poste[postef+1:])
            except Exception as e:
                print(f"Error : {e}")
                postes.append('non spécifié')
                #Creation of details of post
            try:
                F = []
                a = driver.find_element(By.XPATH, '//*[@id="annonce_emploi"]').text.replace('\n','').replace('Description de Poste','').replace('Profil recherché','').replace('Partager sur :','')
                for elem in driver.find_elements_by_xpath('.//span[@class = "small_puce"]'):
                    F.append(elem.text.replace(':',''))

                dicti= {}
                for i in range(0,len(F)-1):
                    y = a.find(F[i])
                    dicti[F[i]]=a[y+len(F[i])+1:a.find(F[i+1])]
                yy = a.find(F[-1])
                dicti[F[-1]]= a[yy+len(F[-1])+1:]


                if 'Secteur d’activité ' in dicti:
                    secteurs.append(dicti['Secteur d’activité '])
                else:
                    secteurs.append('non spécifié')

                if 'Compétences ' in dicti:
                    competences.append(dicti['Compétences '])
                elif 'Compétences spécifiques ' in dicti:
                    #.replace('Partager sur :','')
                    competences.append(dicti['Compétences spécifiques '])
                else:
                    competences.append('non spécifié')

                if 'Type de contrat ' in dicti:
                    contrats.append(dicti['Type de contrat '])
                else:
                    contrats.append('non spécifié')

                if 'Lieu de travail ' in dicti:
                    lieux.append(dicti['Lieu de travail '])
                else:
                    lieux.append('non spécifié')

                if 'Salaire mensuel ' in dicti:
                    salaires.append(dicti['Salaire mensuel '])
                else:
                    salaires.append('non spécifié')

                if 'Caractéristiques du poste ' in dicti:
                    cardepostes.append(dicti['Caractéristiques du poste '])
                else:
                    cardepostes.append('non spécifié')

                if 'Formation ' in dicti:
                    #.replace('Partager sur :','')
                    formations.append(dicti['Formation '])
                else:
                    formations.append('non spécifié')

                if 'Expérience professionnelle ' in dicti:
                    expprofess.append(dicti['Expérience professionnelle '])
                else:
                    expprofess.append('non spécifié')

                if 'Langues ' in dicti:
                    langues.append(dicti['Langues '])
                else:
                    langues.append('non spécifié')

                if 'Date de début ' in dicti:
                    Datedebuts.append(dicti['Date de début '])
                else:
                    Datedebuts.append('non spécifié')
                    
            except Exception as e:
                print(f"Error : {e}")
                secteurs.append('non spécifié')
                competences.append('non spécifié')
                contrats.append('non spécifié')
                lieux.append('non spécifié')
                salaires.append('non spécifié')
                cardepostes.append('non spécifié')
                formations.append('non spécifié')
                expprofess.append('non spécifié')
                langues.append('non spécifié')
                Datedebuts.append('non spécifié')
                
       
        else:
            secteurs.append('non spécifié')
            postes.append('non spécifié')
            competences.append('non spécifié')
            contrats.append('non spécifié')
            lieux.append('non spécifié')
            salaires.append('non spécifié')
            cardepostes.append('non spécifié')
            formations.append('non spécifié')
            expprofess.append('non spécifié')
            langues.append('non spécifié')
            Datedebuts.append('non spécifié')
           
        
        jobs_list = pd.DataFrame({
            
         'Poste': postes,
         'Secteurs':secteurs,
         'nombre de postes':npostes[:compteur+1],

         'date publication': date_pubs[:compteur+1],
         'date de debut':Datedebuts,
         'lieux':lieux,
         
         #'salaires':salaires,
         
         'contrats': contrats,
         'description de profil': cardepostes,
         'formations':formations,
         'Expérience professionnelle':expprofess,
         'langues':langues,
         'competences spécifiques':competences
        })
        
        #today = date.today()
        #d1 = today.strftime("%d/%m/%Y")

        jobs_list['langues'] = jobs_list['langues'].apply(cleanlanguage)
        
        excel_data =jobs_list.to_csv(f"./Anapec-{s}.csv", encoding='utf-8-sig')
        
        
        df1 = pd.read_csv(current, encoding='utf-8-sig')
        
        #Drop of 'non spécifié' raws:
        D = []
        for x in range(len(df1)):
            #print(x)
            m = df1.iloc[x].tolist()
            if m[1:]==['non spécifié' for i in range(len(df1.columns)-1)]:
                 D.append(m[0])
        df1 = df1.drop(D)
        
        #df1['lieux'].iloc[1] = df1['lieux'].iloc[1].strip()
        #df1 = df1.drop(['Unnamed: 0'],axis=1)
        
        df2 = pd.read_csv(f"./Anapec-{s}.csv",encoding='utf-8-sig')
        #df1['lieux'].iloc[1] = df1['lieux'].iloc[1].strip()
        #df2=excel_data
        
        columns = ['Poste','Secteurs','lieux','contrats','description de profil','formations','langues','competences spécifiques']
        for x in columns:
            df1[x] = df1[x].str.strip()
            df2[x] = df2[x].str.strip()
        
        current_row = df2.iloc[-1].tolist()[1:]

        if isreplicated(df1,df2):
            #print(cpt)
            break
            
        compteur+=1

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
    df = spark.read.option("delimiter",",").option("encoding", "utf-8").option("parserLib", "univocity").option('escape','"').option("multiline",True).option("header",True).option("inferSchema","true").csv("./Anapec-"+s+".csv")
    df.repartition(1).write.mode('overwrite').parquet('./ANAPECparquet-'+s)
    #df.repartition(1).write.mode('overwrite').parquet('/root/python_scripts/data_web_code/Dataparquet13/ANAPECparquet-'+s) 
    #df2.to_parquet("/root/python_scripts/data_web_scrap/"+'ANAPEC/'+"Anapec"+s+".parquet")    
    #import os
    #os.remove(current)
    
    
import time
current_working_dir = './'

for i in range(1):
    anapecdata(current_working_dir)
    print('data successfully collected at ' + time.ctime())
   


    



