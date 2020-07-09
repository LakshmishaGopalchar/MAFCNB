import pyodbc
import pandas as pd
import pyodbc
from flask import Flask
from flask import Flask, jsonify
#import spacy
from flask import request

# importing libraries
from bs4 import BeautifulSoup
#import urllib.request
#import re
import requests
from datetime import date

server = 'mafvdata.database.windows.net'
database = 'mafvaa_in'
username = 'aateam'
password = 'uKPdyRRNEK7qQ9xS'
#driver= '{ODBC Driver 17 for SQL Server}'
drivers = [item for item in pyodbc.drivers()]
driver = drivers[-1]
cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()



#Reel cinema

def ReelResult():
#    server = 'mafvdata.database.windows.net'
#    database = 'mafvaa_in'
#    username = 'aateam'
#    password = 'uKPdyRRNEK7qQ9xS'
    #driver= '{ODBC Driver 17 for SQL Server}'
 #   drivers = [item for item in pyodbc.drivers()]
#    driver = drivers[-1]
#    cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
 #   cursor = cnxn.cursor()


    URL = "https://www.reelcinemas.ae/en/showtime";
    #print(URL)

    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    r=soup.find('div',{'class':'js-loadCinamaListing showtime-blk'})

    cinemaList=[]
    for iterm in r.findAll('div',{'class':'tileview-movies-list'}):

        mallName=(iterm.find('div',{'class':'movielocation'})).find('span').text
        movieName=(iterm.find('div',{'class':'moviename'})).find('span').text
        locationmobile=(iterm.find('div',{'class':'locationmobile'})).text
        logoimg=(iterm.find('div',{'class':'logoimg'})).text
        #logoimg=(iterm.find('div',{'class':'logoimg'})).find('span').text
        showtimewrap=(iterm.find('div',{'class':'showtimewrap'}))
        sub_items = showtimewrap.findAll('li')
        for sub_item in sub_items:
            TIME=(sub_item.find('div',{'class':'showtime'})).text
            #print(TIME)
            nList=[]


            today = date.today()  
            nList=({'mallName':mallName,'movieName':movieName,'location':locationmobile,'logoimg':logoimg,'Showtime':TIME,'Date':today})
            cinemaList.append(nList)
    data=pd.DataFrame(cinemaList)

    cols = ",".join([str(i) for i in data.columns.tolist()])
    # Insert DataFrame recrds one by one.
    cols = ",".join([str(i) for i in data.columns.tolist()])
    for i,row in data.iterrows():
        SQLCommand = "INSERT INTO Cinema_Reel (" +cols + ") VALUES (?,?,?,?,?,?)"
        Values = row.tolist()  
        #Processing Query    
        cursor.execute(SQLCommand,Values)  
        # the connection is not autocommitted by default, so we must commit to save our changes
        cnxn.commit()  
        
        
def NovoResult():
    #Nova film
    cinemaList=[]
    URL = "https://uae.novocinemas.com/";
    #print(URL)

    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')

    price=soup.find("select",{"id":"drpMovie"})
    options = price.find_all("option")
    options1=[y.text for y in options]
    values = [o.get("value") for o in options]
    for x in range(1,len(values)):
        #print (options1[x], values[x])
        name=options1[x].replace(" ", "-")
        #print(name)
       # options1[x]
        URLN = "https://uae.novocinemas.com/movie/details/"+values[x]+"/"+name;
        #print(URLN)

        pageN = requests.get(URLN)
        soupN = BeautifulSoup(pageN.content, 'html.parser')

        #cinemaList=[]
        for iterm in soupN.findAll('div',{'class':'n-cinema-desc'}):

            location=(iterm.find('a',{'class':'n-cinema'})).text
            mallLocation=" ".join(location.split())
            #print(mallLocation)
            showtimewrap=(iterm.find('div',{'class':'collapse n-movie-timings'}))
            sub_items = showtimewrap.findAll('li')
            for sub_item in sub_items:
                TIME=(sub_item.find('a',{'class':'n-time'})).text
                Experience=(sub_item.find('span',{'class':'n-info-experience'})).text
                #print(Experience)
                nList=[]
                today = date.today()
                nList=({'mallName':mallLocation,'movieName':name,'location':mallLocation,'Showtime':TIME,'Experience':Experience,'Date':today})
                #print(nList)
                cinemaList.append(nList)
    NovoData=pd.DataFrame(cinemaList)
    cols = ",".join([str(i) for i in NovoData.columns.tolist()])
    # Insert DataFrame recrds one by one.
    for i,row in NovoData.iterrows():

        SQLCommand = "INSERT INTO Cinema_Nova (" +cols + ") VALUES (?,?,?,?,?,?)"
        Values = row.tolist()  
        #Processing Query    
        cursor.execute(SQLCommand,Values)  
        # the connection is not autocommitted by default, so we must commit to save our changes
        cnxn.commit()  

app = Flask(__name__)

app.debug = True

#app.run(debug=False)

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/ReelCinema")
def cinemaReel():
    ReelResult()
    
    return (("succesfully Reel data inserted")) 

@app.route("/NovoCinema")
def cinemaNovo():
    NovoResult()
    
    return (("succesfully Novo data inserted")) 
