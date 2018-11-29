import sqlite3
import csv
import json
import pprint
# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

def insert_test():
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    statement = '''
            CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT NOT NULL,
                'Region' TEXT NOT NULL,
                'Subregion' Text NOT NULL,
                'Population' INTEGER NOT NULL,
                'Area' REAL 
            );
        '''
    cur.execute(statement)

    statement = '''
            CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' TEXT NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'Ref' TEXT NOT NULL,
                'ReviewDate' TEXT NOT NULL,
                'CocoaPercent' REAL NOT NULL,
                'CompanyLocationId' INTEGER NOT NULL,
                'Rating' REAL NOT NULL,
                'BeanType' TEXT NOT NULL,
                'BroadBeanOriginId' INTEGER NOT NULL,
                FOREIGN KEY('CompanyLocationId') REFERENCES Countries ('Id'),
                FOREIGN KEY('BroadBeanOriginId') REFERENCES Countries ('Id')
                ); 
        '''
    cur.execute(statement)

    countries = json.load(open(COUNTRIESJSON, encoding='utf-8'))

    for country in countries:      
        insertion = (country["alpha2Code"], country["alpha3Code"], country["name"], country["region"], country["subregion"], country["population"], country["area"])
        
        statement = 'INSERT INTO "Countries" (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)'
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ? )'
        
        cur.execute(statement, insertion)
        conn.commit()


    with open(BARSCSV) as csvDataFile:
        next(csvDataFile)
        csvReader = csv.reader(csvDataFile)
        
        for row in csvReader:
            countryid = ""
            beanid = ""

            statement = 'SELECT ID from "Countries" WHERE EnglishName=?'
            countries = cur.execute(statement, (row[5],))
            for c in countries:
                countryid = (str(c[0]))
            
            statement = 'SELECT ID from "Countries" WHERE EnglishName=?'
            beans = cur.execute(statement, (row[8],))
            for b in beans:
                beanid = (str(b[0]))

            insertion = (row[0], row[1], row[2], row[3], row[4].rstrip("%"), countryid, row[6], row[7], beanid)
            statement = 'INSERT INTO "Bars" (Company, SpecificBeanBarName, Ref, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId)'
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? )'
            cur.execute(statement, insertion)
            conn.commit()


# Part 2: Implement logic to process user commands
def process_command(userinput):
    command_list = userinput.split()
    
    if command_list[0] == "bars":
        sellcountry = ""
        sourcecountry =""
        sellregion =""
        sourceregion =""
        ratings=""
        cocoa =""
        top = ""
        bottom =""

        for text in command_list:
            if "sellcountry" in text:
                sellcountry = text.split('=')[1]
            if "sourcecountry" in text:
                sourcecountry = text.split('=')[1]
            if "sellregion" in text:
                sellregion = text.split('=')[1]
            if "sourceregion" in text:
                sourceregion = text.split('=')[1]            
            if "ratings" in text:
                ratings = "Rating"
            if "cocoa" in text:
                cocoa = "CocoaPercent"
            if "top" in text:
                top = text.split('=')[1]
            if "bottom" in text:
                bottom = text.split('=')[1]
                        

        statement = '''SELECT B.SpecificBeanBarName,
        B.Company,
        C.EnglishName as CompanyLocation,
        B.Rating,
        B.CocoaPercent,
        C2.EnglishName as BroadBeanOrigin
        FROM BARS B 
        LEFT JOIN COUNTRIES C
        ON B.CompanyLocationId = C.Id
        LEFT JOIN COUNTRIES C2
        ON B.BroadBeanOriginId = C2.Id
        '''
        
        if sellcountry != "":
            statement += "WHERE C.ALPHA2 = '"+sellcountry+"' "
        elif sourcecountry != "":
            statement += "WHERE C2.ALPHA2 = '"+sourcecountry+"' "
        elif sellregion != "":
            statement += "WHERE C.Region = '"+sellregion+"' "
        elif sourceregion != "":
            statement += "WHERE C2.Region = '"+sourceregion+"' "
       
         
        if ratings != "":
            statement += "ORDER BY B.'"+ratings+"' "
        elif cocoa != "":
            statement += "ORDER BY B.'"+cocoa+"' "            
        else:
            statement += "ORDER BY B.'Rating' "

        if top != "":
            statement += "DESC LIMIT "+top+" "
        elif bottom != "":
            statement += "ASC LIMIT "+bottom+" "           
        else:
            statement += "DESC LIMIT 10"

        rows = cur.execute(statement)
        results = []
        new = []

        for row in rows:
            results.append(row)
            b = ["Unknown" if v is None else v for v in row]
            new.append(b)

        for row in new:
            print("{:23} {:23} {:23} {:23} {:23}{:23}".format(
                '{:10.10}'.format(row[0]) +'...',
                '{:10.10}'.format(row[1]) +'...',
                '{:10.10}'.format(row[2]) +'...',
                row[3],
                str(row[4]) +'%',
                '{:10.10}'.format(row[5]) +'...',
                ))


    elif command_list[0] == "companies":
        country = ""
        region =""
        ratings =""
        cocoa =""
        bars_sold=""
        top = ""
        bottom =""

        for text in command_list:
            if "country" in text:
                country = text.split('=')[1]
            if "region" in text:
                region = text.split('=')[1]        
            if "ratings" in text:
                ratings = "Rating"
            if "cocoa" in text:
                cocoa = "CocoaPercent"
            if "bars_sold" in text:
                bars_sold = "Bars_Sold"
            if "top" in text:
                top = text.split('=')[1]
            if "bottom" in text:
                bottom = text.split('=')[1]
                        

        statement = '''SELECT 
        B.Company,
		C.EnglishName as CompanyLocation,
        '''
        if ratings != "":
            statement += "  AVG(B.Rating) as Rating"
        elif cocoa != "":
            statement += "AVG(B.CocoaPercent) as CocoaPercent" 
        elif bars_sold != "":
            statement += "COUNT(B.SPECIFICBEANBARNAME) as Bars_Sold" 

        statement += '''
        FROM BARS B 
        LEFT JOIN COUNTRIES C
        ON B.CompanyLocationId = C.Id
       '''
        
        if country != "":
            statement += "WHERE C.Alpha2 = '"+country+"' "
        elif region != "":
            statement += "WHERE C.Region = '"+region+"' "
       
        
        statement += '''GROUP BY B.COMPANY 
		HAVING COUNT(B.SPECIFICBEANBARNAME) > 4 
        '''

        if ratings != "":
            statement += "ORDER BY "+ratings+" "
        elif cocoa != "":
            statement += "ORDER BY "+cocoa+" " 
        elif bars_sold != "":
            statement += "ORDER BY "+bars_sold+" "            
        else:
            statement += "ORDER BY Rating "

        if top != "":
            statement += "DESC LIMIT "+top+" "
        elif bottom != "":
            statement += "ASC LIMIT "+bottom+" "           
        else:
            statement += "DESC LIMIT 10"

        rows = cur.execute(statement)
        results = []
        new = []

        for row in rows:
            results.append(row)
            b = ["Unknown" if v is None else v for v in row]
            new.append(b)

        for row in new:
            print("{:23} {:23} {:23} ".format(
                '{:10.10}'.format(row[0]) +'...',
                '{:10.10}'.format(row[1]) +'...',
                '{:10.10}'.format(row[2])
                ))
        
    elif command_list[0] == "countries":
        region = ""
        sources = ""
        sellers =""
        ratings =""
        cocoa =""
        bars_sold=""
        top = ""
        bottom =""

        for text in command_list:
            if "region" in text:
                region = text.split('=')[1]
            if "sources" in text:
                sources = "BroadBeanOriginId"
            if "sellers" in text:
                sellers = "CompanyLocationId"      
            if "ratings" in text:
                ratings = "Rating"
            if "cocoa" in text:
                cocoa = "CocoaPercent"
            if "bars_sold" in text:
                bars_sold = "Bars_Sold"
            if "top" in text:
                top = text.split('=')[1]
            if "bottom" in text:
                bottom = text.split('=')[1]
                        

        statement = '''SELECT 
        C.EnglishName as Country,
		C.Region as Region,
        '''
        if ratings != "":
            statement += " AVG(B.Rating) as Rating "
        elif cocoa != "":
            statement += "AVG(B.CocoaPercent) as CocoaPercent " 
        elif bars_sold != "":
            statement += "COUNT(B.SPECIFICBEANBARNAME) as Bars_Sold " 

        statement += '''
        FROM BARS B 
        LEFT JOIN COUNTRIES C
       '''
        if sources != "":
            statement += " ON B.BroadBeanOriginId = C.Id "
        elif sellers != "":
            statement += " ON B.CompanyLocationId = C.Id " 
        else:
            statement += " ON B.CompanyLocationId = C.Id " 

        if region != "":
            statement += " WHERE C.Region = '"+region+"' "
       
        
        statement += '''GROUP BY COUNTRY, REGION
		HAVING COUNT(B.SPECIFICBEANBARNAME) > 4 
        '''

        if ratings != "":
            statement += "ORDER BY "+ratings+" "
        elif cocoa != "":
            statement += "ORDER BY "+cocoa+" " 
        elif bars_sold != "":
            statement += "ORDER BY "+bars_sold+" "            
        else:
            statement += "ORDER BY Rating "

        if top != "":
            statement += "DESC LIMIT "+top+" "
        elif bottom != "":
            statement += "ASC LIMIT "+bottom+" "           
        else:
            statement += "DESC LIMIT 10"

        print(statement)
        rows = cur.execute(statement)
        results = []
        new = []

        for row in rows:
            results.append(row)
            b = ["Unknown" if v is None else v for v in row]
            new.append(b)

        for row in new:
            print("{:23} {:23} {:23} ".format(
                '{:10.10}'.format(row[0]) +'...',
                '{:10.10}'.format(row[1]) +'...',
                row[2]
                ))

    elif command_list[0] == "regions":
        sources = ""
        sellers =""
        ratings =""
        cocoa =""
        bars_sold=""
        top = ""
        bottom =""

        for text in command_list:
            if "sources" in text:
                sources = "BroadBeanOriginId"
            if "sellers" in text:
                sellers = "CompanyLocationId"      
            if "ratings" in text:
                ratings = "Rating"
            if "cocoa" in text:
                cocoa = "CocoaPercent"
            if "bars_sold" in text:
                bars_sold = "Bars_Sold"
            if "top" in text:
                top = text.split('=')[1]
            if "bottom" in text:
                bottom = text.split('=')[1]
                        

        statement = '''SELECT 
		C.Region as Region,
        '''
        if ratings != "":
            statement += " AVG(B.Rating) as Rating "
        elif cocoa != "":
            statement += "AVG(B.CocoaPercent) as CocoaPercent " 
        elif bars_sold != "":
            statement += "COUNT(B.SPECIFICBEANBARNAME) as Bars_Sold " 

        statement += '''
        FROM BARS B 
        LEFT JOIN COUNTRIES C
       '''
        if sources != "":
            statement += " ON B.BroadBeanOriginId = C.Id "
        elif sellers != "":
            statement += " ON B.CompanyLocationId = C.Id " 
        else:
            statement += " ON B.CompanyLocationId = C.Id " 

        statement += "WHERE Region IS NOT NULL "

        statement += '''GROUP BY REGION
		HAVING COUNT(B.SPECIFICBEANBARNAME) > 4 
        '''

        if ratings != "":
            statement += "ORDER BY "+ratings+" "
        elif cocoa != "":
            statement += "ORDER BY "+cocoa+" " 
        elif bars_sold != "":
            statement += "ORDER BY "+bars_sold+" "            
        else:
            statement += "ORDER BY Rating "

        if top != "":
            statement += "DESC LIMIT "+top+" "
        elif bottom != "":
            statement += "ASC LIMIT "+bottom+" "           
        else:
            statement += "DESC LIMIT 10"

        
        print(statement)
        rows = cur.execute(statement)
        results = []
        new = []

        for row in rows:
            results.append(row)
            b = ["Unknown" if v is None else v for v in row]
            new.append(b)

        for row in new:
            print("{:23} {:23} ".format(
                '{:10.10}'.format(row[0]) +'...',
                row[1]
                ))
    else:
        print("Command not recognized")
        results = None

    return results


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue
        
        else:
            process_command(response)

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    insert_test()
    #process_command('regions sources bars_sold top=5')
    interactive_prompt()
    pass