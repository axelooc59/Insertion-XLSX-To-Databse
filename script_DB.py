import mysql.connector
import pandas as pd
from datetime import datetime
cod_client=pd.read_excel("Client_AS400.xlsx")
#cod_art=pd.read_excel("Article_AS400.xlsx")
#filtering useless data  
cod_client=cod_client[cod_client.iloc[:,-6:].isna().sum(axis=1)!=6]
now = datetime.now()
dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
logfile= "logInsert_"+dt_string+".log"
print()
print("date and time =", dt_string)	
f=open(logfile,"a")
f.write(f"Operation times :{now.strftime('%d/%m/%Y %H:%M:%S')}\n")


#connecting to databse
try:
    connection = mysql.connector.connect(host='XXXX',
                                         database='EDI_PORTAL',
                                         user='XXX',
                                         password='XXX')

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        #inserting data to the table COD_CLIENTS

       
        
        sql = "DELETE FROM TEST_COD_CLIENTS "
        cursor.execute(sql)
        connection.commit()
        print(cursor.rowcount, "record(s) was deleted in COD_CLIENTS.")
        f.write( f"{cursor.rowcount} record(s) was deleted in COD_CLIENTS.\n")
        
        sql = "INSERT INTO TEST_COD_CLIENTS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val=[]
        for i in range(len(cod_client)):
            elt=[str(e) for e in cod_client.iloc[i,:]]
            elt = list(map(lambda x: x.replace('nan', ''), elt))
            val.append(tuple(elt))
        try:
            cursor.executemany(sql, val)
            print(cursor.rowcount, "was inserted in COD_CLIENTS.")
            f.write(f"{cursor.rowcount} was inserted in COD_CLIENTS.\n")
        except Exception as e:
            print('Insert Error in COD_CLIENTS:', e)
            f.write(f'Insert Error in COD_CLIENTS:{e}')
        connection.commit()
        






        inserting data to the table COD_ARTICLES
         sql = "DELETE FROM TEST_ARTICLES "
         cursor.execute(sql)
         connection.commit()
         print(cursor.rowcount, "record(s) was deleted in COD_ARTICLES.")
         f.write( f"{cursor.rowcount} record(s) was deleted in COD_ARTICLES.\n")
         #inserting new data
         sql = "INSERT INTO TEST_ARTICLES VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
         val=[]
         for i in range(len(cod_art)):
             elt=[str(e) for e in cod_art.iloc[i,:]]
             elt = list(map(lambda x: x.replace('nan', ''), elt))
             val.append(tuple(elt))
         try:
           # cursor.executemany(sql, val)
           # print(cursor.rowcount, "was inserted in COD_ARTICLES.")
           # f.write(f"{cursor.rowcount} was inserted in COD_ARTICLES.\n")
         except Exception as e:
             print('Insert Error in COD_ARTICLES:', e)
             f.write(f'Insert Error in COD_ARTICLES:{e}')
         connection.commit()
        f.close()
        
         





                                
except Error as e:
    print("Error while connecting to MySQL", e)    

