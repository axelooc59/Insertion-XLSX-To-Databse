# Script python to insert Data from an excel file in a MySQL DB
Using *mysql.connector* library

Script was made in the context of my internship at **CEBI Luxembourg**

Goal of this script :

Currently at Cebi we have to get manually Article and Client data from the ERP AS400. It generate a XLSX file containing data of interest and with that file we can fill both table of the database.

We wanted to make this process automatically, without any human intervention.

My job was first to create this script that with a XLSX file insert data into both Table.
Then to configurate Chron in linux to run this script one time per day automatically.

Notes:
- It generate a Log file when you can see what the script done and weither he succeed or not
- Back up support avoid to let table void after getting a possible exception during insertion. We use for that the last file which been succesfully inserted. When we succeed a new insertion we replace the old backup file with the new one.
 








