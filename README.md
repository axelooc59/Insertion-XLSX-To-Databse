# Script python to insert Data from an excel file in a MySQL DB
Using mysql.connector library 

Script was made in the context of my internship at CEBI Luxembourg

Goal of this script :

Currently at Cebi we have to get manually Article and Client data from the ERP AS400. It generate a XLSX file containing data of interest and with that file we can fill both table of the database.

We wanted to make this process automatically, without any human intervention.

My job was first to create this script that with a XLSX file insert data into both Table.
Then to configurate Chron in linux to run this script one time per day automatically.






