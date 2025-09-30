/*
==============================
Create Database & Schema
==============================
Script Purpose
It Create a new database called DataWarehouse and schema called bronze,silver and gold

Warning
Running this script will drop the already existing database DataWarehouse and create a new one

*/



-- Create Database
Use master
 Create Database DataWarehouse
Use DataWarehouse
-- Create Schema
Create Schema Bronze
Go
Create Schema Silver
Go
Create Schema Gold
Go
