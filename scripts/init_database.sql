/*
=========================================
Create Database and Schemas
=========================================

Script purpose:
     This script creates a new databse named 'DataWarehouse', then go ahead and sets up three schemas within the database: 'bronze', 'silver',  and 'gold'

I assumed you do not have a database named: DataWarehouse, if yyou do, you could drop it and create a new one or change the name!




*/

CREATE DATABASE DataWarehouse;

-- create the schemas

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

