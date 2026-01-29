/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'Raw_data', 'Base_data', and 'Analytics_data'.
	
*/

USE master;
GO

-- Drop and recreate the 'DataBank' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataBank')
BEGIN
    ALTER DATABASE DataBank SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataBank;
END;
GO

-- Create the 'DataBank' database
CREATE DATABASE DataBank;
GO

USE DataBank;
GO

-- Create Schemas
CREATE SCHEMA Raw_Data;
GO

CREATE SCHEMA Base_Data;
GO

CREATE SCHEMA Analytics_Data;
GO

