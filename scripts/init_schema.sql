/*
=======================================================================
Create Schemas
=======================================================================
Script Purpose:
    This script creates the schemas required for the Data Warehouse project:
    bronze, silver, and gold. Each schema is created if it does not already exist.

    Note:
        The "warehouse" database is created at the infracture level through Docker Compose using
        PostgreSQL evironment variables. (POSTGRES_DB). This script assumes the database already 
        exists and that the connection is established.

    Schemas:
        - bronze: This schema is intended for raw, unprocessed data. It serves as the landing zone for all incoming data.
        - silver: This schema is designed for cleaned and transformed data. It contains tables that have been processed and are ready for analysis.
        - gold: This schema is reserved for aggregated and summarized data. It includes tables that are optimized for reporting and business intelligence purposes.
*/

-- Create schemas if they do not already exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;