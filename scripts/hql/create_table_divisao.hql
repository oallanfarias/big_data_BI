CREATE DATABASE IF NOT EXISTS DESAFIO_CURSO;

CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_DIVISAO ( 
    Division string,
    Division_Name string
    )
COMMENT 'Tabela de divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1")
;