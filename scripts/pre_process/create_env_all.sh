#!/bin/bash

# Criação das pastas

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
	echo "Ingestão do arquivo $i.csv"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f /input/big_data_BI/scripts/hql/create_table_$i.hql
done