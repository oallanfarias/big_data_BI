from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import when, col, sum, count, isnan, round, isnull, to_timestamp, unix_timestamp
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType, DecimalType, TimestampType, FloatType
from pyspark.sql import HiveContext
import os
import re
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import year, month, dayofmonth, quarter 
from pyspark.sql.functions import col, to_date, unix_timestamp

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

def salvar_df(df, file):
    output = "/input/big_data_BI/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/" + file + "/part-* /input/big_data_BI/gold/" + file + ".csv"
    print(rename)

    df.coalesce(1).write         .format("csv")         .option("header", True)         .option("delimiter", ";")         .mode("overwrite")         .save("/datalake/gold/" + file + "/")

    os.system(erase)
    os.system(rename)


#Vendas
df_vendas = spark.sql("""
                    SELECT DISTINCT * 
                    FROM desafio_curso.tbl_vendas
                    """)


#Clientes
df_clientes = spark.sql("""
                    SELECT DISTINCT * 
                    FROM desafio_curso.tbl_clientes
                    """)


#Endereço
df_endereco = spark.sql("""
                    SELECT * 
                    FROM desafio_curso.tbl_endereco
                    """)
#df_endereco.show(5)


#Região
df_regiao = spark.sql("""
                    SELECT * 
                    FROM desafio_curso.tbl_regiao
                    """)
#df_regiao.show(5)


#Divisão
df_divisao = spark.sql("""
                    SELECT * 
                    FROM desafio_curso.tbl_divisao
                    """)
#df_divisao.show(5)


 #Criando a df_stage

#Tabelas temporárias para VENDAS e CLIENTES
df_vendas.createOrReplaceTempView("tbl_vendas")
df_clientes.createOrReplaceTempView("tbl_clientes")


#VENDAS + CLIENTES
df_stage = spark.sql("""
                    SELECT DISTINCT v.*, 
                        c.address_number, 
                        c.business_family, 
                        c.business_unit,
                        c.customer,
                        c.customer_type, 
                        c.division, 
                        c.line_of_business, 
                        c.phone, 
                        c.region_code, 
                        c.regional_sales_mgr, 
                        c.search_type
                    FROM tbl_vendas AS v
                    INNER JOIN tbl_clientes AS c
                    ON v.customerkey = c.customerkey
                    """)
df_stage = df_stage.drop("c.customerkey")
#df_stage.show(5)

#Tabelas temporárias para ENDEREÇO 
df_endereco.createOrReplaceTempView("tbl_endereco")
df_stage.createOrReplaceTempView("df_stage")


#STAGE + ENDEREÇO
df_stage = spark.sql("""
                    SELECT DISTINCT s.*, 
                        e.city, 
                        e.country, 
                        e.customer_address_1, 
                        e.customer_address_2, 
                        e.customer_address_3, 
                        e.customer_address_4, 
                        e.state, 
                        e.zip_code
                    FROM df_stage AS s
                    LEFT JOIN tbl_endereco AS e
                    ON s.address_number = e.address_number
                    """)

# Remove a coluna duplicada 'address_number'
df_stage = df_stage.drop("e.address_number")
#df_stage.show(5)

#Tabela temporária para REGIÃO
df_regiao.createOrReplaceTempView("tbl_regiao")
df_stage.createOrReplaceTempView("df_stage")


#STAGE + REGIÃO
df_stage = spark.sql("""
                    SELECT DISTINCT s.*, r.region_name
                    FROM df_stage AS s
                    LEFT JOIN tbl_regiao AS r
                    ON s.region_code = r.region_code
                    """)
df_stage = df_stage.drop("r.region_code")
#df_stage.show(5)


#Tabela temporária para DIVISÃO
df_divisao.createOrReplaceTempView("tbl_divisao")
df_stage.createOrReplaceTempView("df_stage")


#STAGE + DIVISÃO
df_stage = spark.sql("""
                    SELECT DISTINCT s.*, 
                        d.division_name
                    FROM df_stage AS s
                    LEFT JOIN tbl_divisao AS d
                    ON s.division = d.division
                    """)

df_stag = df_stage.drop("d.division")
#df_stage.show(5)


#Aplicando regras


#Verificando Schema
df_stage.printSchema()

#Fazendo uma copia do df
df_stage2 = df_stage

#Convertendo colunas de data
data_columns = ['actual_delivery_date', 
                'datekey', 
                'invoice_date', 
                'promised_delivery_date']

date_format = "yyyy-MM-dd"

for c in data_columns:
    df_stage2 = df_stage2.withColumn(c, to_timestamp(col(c), date_format).cast(TimestampType()))

#Convertendo colunas de string
string_columns = ['customerkey',
                   'invoice_number',
                   'item_class', 
                   'item_number',
                   'item', 
                   'line_number',
                   'order_number',
                   'sales_quantity',
                   'sales_rep',
                   'u_m',
                   'address_number',
                   'business_family',
                   'business_unit',
                   'customer',
                   'customer_type',
                   'division',
                   'line_of_business',
                   'phone',
                   'regional_sales_mgr',
                   'search_type',
                   'city',
                   'country',
                   'customer_address_1',
                   'customer_address_2',
                   'customer_address_3',
                   'customer_address_4',
                   'state',
                   'zip_code',
                   'region_code',
                   'region_name',
                   'division_name']

for c in string_columns:
    df_stage2 = df_stage2.withColumn(c, col(c).cast(StringType()))


#Converter colunas de decimais
decimal_columns = ['discount_amount',
                   'list_price',
                   'sales_amount',
                   'sales_amount_based_on_list_price',
                   'sales_cost_amount',
                   'sales_margin_amount',
                   'sales_price']

for c in decimal_columns:
    df_stage2 = df_stage2.withColumn(c, col(c).cast(FloatType()))


#Converter colunas de inteiros
integer_columns = ['sales_quantity', 
                   'sales_rep', 
                   'business_unit', 
                   'division']

for c in integer_columns:
    df_stage2 = df_stage2.withColumn(c, col(c).cast(IntegerType()))


#Verificando o schema
df_stage2.printSchema()


#Campos strings vazios deverão ser preenchidos com 'Não informado'.

#Campos decimais ou inteiros nulos ou vazios, deversão ser preenchidos por 0.

#Gravar as informações em tabelas dm em formato cvs delimitado por ';'.

#Criando as colunas dia, mês, ano e trimestre para a dimensão tempo
df_stage2 = df_stage2.withColumn("data", to_date("invoice_date", "dd/MM/yyyy"))
df_stage2 = df_stage2.withColumn("dia", dayofmonth("data"))
df_stage2 = df_stage2.withColumn("mes", month("data"))
df_stage2 = df_stage2.withColumn("ano", year("data"))
df_stage2 = df_stage2.withColumn("trimestre", quarter("data")).drop(df_stage2.invoice_date)  


#Criação das chaves - PK_LOCALIDADE
df_stage2 = df_stage2.withColumn('PK_LOCALIDADE', sha2(concat_ws("", df_stage2.customer_address_1, df_stage2.customer_address_2, df_stage2.customer_address_3, df_stage2.customer_address_4, df_stage2.address_number, df_stage2.region_code, df_stage2.region_name, df_stage2.city, df_stage2.country, df_stage2.state, df_stage2.zip_code ), 256)) 

#Criação das chaves - PK_TEMPO
df_stage2 = df_stage2.withColumn('PK_TEMPO', sha2(concat_ws("", df_stage2.data, df_stage2.dia, df_stage2.mes, df_stage2.ano, df_stage2.trimestre), 256))

#Criação das chaves - PK_CLIENTES
df_stage2 = df_stage2.withColumn('PK_CLIENTES', sha2(concat_ws("", df_stage2.customerkey, df_stage2.customer, df_stage2.customer_type, df_stage2.phone, df_stage2.line_of_business), 256))

#Tabela temp para a fatos e as dimensões
df_stage2.createOrReplaceTempView("stage")

#FT_VENDAS
ft_vendas = spark.sql("""
                    SELECT PK_CLIENTES,
                            PK_TEMPO, 
                            PK_LOCALIDADE, 
                            COUNT(sales_price) AS VALOR_DE_VENDA 
                    FROM stage 
                    GROUP BY PK_CLIENTES, PK_TEMPO, PK_LOCALIDADE
                    """)

#CLIENTES
df_clientes = spark.sql("""
                        SELECT DISTINCT PK_CLIENTES,
                                        customerkey,
                                        customer,
                                        customer_type,
                                        phone,
                                        line_of_business
                        FROM STAGE
                        """)

#LOCALIDADE
df_localidade = spark.sql("""
                        SELECT DISTINCT PK_LOCALIDADE,
                                        customer_address_1,
                                        customer_address_2,
                                        customer_address_3,
                                        customer_address_4,
                                        address_number,
                                        region_code,
                                        region_name,
                                        city,
                                        country,
                                        state,
                                        zip_code
                        FROM STAGE
                        """)


df_tempo = spark.sql("""
                    SELECT DISTINCT PK_TEMPO,
                                    data,
                                    dia,
                                    mes,
                                    ano,
                                    trimestre
                    FROM STAGE
                    """)

#Enviando para a GOLD

#FT_VENDAS
salvar_df(ft_vendas, 'ft_vendas')

#DIM_CLIENTES
salvar_df(df_clientes, 'dim_clientes')

#DIM_TEMPO
salvar_df(df_tempo, 'dim_tempo')

#DIM_LOCALIDADE
salvar_df(df_localidade, 'dim_localidade')