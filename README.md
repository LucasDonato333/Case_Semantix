#### Qual o objetivo do comando cache em Spark?
Cache é uma operação de otimização para cálculos Spark (iterativos e interativos). Eles ajudam a salvar resultados parciais para que possam ser reutilizados nos estágios consecutivos. A utilização do comando, ajuda na melhoria e eficiência do código, permitindo que resultados intermediários de operações lazy, sejam armazenados e reutilizados diversas vezes.

#### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
Spark chega a ser mais rápido que uma implementação equivalente em MapReduce. Isso ocorre porque Spark funciona na memoria e não em unidades de disco rígido. Já o MapReduce lê os dados do cluster, executa uma operação e escreve os resultados de volta no cluster.

#### Qual é a função do SparkContext ?
 O SparkContext configura serviços internos e estabelece uma conexão com um ambiente de execução do Spark Ele pode ser acessado como uma variável em um programa que para utilizar os seus recursos. 

#### Explique com suas palavras o que é Resilient Distributed Datasets (RDD)
É a parte de maior importância no Spark, pois nele são executados os processamentos de dados. RDD é uma coleção de registros particionados apenas para leitura.

#### GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
Ao aplicar  groupByKey ()  em um conjunto de dados de pares (K, V), os dados são arrastados aleatoriamente de acordo com o valor da chave K em outro  RDD . Nesta transformação, muitos dados desnecessários são transferidos pela rede.
O Spark fornece a provisão para salvar dados no disco quando há mais dados sendo embaralhados em um único executor do que na memória.
Ao aplicar  reduceByKey  em um conjunto de dados (K, V), antes de embaralhar os dados, os pares na mesma máquina com a mesma chave são combinados.

#### Explique o que o código Scala abaixo faz
```scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```
1-	Le o arquivo de texto.<br>
2-	Quebra a linha, dando um Split, realizando uma “lista” de palavras.<br>
3-	Transforma cada palavra em um conjunto de chave-valor, onde chave é igual a palavra e valor é igual a 1.<br>
4-	Pela função soma, faz uma agregação desses valores por chave<br>
5-	Salva o arquivo, realizando a contagem por palavra.<br>
<hr>

## Exercicios
#### Importação

```python
from pyspark.sql.functions import *
from pyspark.sql import functions as F
```

#### Transformando os arquivos de log em um DataFrame
##### JULHO
```python
arq_jul = sqlContext.read.text("wasb://nomedocluster@j4wqp6rby7ga2.blob.core.windows.net/HdiNotebooks/PySpark/access_log_Jul95")

df_Jul95 = arq_jul.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('HOST'),
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('TIMESTAMP'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('PATH'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('STATUS'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('SIZE'))
```
##### AGOSTO
```python
arq_aug = sqlContext.read.text("wasb://nomedocluster@j4wqp6rby7ga2.blob.core.windows.net/HdiNotebooks/PySpark/access_log_Aug95")

df_Aug95 = arq_aug.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('HOST'),
                          regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('TIMESTAMP'),
                          regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('PATH'),
                          regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('STATUS'),
                          regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('SIZE'))
```
#### 1. Número de hosts únicos.

```python
host_Jul = df_Jul95.select(countDistinct("HOST")
                           .alias("Número de hosts únicos(JULHO)")).show()
```

```python
host_Aug = df_Aug95.select(countDistinct("HOST")
                           .alias("Número de hosts únicos(AGOSTO)")).show()
```
#### 2. O total de erros 404.

```python
df_Jul95.select('STATUS')\
        .where('STATUS == 404').count()
```
```python
df_Aug95.select('STATUS')\
        .where('STATUS == 404').count()
```
#### 3. Os 5 URLs que mais causaram erro 404.

```python
# Seleciona coluna Host,com Status seja igual a 404.
# Coloca o número 1 em toda a linha que tiver "404", criando uma coluna chamada "count".
# Agrupa todo host igual.
# Faz uma soma da coluna por agrupamento.
# Ordena a coluna COUNT de forma decrescente 

df_Jul95.select(["HOST"])\
        .where('STATUS == 404')\
        .withColumn('COUNT',lit(1))\
        .groupBy('HOST')\
        .sum('COUNT')\
        .orderBy(desc('sum(COUNT)')).show(5)
```

```python
df_Aug95.select(["HOST"])\
        .where('STATUS == 404')\
        .withColumn('count',lit(1))\
        .groupBy('Host')\
        .sum('count')\
        .orderBy(desc('sum(count)')).show(5)
```
