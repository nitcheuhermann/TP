import sys
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

import pandas as pd
import pandas as pd
import numpy as np
import matplotlib.pyplot as pl
from math import sqrt
from numpy import array
from pyspark.sql.types import FloatType
import os
import matplotlib.colors as mcolors
from time import *
from operator import add


tabe = []
tabes=[]
tab_filiere=[]
tab_sexe = []
tab_classe = []
tab_total = []
tabe1 = []
tabe2 = []
tabe3 = []
tabe4 = []
tabe5=[]
tabe6 = []
tabe7 = []
tab_3ans=[]
tab_4ans=[]

#démarrage du driver
sc = SparkContext()
spark = SparkSession.builder.appName("teste").getOrCreate()


#Lecture du fichier stocké dans hdfs à partir de spark. Nous lisons ce fichier comme un .CSV
fichier = sc.textFile("hdfs://localhost:9000/TP/données_TP.csv")
finale1 = spark.read.csv(fichier,sep=";",encoding="utf-8")


finale1.createOrReplaceTempView("nouveau")
req = spark.sql("select distinct _c1 from nouveau")
tabe = req.rdd.map(tuple)
for t in tabe.take(10):
	tab_filiere.append(t[0])


		
rdd1 = spark.sparkContext.parallelize(tab_filiere)
rdd11 = rdd1.map(lambda x: (x,))
df111 = rdd11.toDF()
df1111 = df111.selectExpr("_1 as filieres")
df11111 = df1111.select("*").toPandas()


for i in range(0,len(tab_filiere)):
	f = tab_filiere[i]
	tabe00 = spark.sql("select count(_c0) from nouveau where _c1='{}' AND _c0='M'".format(f))
	tabe000 = tabe00.rdd.map(tuple)
	for t in tabe000.take(10):
		tabes.append(int(t[0]))

rdd2 = spark.sparkContext.parallelize(tabes)
rdd22 = rdd2.map(lambda x: (x,))
df222 = rdd22.toDF()
df2222 = df222.selectExpr("_1 as nombre_garcons")
df22222 = df2222.select("*").toPandas()

for i in range(0,len(tab_filiere)):
	f = tab_filiere[i]
	tabe00 = spark.sql("select count(_c0) from nouveau where _c1='{}' AND _c0='F'".format(f))
	tabe000 = tabe00.rdd.map(tuple)
	for t in tabe000.take(10):
		tabe1.append(int(t[0]))

rdd = spark.sparkContext.parallelize(tabe1)
rdd2 = rdd.map(lambda x: (x,))
df22 = rdd2.toDF()
df222 = df22.selectExpr("_1 as nombre_filles")
df2222 = df222.select("*").toPandas()

finale3 =pd.concat([df11111,df22222,df2222],axis=1)

pl.figure(figsize=(16,8))
dep=finale3[["filieres","nombre_garcons","nombre_filles"]].groupby("filieres",as_index=True).sum()
ax = dep.plot(kind="bar",figsize=(18,2))
ax.set_xlabel("filieres",fontsize=16)
ax.set_title("repartition des filieres par sexe",fontsize=10)
ax.legend().set_visible(True)


#tab = table(ax,finale3,loc='top')
#tab.auto_set_font_size(False)
#tab.set_fontsize(14)
pl.show()
req4 = spark.createDataFrame(finale3)
req4.show()
