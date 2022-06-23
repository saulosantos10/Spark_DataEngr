from pyspark.sql import SparkSession


if __name__ == "__main__":
	#Cria a sessão do spark
	spark = SparkSession.builder.appName("Streaming_02").getOrCreate()
	
	#Schema para arquivos JSON
	jsonschema = "nome STRING, postagem STRING, data INT"
	
	#Pasta monitorada aguardando a entrada de arquivos JSON
	df = spark.readStream.json("/home/caldas/testestream", schema=jsonschema)
	
	#Diretorio onde guarda o estado do Spark caso seja desligado
	diretorio = "/home/caldas/temp/"
	
	#Função para acessar banco de dados postgres
	def atualizapostgres(dataf, batchId):
		dataf.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/posts").option("dbtable", "posts").option("user", "postgres").option("password", "123456").option("driver", "org.postgresql.Driver").mode("append").save()
	
	#chama função e escreve o dataframe lido dentro do banco em modo micro-batch	
	Stcal = df.writeStream.foreachBatch(atualizapostgres).outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", diretorio).start()
	
	#Aguarda comando para terminar no console
	Stcal.awaitTermination()
