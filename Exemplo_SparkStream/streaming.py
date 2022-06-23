from pyspark.sql import SparkSession


if __name__ == "__main__":
	#Cria a sessão do spark
	spark = SparkSession.builder.appName("Streaming").getOrCreate()
	
	#Schema para arquivos JSON
	jsonschema = "nome STRING, postagem STRING, data INT"
	
	#Pasta monitorada aguardando a entrada de arquivos JSON
	df = spark.readStream.json("/home/caldas/testestream", schema=jsonschema)
	
	#Diretorio onde guarda o estado do Spark caso seja desligado
	diretorio = "/home/caldas/temp/"
	
	#Escreve o arquivo lido na pasta no console
	stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", diretorio).start()
	
	
	#Aguarda ação para finalizar o processo
	stcal.awaitTermination()
