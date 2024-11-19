import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # Criar SparkContext
    conf = SparkConf().setAppName("Conta Palavras").setMaster("spark://192.168.0.19:7077")

    sc = SparkContext(conf=conf)


    # Carregar o Arquivo de Texto (está no diretório local, caso estivesse no HDFS deve ser informado)
    palavras = sc.textFile("/home/eduardo/Compartilhada/input.txt")

    # Quebrar cada linha do texto em palavras individuais, utilizando o espaço como delimitador
    palavras = palavras.flatMap(lambda line: line.split(" "))

    # Mapear cada palavra para uma contagem de 1
    contagem = palavras.map(lambda palavra: (palavra, 1))

    # Reduzir por chave (palavra) para somar as contagens
    resultado = contagem.reduceByKey(lambda a, b: a + b)

    # Salvar o resultado em um novo arquivo 
    resultado.saveAsTextFile("/home/eduardo/Compartilhada/saida10")
