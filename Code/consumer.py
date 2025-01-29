import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as F
from textblob import TextBlob

# Configuration des packages nécessaires pour Kafka avec Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

def analyze_sentiment(tweet):
    """Analyse le sentiment d'un tweet avec TextBlob."""
    analysis = TextBlob(tweet)
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        return "Positive"
    elif polarity < 0:
        return "Negative"
    else:
        return "Neutral"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""
        Usage: consumer_nlp.py <bootstrap-servers> <topic>
        Example: python consumer_nlp.py localhost:9092 Twitter_Streaming
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    topic = sys.argv[2]

    print(f"Connexion au cluster Kafka via {bootstrapServers}...")
    print("En attente de données sur le topic", topic)

    # Réduire les logs Spark pour éviter les warnings inutiles
    spark = SparkSession.builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Définition du schéma pour les tweets
    tweet_schema = StructType([
        StructField("likes", IntegerType(), True),
        StructField("retweet_count", IntegerType(), True),
        StructField("cleaned_tweet", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    # Lecture du flux Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='YOUR USERNAME' password='YOUR PASSWORD';") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), tweet_schema).alias("data")) \
        .select("data.*")

    # Fonction pour traiter les tweets
    def process_batch(batch_df, batch_id):
        print("\n--- Nouvelle itération ---")
        batch_df = batch_df.withColumn("sentiment", F.udf(analyze_sentiment, StringType())(F.col("cleaned_tweet")))

        trump_mentions = batch_df.filter(F.col("cleaned_tweet").rlike(r"\bTrump\b"))
        biden_mentions = batch_df.filter(F.col("cleaned_tweet").rlike(r"\bBiden\b"))

        trump_sentiments = trump_mentions.groupBy("sentiment").count()
        biden_sentiments = biden_mentions.groupBy("sentiment").count()

        print("\n--- Sentiments pour Trump ---")
        trump_sentiments.show(truncate=False)

        print("\n--- Sentiments pour Biden ---")
        biden_sentiments.show(truncate=False)

    # Traitement du flux avec affichage toutes les 10 secondes
    query = kafka_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur. Fermeture du consumer...")
        query.stop()

    print("Consumer terminé.")
