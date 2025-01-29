from confluent_kafka import Producer
import pandas as pd
import json
import time
from datetime import datetime
import signal
import sys


def read_config():
    """Reads Kafka configuration from client.properties."""
    config = {}
    try:
        with open("client.properties", "r") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.split("=", 1)
                    config[parameter.strip()] = value.strip()
    except FileNotFoundError:
        print("Error: client.properties file not found.")
        exit(1)
    except Exception as e:
        print(f"Error while reading client.properties: {e}")
        exit(1)
    return config


def produce_tweets(producer, topic, dataset_path):
    """Produce tweets to the Kafka topic."""
    try:
        # Charger le dataset
        data = pd.read_csv(dataset_path)

        for index, row in data.iterrows():
            tweet_data = {
                "likes": int(row["likes"]),
                "retweet_count": int(row["retweet_count"]),
                "cleaned_tweet": row["cleaned_tweet"],
                "timestamp": datetime.now().isoformat()  # Ajout du temps de production
            }

            # Envoyer le message au topic Kafka
            producer.produce(topic, key=str(index), value=json.dumps(tweet_data))
            producer.flush()
            print(f"Produced: {tweet_data}")

            # Simuler une latence entre les envois pour la simulation en temps réel
            time.sleep(0.1)

    except Exception as e:
        print(f"Error producing tweets: {e}")


def handle_exit(signum, frame):
    """Handles termination signals to stop the program cleanly."""
    print("\nReceived exit signal. Stopping production...")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Lire la configuration Kafka
    config = read_config()
    producer = Producer(config)

    # Nom du topic Kafka
    topic = "tweet_test"

    # Chemin du dataset
    dataset_path = "prepared_dataset.csv"

    print("Starting Kafka Producer...")

    try:
        # Lancer la production des tweets
        produce_tweets(producer, topic, dataset_path)

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Stopping production...")
    finally:
        producer.flush()
        print("Producer stopped cleanly.")


if __name__ == "__main__":
    main()
