# Real-time-Social-Media-Sentiment-Analysis


The U.S. presidential elections are events of immense global significance, shaping political, economic, and social landscapes not only within the United States but across the world. These elections spark widespread debates and discussions, particularly on social media platforms, which have become central to public discourse in recent years. These platforms act as real-time baro- meters of public opinion, capturing the sentiments, attitudes, and perceptions of voters. This vast trove of data offers invaluable insights into how people feel about the candidates, their campaigns, and the issues at the forefront of the election.
This project specifically focuses on real-time sentiment analysis of social media discussions centered around Donald Trump. As a highly visible and polarizing figure in recent U.S. presidential elections, Donald Trump generates a diverse range of opinions, making him a compelling subject for sentiment analysis. Understanding public sentiment toward Trump can reveal critical patterns about voter behavior, reactions to political events, and the impact of his statements or actions on the electorate. The goal is to capture and analyze ongoing conversations to :
1. Emotion Identification : The analysis aims to categorize and quantify public emotions as positive, negative, or neutral, offering a broad overview of how Trump is perceived in real-time.
2. Sentiment Evolution Tracking : By monitoring sentiment trends over time, the project seeks to highlight how public opinions shift in response to key political events, such as debates, policy announcements, or controversial statements.
3. Trend Visualization : The data will be presented using intuitive and clear visualizations, making it easier to interpret how sentiments fluctuate during the election cycle and offering actionable insights.
To do this project we will proceed as follows :
Data Generation & Streaming: Developed a Kafka producer to collect and stream tweets in JSON format to Confluent Cloud.
Data Processing & Sentiment Analysis: Implemented a Kafka , spark  consumer for filtering, enriching data, and performing sentiment analysis using the DistilBERT model.
Real-time Visualization: Built an interactive dashboard with Dash to display sentiment analysis results dynamically.
