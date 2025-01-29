import dash
from dash import dcc, html, Input, Output
import pandas as pd
import plotly.express as px
import json
import os

# Charger les données depuis le fichier JSON
def load_data():
    if os.path.exists("consumer_output.json"):
        with open("consumer_output.json", "r") as file:
            return json.load(file)
    return []

# Initialiser l'application Dash
app = dash.Dash(__name__)

# Layout de l'application
app.layout = html.Div([
    html.H1("Visualisation des Sentiments"),
    dcc.Interval(id="update-interval", interval=3000, n_intervals=0),
    dcc.Graph(id="sentiment-pie-chart"),
    dcc.Graph(id="sentiment-timeline"),
    html.H2("Détails des Messages"),
    html.Div(id="data-table")
])

# Callback pour mettre à jour les graphiques et le tableau
@app.callback(
    [
        Output("sentiment-pie-chart", "figure"),
        Output("sentiment-timeline", "figure"),
        Output("data-table", "children")
    ],
    [Input("update-interval", "n_intervals")]
)
def update_dashboard(_):
    data = load_data()
    if not data:
        return {}, {}, "Aucune donnée disponible."

    df = pd.DataFrame(data)

    # Camembert des sentiments
    pie_chart = px.pie(
        names=df["sentiment"].value_counts().index,
        values=df["sentiment"].value_counts().values,
        title="Répartition des Sentiments"
    )

    # Graphique des scores
    timeline_chart = px.line(
        df,
        x="timestamp",
        y=["positive_score", "negative_score", "neutral_score"],
        title="Scores des Sentiments au Fil du Temps"
    )

    # Tableau des données
    table = html.Table([
        html.Thead(html.Tr([html.Th(col) for col in df.columns])),
        html.Tbody([
            html.Tr([html.Td(df.iloc[i][col]) for col in df.columns])
            for i in range(len(df))
        ])
    ])

    return pie_chart, timeline_chart, table

# Lancer l'application
if __name__ == "__main__":
    app.run_server(debug=True)
