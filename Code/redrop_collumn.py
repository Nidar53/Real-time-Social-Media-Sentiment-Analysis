import pandas as pd

# Charger le fichier CSV
file_path = "cleaned_datasetv2.csv"
output_file = "vraidataset.csv"  # Nouveau fichier de sortie

# Colonnes à supprimer
columns_to_drop = ["user_location", "city", "continent", "user_followers_count", "source","created_at"]

# Charger le dataset et supprimer les colonnes
data = pd.read_csv(file_path)
data = data.drop(columns=columns_to_drop, errors='ignore')

# Sauvegarder le dataset nettoyé
data.to_csv(output_file, index=False)

print(f"Les colonnes {columns_to_drop} ont été supprimées.")
print(f"Dataset nettoyé sauvegardé dans : {output_file}")
