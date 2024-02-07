import requests
import json
from hdfs import InsecureClient  # Assurez-vous d'installer le module hdfs avec pip install hdfs
from datetime import datetime

# Fonction pour sauvegarder les données sur HDFS
def sauvegarder_sur_hdfs(data, hdfs_file_path):
    client = InsecureClient('http://localhost:9870', user='hadoop')
    
    with client.write(hdfs_file_path, encoding='utf-8') as hdfs_file:
        json.dump(data, hdfs_file, indent=2)

try:
    # Assurez-vous que 'current_date_as_str_time' contient la date au format 'YYYY-MM-DD'
    current_date = datetime.now()
    current_date_as_str_time = current_date.strftime('%Y-%m-%d')
    heure_en_temps_reel = current_date.strftime('%H:%M:%S')

    # Requête pour les prévisions
    reponse_forecast = requests.get('https://api.openweathermap.org/data/2.5/forecast?q=limoges,FR1&appid=eea045ed57d81cb0b2ad92319810b8c6')

    if reponse_forecast.status_code == 200:
        data_forecast = reponse_forecast.json()

        #stockage en local de api
        #json_file_path = '/home/ubuntu/mnmcount/scala/data/pays.json'

        #ecrire dans le json
        #with open(json_file_path, 'w', encoding='utf-8') as json_file:
            #json.dump(data_forecast, json_file, indent=2)

        hdfs_file_path_forecast = f'/user/Datalake2/raw/meteo/{current_date_as_str_time}.json'
        sauvegarder_sur_hdfs(data_forecast, hdfs_file_path_forecast)
        print(f"Les données ont été sauvegardées dans {hdfs_file_path_forecast}")
        #print(f"Les données ont été sauvegardées dans {json_file_path}")
    else:
        print(f"Échec de la requête avec le code : {reponse_forecast.status_code}")

except Exception as e:
    print(f"Une erreur s'est produite : {str(e)}")
