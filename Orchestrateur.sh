#!/bin/bash

# Stockage de l'API en local
wget -P /home/ubuntu/mnmcount/scala/data "https://api.openweathermap.org/data/2.5/forecast?q=limoges,FR&appid=eea045ed57d81cb0b2ad92319810b8c6"

# Renommage de l'API
mv "/home/ubuntu/mnmcount/scala/data/forecast?q=limoges,FR&appid=eea045ed57d81cb0b2ad92319810b8c6" "/home/ubuntu/mnmcount/scala/data/meteo.json"

# Stockage sur HDFS
hdfs dfs -put -f /home/ubuntu/mnmcount/scala/data/meteo.json /user/datalake3/raw

#chemin du run.sh
cd /home/ubuntu/mnmcount/scala
sh run.sh


# Get the current date
#current_date=$(date +%Y-%m-%d)

# Construct the new filename with the current date
#new_filename="/home/ubuntu/mnmcount/scala/data/meteo_$current_date.json"

# Rename the file
#mv "/home/ubuntu/mnmcount/scala/data/meteo.json" "$new_filename"

# Upload the renamed file to HDFS
#hdfs dfs -put "$new_filename" "/user/Datalake2/raw2"
