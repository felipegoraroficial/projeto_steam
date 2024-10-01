import requests  
import json      
import datetime  
import pandas as pd
import os

def extract_user_steam():

    API_KEY = '1A1F4922939AFD7DFB99B30C31BCE24F'   
    STEAM_ID = '76561198991169245' 

    url = f'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={API_KEY}&steamid={STEAM_ID}&format=json'
    response = requests.get(url)
    data = response.json()
    games = data['response']['games']

    game_user_list = []

    for data in games:
        appid = data['appid']
        app_details_response = requests.get(f'https://store.steampowered.com/api/appdetails?appids={appid}')
        app_details_data = app_details_response.json() 
        game_user_list.append(app_details_data) 

    game_data_user = []

    for response in game_user_list:
        for app_id, data in response.items():
            if 'data' in data:
                game_data = {
                    'steam_appid': data['data'].get('steam_appid'),
                    'type': data['data'].get('type'),
                    'name': data['data'].get('name'),
                    'required_age': data['data'].get('required_age'),
                    'is_free': data['data'].get('is_free'),
                    'header_image': data['data'].get('header_image'),
                    'website': data['data'].get('website'),
                    'pc_requirements': data['data'].get('pc_requirements'),
                    'package_groups': data['data'].get('package_groups'),
                    'short_description': data['data'].get('short_description')
                }
                game_data_user.append(game_data)

    games_df = pd.DataFrame(games)
    final_data_user_df = pd.DataFrame(game_data_user)
    merged_data = pd.merge(games_df, final_data_user_df, left_on='appid', right_on='steam_appid', how='inner')
    merged_data.drop('steam_appid', axis=1, inplace=True)
    final_user_data = merged_data.to_dict('records')


    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    for json_obj in final_user_data:
        json_obj['file_data'] = current_date


    base_path = "/home/fececa/airflow/dags/steam/data/extract/user"
    os.makedirs(base_path, exist_ok=True)
    file_path = os.path.join(base_path, f"{current_date}.json")

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(final_user_data, f, ensure_ascii=False, indent=4)

    print(f"Lista salva em {file_path}")
