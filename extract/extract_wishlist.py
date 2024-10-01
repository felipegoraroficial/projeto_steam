import requests  
import json      
import datetime  
import pandas as pd
import os

def extract_wishlist_steam():
    STEAM_ID = '76561198991169245' 

    url = f'https://store.steampowered.com/wishlist/profiles/{STEAM_ID}/wishlistdata/'

    response = requests.get(url)

    if response.status_code == 200:
        wishlist = response.json()
        
        wishlist_data = []

        for app_id, details in wishlist.items():
            store_url = f'https://store.steampowered.com/api/appdetails?appids={app_id}&cc=br&l=pt'
            store_response = requests.get(store_url)

            if store_response.status_code == 200:
                store_data = store_response.json()
                if store_data[str(app_id)]['success']:
                    game_data = store_data[str(app_id)]['data']
                    
                    price_info = game_data.get('price_overview', {})
                    price = price_info.get('final_formatted', 'N/A')
                    pc_requirements = game_data.get('pc_requirements', {})
                    minimum_requirements = pc_requirements.get('minimum', 'N/A')
                    recommended_requirements = pc_requirements.get('recommended', 'N/A')
                    description = game_data.get('short_description', 'N/A')
                    header_image = game_data.get('header_image', 'N/A')
                    steam_url = f"https://store.steampowered.com/app/{app_id}"
                    
                    wishlist_data.append({
                        'steam_appid': app_id,
                        'name': details.get('name', 'N/A'),
                        'price': price,
                        'release_date': details.get('release_date', 'N/A'),
                        'discount_pct': details.get('discount_pct', 0),
                        'minimum': minimum_requirements,
                        'recommended': recommended_requirements,
                        'short_description': description,
                        'header_image': header_image,
                        'link': steam_url
                    })
                else:
                    print(f"Erro ao obter detalhes do jogo {app_id}")
            else:
                print(f"Erro ao obter detalhes do jogo {app_id}")

        df_wishlist = pd.DataFrame(wishlist_data)

        df_wishlist['file_data'] = datetime.datetime.now().strftime("%Y-%m-%d")

        base_path = "/home/fececa/airflow/dags/steam/data/extract/wishlist"
        os.makedirs(base_path, exist_ok=True)
        file_path = os.path.join(base_path, f"{df_wishlist['file_data'].iloc[0]}.json")

        df_wishlist.to_json(file_path, orient='records', force_ascii=False, indent=4)

        print(f"Lista de desejos salva em {file_path}")
    else:
        print(f"Erro ao obter a lista de desejos: {response.status_code}")







