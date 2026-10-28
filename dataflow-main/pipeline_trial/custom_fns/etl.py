
import pandas as pd
import ast
import numpy as np

def clean_attributes(business):
    print('Cleaning attributes')

    atributtes = pd.json_normalize(data = business['attributes'])
    
    try: 
        atributtes['BusinessParking'].fillna("{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}", inplace=True)
        atributtes.loc[atributtes['BusinessParking'] == 'None', 'BusinessParking'] = "{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}"
    except:
        atributtes['BusinessParking'] = "{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}"

    try:
        atributtes.loc[atributtes['Ambience'] == 'None', 'Ambience'] = "{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False, 'divey': False}"
        atributtes['Ambience'] = atributtes['Ambience'].fillna("{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False, 'divey': False}")
    except:
        atributtes['Ambience'] = "{'romantic': False, 'intimate': False, 'classy': False, 'hipster': False, 'touristy': False, 'trendy': False, 'upscale': False, 'casual': False, 'divey': False}"

    
    garage = pd.json_normalize(atributtes.BusinessParking.apply(lambda x: ast.literal_eval(x)))
    ambience = pd.json_normalize(atributtes['Ambience'].apply(lambda x: ast.literal_eval(x)))

    atributtes['garage'] = garage['garage']
    atributtes['garage'].fillna(0, inplace=True)

    atributtes.drop(['BusinessParking', 'Ambience'], axis=1, inplace=True)

    ambience['good_ambience'] = ambience['romantic'] + ambience['intimate'] + ambience['classy'] + ambience['hipster'] + ambience['touristy'] + ambience['trendy'] + ambience['upscale'] + ambience['casual'] + ambience['divey']

    ambience.loc[ambience['good_ambience'] > 1, 'good_ambience'] = 1
    ambience.fillna(0, inplace=True)
    atributtes['good_ambience'] = ambience['good_ambience']

    try:
        atributtes['RestaurantsTakeOut'].fillna(0, inplace=True)
        atributtes['RestaurantsDelivery'].fillna(0, inplace=True)
        atributtes['RestaurantsTableService'].fillna(0, inplace=True)
    except:
        atributtes['RestaurantsTakeOut'] = 'None'
        atributtes['RestaurantsDelivery'] = 'None'
        atributtes['RestaurantsTableService'] = 'None'
        
    atributtes['RestaurantsTakeOut'] = atributtes['RestaurantsTakeOut'].map({'True': 1, 'False': 0, 'None': 0})
    atributtes['RestaurantsDelivery'] = atributtes['RestaurantsDelivery'].map({'True': 1, 'False': 0, 'None': 0})
    atributtes['RestaurantsTableService'] = atributtes['RestaurantsTableService'].map({'True': 1, 'False': 0, 'None': 0})
    atributtes['delivery'] = atributtes['RestaurantsTakeOut'] + atributtes['RestaurantsDelivery']
    atributtes['delivery'] = pd.to_numeric(atributtes['delivery'], errors='coerce').fillna(0).astype(int)
    atributtes['delivery'] = atributtes['delivery'].replace(to_replace=2, value=1)


    atributtes.drop(['RestaurantsTakeOut', 'RestaurantsDelivery'], axis=1, inplace=True)

    top20 = atributtes.notna().sum().sort_values(ascending=False).head(20).index.tolist()

    atributtes = atributtes[top20]

    atributtes.fillna(0, inplace=True)
    atributtes.replace('None', 0, inplace=True)
    atributtes.replace('False', 0, inplace=True)
    atributtes.replace(False, 0, inplace=True)
    atributtes.replace('True', 1, inplace=True)

    try:
        atributtes['WiFi'] = np.where(atributtes['WiFi'] != 0, 1, 0)
    except:
        atributtes['WiFi'] = 0

    try:
        atributtes.loc[atributtes['Alcohol'] == "u'none'", 'Alcohol'] = 0
        atributtes.loc[atributtes['Alcohol'] == "'none'", 'Alcohol'] = 0
        atributtes.loc[atributtes['Alcohol'] != 0, 'Alcohol'] = 1
        atributtes['Alcohol'].unique()
    except:
        atributtes['Alcohol'] = 0

    try:
        atributtes.loc[atributtes['NoiseLevel'].str.contains('quiet').fillna(False), 'NoiseLevel'] = 1
        atributtes.loc[atributtes['NoiseLevel'].str.contains('average').fillna(False), 'NoiseLevel'] = 2
        atributtes.loc[atributtes['NoiseLevel'].str.contains('loud').fillna(False), 'NoiseLevel'] = 3
    except:
        atributtes['NoiseLevel'] = 0
    try:
        atributtes.loc[atributtes['RestaurantsAttire'] != 0,'RestaurantsAttire'] = 1
    except:
        atributtes['RestaurantsAttire'] = 0
    atributtes.loc[atributtes['garage'] != 0,'garage'] = 1
    

    try:
        atributtes.loc[atributtes['GoodForMeal'] == 0,'GoodForMeal'] = "{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}"
        atributtes['GoodForMeal'].fillna("{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}", inplace=True)
    except:
        atributtes['GoodForMeal'] = "{'dessert': False, 'latenight': False, 'lunch': False, 'dinner': False, 'brunch': False, 'breakfast': False}"
    
    meal_types = pd.json_normalize(atributtes['GoodForMeal'].apply(lambda x: ast.literal_eval(x)))
    atributtes.drop(['GoodForMeal'], axis=1, inplace=True)
    atributtes['meal_diversity'] = meal_types['dessert'] + meal_types['latenight'] + meal_types['lunch'] + meal_types['dinner'] + meal_types['brunch'] + meal_types['breakfast']
    atributtes['meal_diversity'].fillna(0, inplace=True)
    atributtes['meal_diversity'] = atributtes['meal_diversity'].astype(int)

    for i in atributtes.dtypes[atributtes.dtypes == object].index.tolist():
        atributtes[i] = atributtes[i].astype(str)
        atributtes[i] = atributtes[i].str.replace('[^0-9]', '', regex=True)
        atributtes[i] = atributtes[i].astype(int)

    atributtes['business_id'] = pd.to_numeric(business['business_id'], errors='coerce').fillna(0)
    
    atributtes.columns = [i.lower() for i in atributtes.columns.tolist()]

    ## Generate schema
    # schema = atributtes.dtypes.to_dict()
    # schema_str = []
    # for field, dtype in schema.items():
    #     if dtype == 'INTEGER':
    #         bq_type = 'INTEGER'
    #     elif dtype == 'float64':
    #         bq_type = 'FLOAT64'
    #     elif dtype == 'bool':
    #         bq_type = 'BOOLEAN'
    #     else:
    #         bq_type = 'STRING'

    #     schema_str.append(f"{field}:{bq_type}")

    # schema_str = ', '.join(schema_str)

    return atributtes.to_dict(orient='records') #schema_str

