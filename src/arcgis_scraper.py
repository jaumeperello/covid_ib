import os
import requests
from datetime import datetime
import json
import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

llista = {
    "mapa_illes": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/1/query?where=0%3D0&outFields=%2A&f=json",
    "mapa_municipis": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/0/query?where=0%3D0&outFields=%2A&f=json",
    "loc_tractament": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/7/query?where=0%3D0&outFields=%2A&f=json",
    "demografic_defuncions": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/6/query?where=0%3D0&outFields=%2A&f=json",
    "defuncions": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/5/query?where=0%3D0&outFields=%2A&f=json",
    "casos": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/4/query?where=0%3D0&outFields=%2A&f=json",
    "demografic_casos": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/3/query?where=0%3D0&outFields=%2A&f=json",
    "municipis": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/2/query?where=0%3D0&outFields=%2A&f=json",
    "pcr": "https://services8.arcgis.com/bMjf2m00GcTknwWp/arcgis/rest/services/Casos_municipis_illes_covid19/FeatureServer/8/query?where=0%3D0&outFields=%2A&f=json"}


def download(url, dest_folder, filename):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    if r.ok:
        logging.info("saving to" + str(os.path.abspath(file_path)))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
        return True
    else:  # HTTP status code 4XX/5XX
        logging.info("Download failed: status code {}\n{}".format(r.status_code, r.text))
        return False


def generate_municipis(last_date, data_directory="../arcgis_dades/", output_directory="../arcgis_cvs/", final_directory="../download/arcgis/", save=True):
    # previous data
    total_df = pd.read_csv(output_directory + 'municipis_total.csv', dtype='str')

    # new data
    filename = data_directory + "mapa_municipis.json"
    with open(filename) as json_file:
        d = json.load(json_file)
    stoday = last_date.strftime('%Y-%m-%d')
    rows = []
    for row in d["features"]:
        row['attributes']['date'] = stoday
        rows.append(row['attributes'])
    df = pd.DataFrame(rows, dtype='str')
    df.rename(columns={'MUNICIPI': 'region', 'ILLA': 'illa', 'INE_MUN': 'region_code', 'TOTAL': 'cases', 'altes': 'recovered', 'pendent': 'active_cases', 'decessos': 'deceased'}, inplace=True)
    df.drop(columns=['OBJECTID', "POB_2019", "cas_per_10000hab", "Shape__Area", "Shape__Length"], inplace=True)

    # Join all data
    total_df = pd.concat([total_df, df])
    total_df['date'] = pd.to_datetime(total_df['date'])
    total_df.set_index(['date', 'region_code'], inplace=True)

    if save:
        today = datetime.now().strftime('%Y%m%d')
        df['date'] = pd.to_datetime(df['date'])
        df.set_index(['date', 'region_code'], inplace=True)
        df.to_csv(output_directory + 'municipis_' + today.strftime('%Y%m%d') + '.csv')
        total_df.to_csv(output_directory + 'municipis_total_' + today.strftime('%Y%m%d') + '.csv')
        total_df.to_csv(output_directory + 'municipis_total.csv')

    total_df = total_df[~total_df.index.duplicated(keep='last')]
    for illa in total_df['illa'].unique():
        illa_df = total_df[total_df.illa == illa]
        illa_df.drop(columns=['illa'], inplace=True)
        illa_df = illa_df.astype({'cases': 'int64', 'recovered': 'int64', 'active_cases': 'int64', 'deceased': 'int64'})
        dates = illa_df.index.get_level_values('date').unique()
        for date in dates:
            date_df = illa_df.loc[date]
            total = date_df.sum(min_count=1)
            total['region'] = f'total-{illa.lower()}'
            illa_df.loc[(date, 0), illa_df.columns] = total.values
        nou_df = pd.DataFrame()
        for region in illa_df['region'].unique():
            reg_df = illa_df[illa_df.region == region]
            reg_df.reset_index(inplace=True)
            reg_df['date'] = pd.to_datetime(reg_df['date'])
            reg_df.set_index(['date'], inplace=True)
            reg_df.sort_index(inplace=True)
            idx = pd.date_range(reg_df.index.min(), reg_df.index.max())
            reg_df.index = pd.DatetimeIndex(reg_df.index)
            reg_df = reg_df.reindex(idx, method='ffill')
            reg_df.reset_index(inplace=True)
            nou_df = pd.concat([nou_df, reg_df])
        illa_df = nou_df
        illa_df.rename(columns={'index': 'date'}, inplace=True)
        illa_df.sort_index(inplace=True)
        illa_df.to_csv(final_directory + f'{illa.lower()}_total.csv')

    return total_df


def generate_illes(last_date, data_directory="../arcgis_dades/", output_directory="../arcgis_cvs/", final_directory="../download/arcgis/", save=True):
    # previous data
    total_df = pd.read_csv(output_directory + 'illes_total.csv', dtype='str')

    # new data
    filename = data_directory + "mapa_illes.json"
    with open(filename) as json_file:
        d = json.load(json_file)
    stoday = last_date.strftime('%Y-%m-%d')
    rows = []
    for row in d["features"]:
        row['attributes']['date'] = stoday
        rows.append(row['attributes'])
    df = pd.DataFrame(rows, dtype='str')
    df.rename(columns={'ILLA': 'region', 'OBJECTID': 'region_code', 'SUM_TOTAL': 'cases', 'SUM_altes': 'recovered', 'SUM_pendent': 'active_cases', 'SUM_decessos': 'deceased'}, inplace=True)
    df.drop(columns=["SUM_POB_2019", "cas_per_10000hab", "Shape__Area", "Shape__Length"], inplace=True)

    # Join all data
    total_df = pd.concat([total_df, df])
    total_df['date'] = pd.to_datetime(total_df['date'])
    total_df.set_index(['date', 'region_code'], inplace=True)
    if save:
        today = datetime.now().strftime('%Y%m%d')
        df['date'] = pd.to_datetime(df['date'])
        df.set_index(['date', 'region_code'], inplace=True)
        df.to_csv(output_directory + 'illes_' + today.strftime('%Y%m%d') + '.csv')
        total_df.to_csv(output_directory + 'illes_total_' + today.strftime('%Y%m%d') + '.csv')
        total_df.to_csv(output_directory + 'illes_total.csv')

    dates = total_df.index.get_level_values('date').unique()
    total_df = total_df.astype({'cases': 'int64', 'recovered': 'int64', 'active_cases': 'int64', 'deceased': 'int64'})
    total_df = total_df[~total_df.index.duplicated(keep='last')]
    for date in dates:
        date_df = total_df.loc[date]
        total = date_df.sum(min_count=1)
        total['region'] = 'total-balears'
        total_df.loc[(date, 0), total_df.columns] = total.values

    nou_df = pd.DataFrame()
    for region in total_df['region'].unique():
        reg_df = total_df[total_df.region == region]
        reg_df.reset_index(inplace=True)
        reg_df['date'] = pd.to_datetime(reg_df['date'])
        reg_df.set_index(['date'], inplace=True)
        reg_df.sort_index(inplace=True)
        idx = pd.date_range(reg_df.index.min(), reg_df.index.max())
        reg_df.index = pd.DatetimeIndex(reg_df.index)
        reg_df = reg_df.reindex(idx, method='ffill')
        reg_df.reset_index(inplace=True)
        nou_df = pd.concat([nou_df, reg_df])
    total_df = nou_df
    total_df.rename(columns={'index': 'date'}, inplace=True)
    total_df.sort_index(inplace=True)
    total_df.to_csv(final_directory + 'balears_total.csv')

    return total_df


def get_update_date(url_casos, temp_directory="../arcgis_dades/temp/"):
    today = datetime.now().strftime('%Y%m%d')
    if download(url_casos, dest_folder=temp_directory, filename=f'{today}.json'):
        filename = f'{temp_directory}{today}.json'
        with open(filename) as json_file:
            d = json.load(json_file)
        rows = []
        for row in d["features"]:
            rows.append(row['attributes'])
        df = pd.DataFrame(rows)

        df['FIS'] = pd.to_datetime(df['FIS'], unit='ms')
        df.set_index(['FIS'], inplace=True)
        df.sort_index(inplace=True)
        last_date = df.index.get_level_values('FIS')[-1]
        os.remove(filename)
        return last_date
    return None


def get_local_date(output_directory="../arcgis_cvs/"):
    total_df = pd.read_csv(output_directory + 'municipis_total.csv', dtype='str')
    total_df['date'] = pd.to_datetime(total_df['date'])
    total_df.set_index(['date'], inplace=True)
    total_df.sort_index(inplace=True)
    total_date = total_df.index.get_level_values('date')[-1]
    return total_date


def arcgis_has_changes(final_directory="../download/arcgis/", data_path="../arcgis_dades/", output_directory="../arcgis_cvs/"):
    today = datetime.now()
    data_directory = f"{data_path}/{today.strftime('%Y%m%d')}/"
    temp_directory = f"{data_path}/temp/"

    update_date = get_update_date(llista['casos'], temp_directory)
    local_date = get_local_date(output_directory)

    # if there is new data download rest of files and generate csv
    if update_date and local_date < update_date:
        for nom in llista:
            download(llista[nom], dest_folder=data_directory, filename=nom + ".json")
        generate_municipis(update_date, data_directory, output_directory, final_directory)
        generate_illes(update_date, data_directory, output_directory, final_directory)
        logging.info("ArcGis: Data updated")
        return True

    logging.info("ArcGis: No new data")
    return False


if __name__ == "__main__":
    arcgis_has_changes("../download/arcgis/", )
