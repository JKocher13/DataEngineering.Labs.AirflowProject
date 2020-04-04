import datadotworld as dw
import pandas as pd


default_args = {
    'owner': 'James Kocher',
    'depends_on_past': False,
    'start_date': ,
    'retries': 0
	}

dag = DAG(
	data_world_music_pipeline,
	default_args=default_args,
	description = "This dag will get new top 10 every week from billboard, create necessary columns to run through spotify",
	schedule_interval = timedelta(days = 7)
	)

def grab_data():
	 past_music = dw.load_dataset('kcmillersean/billboard-hot-100-1958-2017')
	 features_pd = past_music.dataframes['audiio']
	 billboards_pd = past_music.dataframes['hot_stuff_2']
	 with open("/Users/jkocher/Documents/airflow_home/data/audio_features.json",wb) as f:
	 	f.write(features_pd)
	 with open("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.json",wb) as d:
	 	d.write(billboards_pd)

