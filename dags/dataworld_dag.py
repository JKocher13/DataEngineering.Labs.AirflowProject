import datadotworld as dw
import pandas as pd
import pickle
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime


default_args = {
    'owner': 'James Kocher',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0
	}

dag = DAG(
	"data_world_music_pipeline",
	default_args=default_args,
	description = "This dag will get new top 10 every week from billboard, create necessary columns to run through spotify",
	schedule_interval = timedelta(days = 7)
	)

def grab_data():
	 past_music = dw.load_dataset('kcmillersean/billboard-hot-100-1958-2017')
	 past_music.dataframes
	 features_pd = past_music.dataframes['audiio']
	 billboards_pd = past_music.dataframes['hot_stuff_2']
	 features_pd = past_music.dataframes['audiio']
	 billboards_pd = past_music.dataframes['hot_stuff_2']
	 with open("/Users/jkocher/Documents/airflow_home/data/audio_features.pickle", 'wb') as f:
	 	pickle.dump(features_pd, f)
	 with open("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle", 'wb') as d:
	 	pickle.dump(billboards_pd, d)

def split_data:
	billboard_df = pd.read_pickle("./data/billboard_rankings.pickle")
	billboard_df["year"] = billboard_df["weekid"].str[0:4]
	billboard_df["year"] = billboard_df["year"].astype(int)
	blillboard_df_1960s = billboard_df[billboard_df["year"] >= 1960]
	blillboard_df_1960s = blillboard_df_1960s[blillboard_df_1960s["year"]<=1969]
	blillboard_df_1960s= blillboard_df_1960s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1960s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1960s, d)
	blillboard_df_1970s = billboard_df[billboard_df["year"] >= 1970]
	blillboard_df_1970s = blillboard_df_1970s[blillboard_df_1970s["year"]<=1979]
	blillboard_df_1970s = blillboard_df_1970s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1970s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1970s, d)
	blillboard_df_1980s = billboard_df[billboard_df["year"] >= 1980]
	blillboard_df_1980s = blillboard_df_1980s[blillboard_df_1980s["year"]<=1989]
	blillboard_df_1980s = blillboard_df_1980s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1980s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1980s, d)
	blillboard_df_1990s = billboard_df[billboard_df["year"] >= 1990]
	blillboard_df_1990s = blillboard_df_1990s[blillboard_df_1990s["year"]<=1999]
	blillboard_df_1990s = blillboard_df_1990s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1990s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1990s, d)
	blillboard_df_2000s = billboard_df[billboard_df["year"] >= 2000]
	blillboard_df_2000s = blillboard_df_2000s[blillboard_df_2000s["year"]<=2009]
	blillboard_df_2000s = blillboard_df_2000s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_2000s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_2000s, d)
	blillboard_df_2010s = billboard_df[billboard_df["year"] >= 2010]
	blillboard_df_2010s = blillboard_df_2010s[blillboard_df_2010s["year"]<=2019]
	blillboard_df_2010s = blillboard_df_2010s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_2010s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_2010s, d)

t1 = PythonOperator(
	task_id = "download_from_data_world",
	python_callable = grab_data,
	dag = dag)

t2 = PythonOperator(
	task_id = "split_by_decade",
	python_callable = split_data,
	dag = dag)


t1 >> t2
