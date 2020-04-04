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

t1 = PythonOperator(
	task_id = "download_from_data_world",
	python_callable = grab_data,
	dag = dag)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 1',
    dag=dag,
)

t1 >> t2
