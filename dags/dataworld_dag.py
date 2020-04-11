import datadotworld as dw
import pandas as pd
import pickle
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import sqlalchemy
import pymysql
import papermill as pm
import airflow.hooks.S3_hook

bucket_name = "spotify-billboard-airflow-project"
reports_storage_path = "pass"
data_storage_path = "/Users/jkocher/Documents/airflow_home/data/"
jupyter_notebook_storage = "pass"
billboard_location = "data/raw_data/billboard_pickle"
audio_features_location = "data/raw_data/audio_feature_pickle"
hook = airflow.hooks.S3_hook.S3Hook('my_conn_S3')



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
	features_df = past_music.dataframes['audiio']
	billboard_df = past_music.dataframes['hot_stuff_2']
	features_df = past_music.dataframes['audiio']
	billboard_df = past_music.dataframes['hot_stuff_2']
	billboard_df["year"] = billboard_df["weekid"].str[0:4]
	billboard_df["year"] = billboard_df["year"].astype(int) 
	feautre_obj = pickle.dumps(features_df)
	billboard_obj = pickle.dumps(billboard_df)
	hook.load_bytes(feautre_obj, audio_features_location, bucket_name,replace = True)
	hook.load_bytes(billboard_obj, billboard_location, bucket_name,replace = True)

def get_spotify():
	spotify_df = hook.get_key(audio_features_location, bucket_name)
	spotify_df = pickle.loads(spotify_df.get()['Body'].read())
	return spotify_df

def get_billboard():
	billboard_df = hook.get_key(billboard_location, bucket_name)
	billboard_df = pickle.loads(billboard_df.get()['Body'].read())
	return billboard_df

def split_data_1960s():
	billboard_df = get_billboard()
	billboard_df_1960s = billboard_df[billboard_df["year"] >= 1960]
	billboard_df_1960s = billboard_df_1960s[billboard_df_1960s["year"]<=1969]
	billboard_df_1960s= billboard_df_1960s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	billboard_1960s = pickle.dumps(billboard_df_1960s)
	hook.load_bytes(billboard_1960s, "data/raw_data/billboard_1960s", bucket_name, replace = True)

def split_data_1970s():
	billboard_df = get_billboard()
	billboard_df_1970s = billboard_df[billboard_df["year"] >= 1970]
	billboard_df_1970s = billboard_df_1970s[billboard_df_1970s["year"]<=1979]
	billboard_df_1970s = billboard_df_1970s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	billboard_1970s = pickle.dumps(billboard_df_1970s)
	hook.load_bytes(billboard_1970s, "data/raw_data/billboard_1970s", bucket_name, replace = True)

def split_data_1980s():
	billboard_df = get_billboard()
	billboard_df_1980s = billboard_df[billboard_df["year"] >= 1980]
	billboard_df_1980s = billboard_df_1980s[billboard_df_1980s["year"]<=1989]
	billboard_df_1980s = billboard_df_1980s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	billboard_1980s = pickle.dumps(billboard_df_1980s)
	hook.load_bytes(billboard_1980s, "data/raw_data/billboard_1980s", bucket_name, replace = True)

def split_data_1990s():
	billboard_df = get_billboard()
	billboard_df_1990s = billboard_df[billboard_df["year"] >= 1990]
	billboard_df_1990s = billboard_df_1990s[billboard_df_1990s["year"]<=1999]
	billboard_df_1990s = billboard_df_1990s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	billboard_1990s = pickle.dumps(billboard_df_1990s)
	hook.load_bytes(billboard_1990s, "data/raw_data/billboard_1990s", bucket_name, replace = True)

def split_data_2000s():
	billboard_df = get_billboard()
	blillboard_df_2000s = billboard_df[billboard_df["year"] >= 2000]
	blillboard_df_2000s = blillboard_df_2000s[blillboard_df_2000s["year"]<=2009]
	blillboard_df_2000s = blillboard_df_2000s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	billboard_1990s = pickle.dumps(billboard_df_1990s)
	hook.load_bytes(billboard_1990s, "data/raw_data/billboard_2000s", bucket_name, replace = True)

def split_data_2010s():
	billboard_df = get_billboard()
	blillboard_df_2010s = billboard_df[billboard_df["year"] >= 2010]
	blillboard_df_2010s = blillboard_df_2010s[blillboard_df_2010s["year"]<=2019]
	blillboard_df_2010s = blillboard_df_2010s.groupby(["song","performer","songid"]).agg({"week_position":"min", "year":"min"})
	billboard_1990s = pickle.dumps(billboard_df_1990s)
	hook.load_bytes(billboard_1990s, "data/raw_data/billboard_2010s", bucket_name, replace = True)

def merge_1960s():
	spotify_df = get_spotify()
	billboard_1960s = hook.get_key("data/raw_data/billboard_1960s", bucket_name)
	billboard_df_1960s = pickle.loads(billboard_1960s.get()['Body'].read())
	top1960_df = pd.merge(blillboard_df_1960s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1960_df= top1960_df.dropna(subset=["key"])
	hook.load_bytes(merged_data_1960s, "data/merged_data/merged_data_1960s", bucket_name, replace = True)

def merge_1970s():
	spotify_df = get_spotify()
	billboard_1970s = hook.get_key("data/raw_data/billboard_1960s", bucket_name)
	billboard_df_1970s = pickle.loads(billboard_1970s.get()['Body'].read())
	top1970_df = pd.merge(blillboard_df_1970s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1970_df= top1970_df.dropna(subset=["key"])
	hook.load_bytes(merged_data_1960s, "data/merged_data/merged_data_1970s", bucket_name, replace = True)

def merge_1980s():
	spotify_df = get_spotify()
	billboard_1980s = hook.get_key("data/raw_data/billboard_1980s", bucket_name)
	billboard_df_1980s = pickle.loads(billboard_1980s.get()['Body'].read())
	top1980_df = pd.merge(blillboard_df_1980s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1980_df= top1980_df.dropna(subset=["key"])
	hook.load_bytes(merged_data_1980s, "data/merged_data/merged_data_1980s", bucket_name, replace = True)

def merge_1990s():
	spotify_df = get_spotify()
	billboard_1990s = hook.get_key("data/raw_data/billboard_1990s", bucket_name)
	billboard_df_1990s = pickle.loads(billboard_1990s.get()['Body'].read())
	top1990_df = pd.merge(blillboard_df_1990s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1990_df= top1990_df.dropna(subset=["key"])
	hook.load_bytes(merged_data_1990s, "data/merged_data/merged_data_1990s", bucket_name, replace = True)


def merge_2000s():
	spotify_df = get_spotify()
	billboard_2000s = hook.get_key("data/raw_data/billboard_2000s", bucket_name)
	billboard_df_2000s = pickle.loads(billboard_2000s.get()['Body'].read())
	spotify_df = pd.read_pickle(str(data_storage_path) + "audio_features.pickle")
	top2000_df = pd.merge(blillboard_df_2000s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top2000_df= top2000_df.dropna(subset=["key"])
	hook.load_bytes(merged_data_2000s, "data/merged_data/merged_data_2000s", bucket_name, replace = True)

def merge_2010s():
	spotify_df = get_spotify()
	billboard_2010s = hook.get_key("data/raw_data/billboard_2010s", bucket_name)
	billboard_df_2010s = pickle.loads(billboard_2010s.get()['Body'].read())
	top2010_df = pd.merge(blillboard_df_2010s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top2010_df= top2010_df.dropna(subset=["key"])
	hook.load_bytes(merged_data_2010s, "data/merged_data/merged_data_2010s", bucket_name, replace = True)

def cleaned_data_to_MySql():
	top1960_df = pd.read_pickle(str(data_storage_path) + "billboard_spotify_1960s.pickle")
	top1970_df = pd.read_pickle(str(data_storage_path) + "billboard_spotify_1970s.pickle")
	top1980_df = pd.read_pickle(str(data_storage_path) + "billboard_spotify_1980s.pickle")
	top1990_df = pd.read_pickle(str(data_storage_path) + "billboard_spotify_1990s.pickle")
	top2000_df = pd.read_pickle(str(data_storage_path) + "billboard_spotify_2000s.pickle")
	top2010_df = pd.read_pickle(str(data_storage_path) + "billboard_spotify_2010s.pickle")
	engine = sqlalchemy.create_engine('postgresql+psycopg2://jkocher:@localhost/music_db')
	with engine.connect() as conn, conn.begin():
		top1960_df.to_sql('analysis_1960s', conn, if_exists='replace')
		top1970_df.to_sql('analysis_1970s', conn, if_exists='replace')
		top1980_df.to_sql('analysis_1980s', conn, if_exists='replace')
		top1990_df.to_sql('analysis_1990s', conn, if_exists='replace')
		top2000_df.to_sql('analysis_2000s', conn, if_exists='replace')
		top2010_df.to_sql('analysis_2010s', conn, if_exists='replace')

def downloaded_data_to_mysql():
	engine = sqlalchemy.create_engine('postgresql+psycopg2://jkocher:@localhost/music_db')
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/audio_features.pickle")
	with engine.connect() as conn, conn.begin():
		billboard_df.to_sql('billboard_rankings', conn, if_exists='replace')
		spotify_df.to_sql('music_features', conn, if_exists='replace')

def Papermill_1960s():
	pm.execute_notebook(
		data_storage_path + 'Single_year.ipynb',
		data_storage_path + 'Single_year_output.ipynb',
		parameters=dict(file_path=data_storage_path + "billboard_spotify_1960s.pickle", year_name="1960s")
		)

def Papermill_1970s():
	pm.execute_notebook(
		data_storage_path + 'Single_year.ipynb',
		data_storage_path + 'Single_year_output.ipynb',
		parameters=dict(file_path=data_storage_path + "billboard_spotify_1970s.pickle", year_name="1970s")
		)

def Papermill_1980s():
	pm.execute_notebook(
		data_storage_path + 'Single_year.ipynb',
		data_storage_path + 'Single_year_output.ipynb',
		parameters=dict(file_path=data_storage_path + "billboard_spotify_1980s.pickle", year_name="1980s")
		)

def Papermill_1990s():
	pm.execute_notebook(
		data_storage_path + 'Single_year.ipynb',
		data_storage_path + 'Single_year_output.ipynb',
		parameters=dict(file_path=data_storage_path + "billboard_spotify_1990s.pickle", year_name="1990s")
		)

def Papermill_2000s():
	pm.execute_notebook(
		data_storage_path + 'Single_year.ipynb',
		data_storage_path + 'Single_year_output.ipynb',
		parameters=dict(file_path=data_storage_path + "billboard_spotify_2000s.pickle", year_name="2000s")
		)

def Papermill_2010s():
	pm.execute_notebook(
		data_storage_path + 'Single_year.ipynb',
		data_storage_path + 'Single_year_output.ipynb',
		parameters=dict(file_path=data_storage_path + "billboard_spotify_2010s.pickle", year_name="2010s")
		)

t1 = PythonOperator(
	task_id = "download_from_data_world",
	python_callable = grab_data,
	dag = dag)

t1a = PythonOperator(
	task_id = "write_raw_data_to_sql",
	python_callable = downloaded_data_to_mysql,
	dag = dag)

t2 = PythonOperator(
	task_id = "split_1960s",
	python_callable = split_data_1960s,
	dag = dag)

t3 = PythonOperator(
	task_id = "split_1970s",
	python_callable = split_data_1970s,
	dag = dag)

t4 = PythonOperator(
	task_id = "split_1980s",
	python_callable = split_data_1980s,
	dag = dag)

t5 = PythonOperator(
	task_id = "split_1990s",
	python_callable = split_data_1990s,
	dag = dag)

t6 = PythonOperator(
	task_id = "split_2000s",
	python_callable = split_data_2000s,
	dag = dag)

t7 = PythonOperator(
	task_id = "split_2010s",
	python_callable = split_data_2010s,
	dag = dag)

t8 = PythonOperator(
	task_id = "merged_spotify_1960",
	python_callable = merge_1960s,
	dag = dag)

t9 = PythonOperator(
	task_id = "merged_spotify_1970",
	python_callable = merge_1970s,
	dag = dag)

t10 = PythonOperator(
	task_id = "merged_spotify_1980",
	python_callable = merge_1980s,
	dag = dag)

t11 = PythonOperator(
	task_id = "merged_spotify_1990",
	python_callable = merge_1990s,
	dag = dag)

t12 = PythonOperator(
	task_id = "merged_spotify_2000",
	python_callable = merge_2000s,
	dag = dag)

t13 = PythonOperator(
	task_id = "merged_spotify_2010",
	python_callable = merge_2010s,
	dag = dag)

t14 = PythonOperator(
	task_id = "cleaned_data_to_mySql",
	python_callable = cleaned_data_to_MySql,
	dag = dag)

t15 = PythonOperator(
	task_id="run_bash_example_notebook_60s",
	python_callable = Papermill_1960s,
	dag = dag
	)

t16 = PythonOperator(
	task_id="run_bash_example_notebook_70s",
	python_callable = Papermill_1970s,
	dag = dag
	)


t17 = PythonOperator(
	task_id="run_bash_example_notebook_80s",
	python_callable = Papermill_1980s,
	dag = dag
	)


t18 = PythonOperator(
	task_id="run_bash_example_notebook_90s",
	python_callable = Papermill_1990s,
	dag = dag
	)


t19 = PythonOperator(
	task_id="run_bash_example_notebook_00s",
	python_callable = Papermill_2000s,
	dag = dag
	)


t20 = PythonOperator(
	task_id="run_bash_example_notebook_10s",
	python_callable = Papermill_2010s,
	dag = dag
	)

t1 >> t2 >> t8 >> t14 >> t15
t1 >> t3 >> t9 >> t14 >> t16
t1 >> t4 >> t10 >> t14 >> t17
t1 >> t5 >> t11 >> t14 >> t18
t1 >> t6 >> t12 >> t14 >> t19
t1 >> t7 >> t13 >> t14 >> t20
t1 >> t1a


