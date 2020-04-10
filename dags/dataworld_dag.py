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
	with open("/Users/jkocher/Documents/airflow_home/data/audio_features.pickle", 'wb') as f:
		pickle.dump(features_df, f)
	with open("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle", 'wb') as d:
		pickle.dump(billboard_df, d)

def split_data_1960s():
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	blillboard_df_1960s = billboard_df[billboard_df["year"] >= 1960]
	blillboard_df_1960s = blillboard_df_1960s[blillboard_df_1960s["year"]<=1969]
	blillboard_df_1960s= blillboard_df_1960s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1960s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1960s, d)

def split_data_1970s():
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	blillboard_df_1970s = billboard_df[billboard_df["year"] >= 1970]
	blillboard_df_1970s = blillboard_df_1970s[blillboard_df_1970s["year"]<=1979]
	blillboard_df_1970s = blillboard_df_1970s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1970s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1970s, d)

def split_data_1980s():
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	blillboard_df_1980s = billboard_df[billboard_df["year"] >= 1980]
	blillboard_df_1980s = blillboard_df_1980s[blillboard_df_1980s["year"]<=1989]
	blillboard_df_1980s = blillboard_df_1980s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1980s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1980s, d)

def split_data_1990s():
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	blillboard_df_1990s = billboard_df[billboard_df["year"] >= 1990]
	blillboard_df_1990s = blillboard_df_1990s[blillboard_df_1990s["year"]<=1999]
	blillboard_df_1990s = blillboard_df_1990s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1990s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_1990s, d)

def split_data_2000s():
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	blillboard_df_2000s = billboard_df[billboard_df["year"] >= 2000]
	blillboard_df_2000s = blillboard_df_2000s[blillboard_df_2000s["year"]<=2009]
	blillboard_df_2000s = blillboard_df_2000s.groupby(["performer", "song","year","songid"]).agg({"week_position":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_2000s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_2000s, d)

def split_data_2010s():
	billboard_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/billboard_rankings.pickle")
	blillboard_df_2010s = billboard_df[billboard_df["year"] >= 2010]
	blillboard_df_2010s = blillboard_df_2010s[blillboard_df_2010s["year"]<=2019]
	blillboard_df_2010s = blillboard_df_2010s.groupby(["song","performer","songid"]).agg({"week_position":"min", "year":"min"})
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_df_2010s.pickle", 'wb') as d:
	    pickle.dump(blillboard_df_2010s, d)

def merge_1960s():
	blillboard_df_1960s = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1960s.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/projects/DataEngineering.Labs.AirflowProject/data/audio_features.pickle")
	top1960_df = pd.merge(blillboard_df_1960s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1960_df= top1960_df.dropna(subset=["key"])
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1960s.pickle", 'wb') as d:
	    pickle.dump(top1960_df, d)

def merge_1970s():
	blillboard_df_1970s = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1970s.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/projects/DataEngineering.Labs.AirflowProject/data/audio_features.pickle")
	top1970_df = pd.merge(blillboard_df_1970s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1970_df= top1970_df.dropna(subset=["key"])
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1970s.pickle", 'wb') as d:
	    pickle.dump(top1970_df, d)

def merge_1980s():
	blillboard_df_1980s = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1980s.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/projects/DataEngineering.Labs.AirflowProject/data/audio_features.pickle")
	top1980_df = pd.merge(blillboard_df_1980s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1980_df= top1980_df.dropna(subset=["key"])
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1980s.pickle", 'wb') as d:
	    pickle.dump(top1980_df, d)

def merge_1990s():
	blillboard_df_1990s = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_df_1990s.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/projects/DataEngineering.Labs.AirflowProject/data/audio_features.pickle")
	top1990_df = pd.merge(blillboard_df_1990s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top1990_df= top1990_df.dropna(subset=["key"])
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1990s.pickle", 'wb') as d:
	    pickle.dump(top1990_df, d)


def merge_2000s():
	blillboard_df_2000s = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_df_2000s.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/projects/DataEngineering.Labs.AirflowProject/data/audio_features.pickle")
	top2000_df = pd.merge(blillboard_df_2000s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top2000_df= top2000_df.dropna(subset=["key"])
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_2000s.pickle", 'wb') as d:
	    pickle.dump(top2000_df, d)

def merge_2010s():
	blillboard_df_2010s = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_df_2010s.pickle")
	spotify_df = pd.read_pickle("/Users/jkocher/Documents/projects/DataEngineering.Labs.AirflowProject/data/audio_features.pickle")
	top2010_df = pd.merge(blillboard_df_2010s, spotify_df, left_on = "songid", right_on = "songid", how= "left")
	top2010_df= top2010_df.dropna(subset=["key"])
	with open("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_2010s.pickle", 'wb') as d:
	    pickle.dump(top2010_df, d)

def cleaned_data_to_MySql():
	top1960_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1960s.pickle")
	top1970_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1970s.pickle")
	top1980_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1980s.pickle")
	top1990_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1990s.pickle")
	top2000_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_2000s.pickle")
	top2010_df = pd.read_pickle("/Users/jkocher/Documents/airflow_home/data/blillboard_spotify_2010s.pickle")
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

t15 = BashOperator(task_id="run_bash_example_notebook_60s",
	bash_command= "papermill /Users/jkocher/Documents/airflow_home/data/Single_year.ipynb /Users/jkocher/Documents/airflow_home/data/Single_year_output.ipynb -p file_path /Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1960s.pickle -pyear_name 1960s",
	dag = dag)

t16 = BashOperator(task_id="run_bash_example_notebook_70s",
	bash_command= "papermill /Users/jkocher/Documents/airflow_home/data/Single_year.ipynb /Users/jkocher/Documents/airflow_home/data/Single_year_output.ipynb -p file_path /Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1970s.pickle -pyear_name 1970s",
	dag = dag)

t17 = BashOperator(task_id="run_bash_example_notebook_80s",
	bash_command= "papermill /Users/jkocher/Documents/airflow_home/data/Single_year.ipynb /Users/jkocher/Documents/airflow_home/data/Single_year_output.ipynb -p file_path /Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1980s.pickle -pyear_name 1980s",
	dag = dag)

t18 = BashOperator(task_id="run_bash_example_notebook_90s",
	bash_command= "papermill /Users/jkocher/Documents/airflow_home/data/Single_year.ipynb /Users/jkocher/Documents/airflow_home/data/Single_year_output.ipynb -p file_path /Users/jkocher/Documents/airflow_home/data/blillboard_spotify_1990s.pickle -pyear_name 1990s",
	dag = dag)

t19 = BashOperator(task_id="run_bash_example_notebook_00s",
	bash_command= "papermill /Users/jkocher/Documents/airflow_home/data/Single_year.ipynb /Users/jkocher/Documents/airflow_home/data/Single_year_output.ipynb -p file_path /Users/jkocher/Documents/airflow_home/data/blillboard_spotify_2000s.pickle -pyear_name 2000s",
	dag = dag)

t20 = BashOperator(task_id="run_bash_example_notebook_10s",
	bash_command= "papermill /Users/jkocher/Documents/airflow_home/data/Single_year.ipynb /Users/jkocher/Documents/airflow_home/data/Single_year_output.ipynb -p file_path /Users/jkocher/Documents/airflow_home/data/blillboard_spotify_2010s.pickle -pyear_name 2010s",
	dag = dag)

t1 >> t2 >> t8 >> t14 >> t15
t1 >> t3 >> t9 >> t14 >> t16
t1 >> t4 >> t10 >> t14 >> t17
t1 >> t5 >> t11 >> t14 >> t18
t1 >> t6 >> t12 >> t14 >> t19
t1 >> t7 >> t13 >> t14 >> t20
t1 >> t1a

