from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add 
import pendulum
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import random

import os
from os.path import join

########################################################################################################
######################### input parameters for the create_df_modeling function #########################
# Define the range of dates for the trasactional dataset
start_date = datetime(2023, 1, 1).strftime('%Y-%m-%d')
end_date = datetime(2023, 1, 31).strftime('%Y-%m-%d')
# defines the minimum id_user to consider
low_user = 1
# defines the maximum id_user to consider
high_user = 50
# defines the daily audience (the percentage of the sample size of users) for each id_date
dau = 0.1
# defines the minimum id_track available
low_track = 1
# defines the maximum id_track available
high_track = 4000
# defines the minimum number of plays a listener can hit play every day
min_plays = 1
# defines the maximum number of plays a listener can hit play every day
max_plays = 20
# defines the minimum number of tracks a listener can listen to every day
min_n_tracks = 2
# defines the maximum number of tracks a listener can listen to every day
max_n_tracks = 5


########################################################################################################
################ input parameters for the create_dim_content function ##################################
# defines the total number of tracks
n_tracks = high_track
# defines the total number of id_artist
n_artists = 500
# defines the total number of genres
n_genres = 5
# defines the minimum number of tracks by id_artist
min_tracks = 2 * round((n_tracks/n_artists),0) * 0.2
# defines the maximum number of tracks by id_artist
max_tracks = 2 * round((n_tracks/n_artists),0) * 0.8


########################################################################################################
################ input parameters for the creates_features function ##################################
# number of features that are going to appear on the final dataframe
n_features = 10
# maximum standard deviation value for each of the feature
std_max = 1
# minimum value for the mean value for each of the features
min_mean = 100
# maximum value for the mean value for each of the features
max_mean = 1500

def create_folder(folder_path):
    """Cria uma pasta no caminho especificado."""
    os.makedirs(folder_path, exist_ok=True)

# Funções que vão gerar os dados 

## transactional data
def create_df_modeling(start_date, end_date,
                       low_user, high_user, dau,
                       low_track, high_track, 
                       min_plays, max_plays,
                       min_n_tracks, max_n_tracks,
                       data_interval_end
                      ):
    # creates a range of dates
    date_range = pd.date_range(start_date, end_date)
    # creates empty array for the final user array
    user_array_acum = []
    # creates empty array for the final id_date array
    date_array_acum = []
    # creates empty array for the final track array
    tracks_array_acum = []
    # creates empty array for the final plays array
    plays_array_acum = []
    # minimum number of random listeners per day (dau)
    length_user = 1 + int((high_user-low_user) * dau)
    # for loop to random create transactional data
    for current_date in date_range:
        #
        user_array = np.random.randint(low_user, high_user, size = length_user)
        user_array_col = np.reshape(user_array, (len(user_array), 1))
        user_array_acum.append(user_array_col)
        #
        date_array = current_date.strftime("%Y-%m-%d")
        date_array = np.full(len(user_array), current_date)
        date_array_col = np.reshape(date_array, (len(date_array), 1))
        date_array_acum.append(date_array_col)
    # for loop that ranges from the min id_user to the max id_user
    for k in range(0,len(np.concatenate(user_array_acum))):
        # defines randomly the maximum range of tracks a listener can listen in 1 single day
        length_size = random.randint(min_n_tracks, max_n_tracks)
        # selects at random an array of id_track for the specific listener
        tracks_array = np.random.randint(low_track, high_track, size = length_size)
        tracks_array_acum.append(tracks_array)
        # select at random the average of plays given by each user on a giving day
        plays_array = np.random.randint(min_plays, max_plays, size = length_size)
        plays_array_acum.append(plays_array) 
    # concatenates the arrays of date, id_user and plays on the final dataframe
    date_array_final = np.concatenate(date_array_acum, axis = 0)
    user_array_final = np.concatenate(user_array_acum, axis = 0)
    # creates the final transactional dataframe with id_users and number of plays for each day
    df = pd.DataFrame({'id_date':date_array_final.reshape(len(date_array_final)),
                       'user_id':user_array_final.reshape(len(user_array_final)),
                       'id_tracks': tracks_array_acum,
                       'plays':plays_array_acum
                      })
    # explodes the final dataframe to create a vertical transactional table
    df = df.explode(['id_tracks','plays']).reset_index(drop=True)
    #
    file_path = f'./dags/dia={data_interval_end}/'

    df.to_csv(file_path + 'df_transactional.csv', index=False)

## Dimensional content

def create_dim_content(n_tracks, min_tracks, max_tracks, n_artists, n_genres, data_interval_end):
    # creates an initial list of id_tracks
    id_track_list = [i for i in range(1,n_tracks+1)]
    # creates an initial list of id_artists
    id_artist_list = [i for i in range(1, n_artists+1)]
    # creates an initial list of id_genres
    id_genres = [i for i in range(1,n_genres)]
    # creates array with the specific size of tracks by artists to be distributed amongst all artists
    artist_array_sizes = np.random.randint(min_tracks, max_tracks, size = n_artists).tolist()
    # Create a copy of the list
    list_copy = id_track_list.copy()
    # initiates an empty array to store results of for loop append (arrays of id_tracks)
    sampled_arrays_acum = []
    # initiates an empty array to store results of for loop append (arrays of id_genre)
    sampled_arrays_genre_acum = []
    # for loop to interate and create the dimensional data for id_artists and id_tracks
    for size in artist_array_sizes:
        if size <= len(list_copy):
            sampled_arrays = random.sample(list_copy, size)
            sampled_arrays_genre = random.choices(id_genres, k = size)
            sampled_arrays_acum.append(sampled_arrays)
            sampled_arrays_genre_acum.append(sampled_arrays_genre)
            list_copy = [item for item in list_copy if item not in sampled_arrays]
    # creates the final dimensional dataframe
    df = pd.DataFrame({'id_artist':id_artist_list,
                       'id_tracks':sampled_arrays_acum,
                       'id_genre':sampled_arrays_genre_acum
                      })
    # explodes the final dataframe to create a vertical dimensional table
    df = df.explode(['id_tracks','id_genre']).reset_index(drop = True)
    #
    file_path = f'./dags/dia={data_interval_end}/'

    df.to_csv(file_path + 'df_dimentional.csv', index=False)

## Features for artists

def creates_features(n_features,n_artists,std_max, min_mean, max_mean, data_interval_end):
    # creates an array with standard deviation of a uniform distribution
    std_array = [random.uniform(0, std_max) for i in range(0,n_features)]
    # creates an array with mean values of a uniform distribution
    mean_array = [random.randint(min_mean, max_mean) for i in range(0,n_features)]
    # creates an empty array with the number of rows as the same of the length of the quantity of artists
    result = np.empty((n_artists, 0))
    for i in range(0,n_features):
        feature_i = np.random.normal(mean_array[i], std_array[i] * mean_array[i], n_artists)
        feature_i = feature_i.astype(int)
        result = np.column_stack((result, feature_i))
        result = result.astype(int)
    # creates the list of id_artist
    id_artist = [i for i in range(1,n_artists+1)]
    # fills values on the result array
    result = np.column_stack((id_artist,result))
    # creates a list with only one element
    artist_col_names = ['id_artist']
    # creates a list with the names of the features that will be used on the final dataframe
    feat_col_names = ['Feature'+str(i) for i in range(1,n_features+1)]
    # adds two lists (elements will be use as the header of the final dataframe)
    col_names = artist_col_names + feat_col_names
    # final dataframe with features by artist
    df = pd.DataFrame(result, columns = col_names)
    
    file_path = f'./dags/dia={data_interval_end}/'

    df.to_csv(file_path + 'df_feat_artist.csv', index=False)
 
# 0 no campo dos minutos, indicando que a tarefa será executada no início de cada hora.
# 0 no campo da hora, indicando que a tarefa será executada à meia-noite.
# * no campo do Dia do Mês, indicando que a tarefa é independente do dia do mês.
# * no campo do Mês, indicando que a tarefa é independente do mês.
# 1 no campo do Dia da Semana, indicando que a tarefa será executada às segundas-feiras.

with DAG(
    dag_id='suamusica_dag01',
    start_date=pendulum.datetime(2024, 2, 8, tz="UTC"),
    schedule_interval='0 0 * * 1' # CRON Expressions
) as dag:
    
    tarefa_1 = PythonOperator(
    task_id='cria_pasta',
        python_callable=create_folder,
        op_args=['./dags/dia={{data_interval_end.strftime("%Y-%m-%d")}}'],
        dag=dag
    )

    tarefa_2 = PythonOperator(
        task_id='create_df_modeling',
        python_callable=create_df_modeling,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}',
                   'start_date':start_date, 'end_date':end_date,
                    'low_user':low_user, 'high_user':high_user, 'dau':dau,
                    'low_track':low_track, 'high_track':high_track, 
                    'min_plays':min_plays, 'max_plays':max_plays,
                    'min_n_tracks':min_n_tracks, 'max_n_tracks':max_n_tracks},
        dag=dag
    )   
    
    tarefa_3 = PythonOperator(
        task_id='create_dim_content',
        python_callable=create_dim_content,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}',
                   'n_tracks':n_tracks,'min_tracks':min_tracks,
                   'max_tracks':max_tracks,'n_artists':n_artists, 'n_genres':n_genres
                   },
        dag=dag
    )
        
    tarefa_4 = PythonOperator(
        task_id='feat_artist',
        python_callable=creates_features,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}',
                   'n_features': n_features,
                   'n_artists': n_artists,
                   'std_max': std_max,
                   'min_mean': min_mean,
                   'max_mean': max_mean},
        dag=dag)

    tarefa_1 >> [tarefa_2, tarefa_3, tarefa_4]