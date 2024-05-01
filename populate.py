import os
import sys
from datetime import datetime
from datetime import timedelta

cwd=os.getcwd()
sys.path.append(f'../script/')
sys.path.append(f'../psql/')
sys.path.append(f'../temp_storage/')
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from data_extractor import DataExtractor
import db_util

Data_extractor=DataExtractor()

# def separate_data(ti):
#     data_extractor.separate(file_name='20181024_d1_0830_0900.csv',number=5)

def extract_data(ti):

    loaded_df_name=Data_extractor.extract_data(file_name='20181024_d1_0830_0900.csv',return_json=True)
    trajectory_file_name,vehicle_file_name=loaded_df_name
   
    ti.xcom_push(key="trajectory",value=trajectory_file_name)
    ti.xcom_push(key="vehicle",value=vehicle_file_name)

# def create_table():
  #  db_util.create_table()

def populate__vehicles_table(ti):
    trajectory_file_name = ti.xcom_pull(key="trajectory",task_ids='extract_from_file')
    # vehiclea_file_name = ti.xcom_pull(key="vehicle",task_ids='extract_from_file')
    # trajectory_data,vehicle_data=combined_df['trajectory'], combined_df['vehicle']
    db_util.insert_to_table(trajectory_file_name, 'trajectories',from_file=True)
    # db_util.insert_to_table(vehicle_file_name, 'vehicles',from_file=True)

def populate_trajectory_table(ti):
    # trajectory_file_name = ti.xcom_pull(key="trajectory",task_ids='extract_from_file')
    vehicle_file_name = ti.xcom_pull(key="vehicle",task_ids='extract_from_file')
    # trajectory_data,vehicle_data=combined_df['trajectory'], combined_df['vehicle']
    # db_util.insert_to_table(trajectory_file_name, 'trajectories',from_file=True)
    db_util.insert_to_table(vehicle_file_name, 'vehicles',from_file=True)

def clear_memory_vehicle(ti):
    trajectory_file_name = ti.xcom_pull(key="trajectory",task_ids='extract_from_file')
    # vehicle_file_name = ti.xcom_pull(key="vehicle",task_ids='extract_from_file')

    os.remove(f'../temp_storage/{trajectory_file_name}')
    # os.remove(f'../temp_storage/{vehicle_file_name}')

def clear_memory_trajectory(ti):
    # trajectory_file_name = ti.xcom_pull(key="trajectory",task_ids='extract_from_file')
    vehicle_file_name = ti.xcom_pull(key="vehicle",task_ids='extract_from_file')

    # os.remove(f'../temp_storage/{trajectory_file_name}')
    os.remove(f'../temp_storage/{vehicle_file_name}')
