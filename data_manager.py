import pandas as pd
import pickle
import os
import numpy as np

ABSOLUTE_PATH = os.path.dirname(os.path.abspath(__file__))

class DataHandler():
    def __init__(self,first_read_flag,local_execution):
        self.crab_jobs_manager = CrabJobsManager()
        self.snapshot_manager = SnapshotManager()
        self.first_read_flag = first_read_flag
        self.local_execution = local_execution
    
    def get_crab_jobs_data(self):
        if not self.local_execution:
            self.crab_jobs_manager.load_new_file_list()
        
        if not self.first_read_flag:
            self.crab_jobs_manager.load_old_file_list()
            self.crab_jobs_manager.compare_file_lists()
            self.crab_jobs_manager.read_old_dataframe()
        
        if self.crab_jobs_manager.new_read_flag:
            self.crab_jobs_manager.create_new_dataframe()
            self.crab_jobs_manager.combine_dataframes()
            self.crab_jobs_manager.save_dataframe()
        
        return self.crab_jobs_manager.df

    def get_snapshot_data(self):
        if self.first_read_flag:
            self.snapshot_manager.create_new_dataframe()
            self.snapshot_manager.get_percentages()
            self.snapshot_manager.save_percentages()
        else:
            self.snapshot_manager.read_old_percentages()

        return self.snapshot_manager.percentages
        

class CrabJobsManager():
    def __init__(self,new_read_flag = True):
        self.all_csvs_path = "/data/cms/scratch2/xcache_studies/crab/out_processed/"
        self.old_csv_list_path = os.path.join(ABSOLUTE_PATH, 'Data','CrabJobs_Data','last_csv_list.pkl')
        self.old_df_path = os.path.join(ABSOLUTE_PATH, 'Data','CrabJobs_Data','last_df.pkl')

        self.new_read_flag = new_read_flag #true si quieres leer nuevo archivos
        self.csv_list = []

    
    def load_new_file_list(self):
        self.csv_list = [csv_file_name for csv_file_name in os.listdir(self.all_csvs_path) if csv_file_name.endswith('.csv')]
    
    def load_old_file_list(self):
        with open(self.old_csv_list_path, 'rb') as file:
            self.old_csv_list = pickle.load(file)
        
    def compare_file_lists(self):
        self.csv_list = list(set(self.csv_list) - set(self.old_csv_list))
        self.new_read_flag =  bool(self.csv_list)

    def read_old_dataframe(self):
        with open(self.old_df_path, 'rb') as file:
            self.df = pickle.load(file)
        self.df['Flag'] = "Old"

    def create_new_dataframe(self):
        columns = ["TiempoUnix", "JobID", "Archivo", "Tamano", "CentroEjecucion", "CentroApertura", "EficienciaCPU", "CPUtime", "Walltime"]        

        self.new_df = pd.concat(
                (pd.read_csv(os.path.join(self.csvs_path, file), names=columns, index_col=False) for file in self.csv_list), ignore_index=True
            )
        
        self.new_df[self.new_df["CentroApertura"] != self.new_df["CentroEjecucion"]]
        
        self.new_df.loc[self.new_df["Tamano"].isna(), "Tamano"] = 2.9  # Mirar si se puede hacer una media

        self.new_df["Dia"] = pd.to_datetime(self.df["TiempoUnix"], unit='s').dt.date

        self.new_df['Arbol'] = self.df['Archivo'].apply(lambda row:'/'.join(row.split('/')[:3]))
        condition = (self.df['Tipo'] == 'RAW') & (self.df['Arbol'] == '/store/data')
        self.new_df[condition, 'Arbol'] = self.df.loc[condition, 'Arbol'] + '/.../RAW'

        self.new_df.groupby.drop_duplicates(inplace=True)

        grupos_duplicados = self.new_df.groupby(['Archivo', 'TiempoUnix']).filter(lambda group: len(group) > 1).groupby(['Archivo', 'TiempoUnix']).head(1)
        df_no_duplicados = self.new_df.groupby(['Archivo', 'TiempoUnix']).filter(lambda group: len(group) == 1)
        self.new_df = pd.concat([df_no_duplicados, grupos_duplicados])

        self.new_df['Flag'] = "New"

        self.new_df.sort_values("TiempoUnix", inplace=True)
    
    def combine_dataframes(self):
        self.df = pd.concat([self.new_df,self.df])
        print(f"Numero de archivo duplicados según el nombre y el tiempounix: {self.df.duplicated(subset=['Archivo', 'TiempoUnix'], keep=False).sum()} (debería haber 0)")
        self.df.sort_values("TiempoUnix", inplace=True)

    def save_dataframe(self):
        with open(self.old_df_path, 'wb') as file:
            pickle.dump(self.df, file)

class SnapshotManager():
    def __init__(self):
        self.old_percentages_path = os.path.join(ABSOLUTE_PATH, 'Data','Percentaje_Data','last_percentage.pkl')
    
    def read_old_percentages(self):
        with open(self.old_percentages_path, 'rb') as file:
            self.percentages = pickle.load(file)

    def create_new_dataframe(self):
        columns = ['Dia', 'Mes', 'Num', 'Hora', 'Año', 'fname', 'size', 'N_accesses', 'Porcentaje_bajado']
        self.df = pd.read_csv(self.per_csv_filepath, sep=' ', usecols=['fname', 'Porcentaje_bajado'], names=columns, index_col=False)

        self.df['Porcentaje_bajado'] = self.df['Porcentaje_bajado'].str.rstrip('%').astype(float)

        self.df["arbol"] = self.df['fname'].apply(lambda x: "/".join(x.split('/')[:3]))
        self.df['type'] = self.df['fname'].apply(lambda x: x.split('/')[:6][-1])

    def get_percentages(self):
        data_dist =  np.array(self.df[(self.df['arbol'] == '/store/data') & (self.df['type'] == 'RAW')]['Porcentaje_bajado'])/100
        mc_dist = np.array(self.df[(self.df['arbol'] == '/store/mc') & (self.df['N_accesses'] == 1)]['Porcentaje_bajado'])/100
        user_dist = np.array(self.df[(self.df['arbol'] == '/store/user') & (self.df['N_accesses'] == 1)]['Porcentaje_bajado'])/100

        self.percentages = (data_dist,mc_dist,user_dist)

    def save_percentages(self):
        with open(self.old_percentages_path, 'wb') as file:
            pickle.dump(self.percentages, file)
