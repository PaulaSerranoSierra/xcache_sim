from data_manager import DataHandler

first_read_flag = False
local_execution = True

data_handler = DataHandler(first_read_flag, local_execution)
df = data_handler.get_crab_jobs_data()
per_df = data_handler.get_snapshot_data()