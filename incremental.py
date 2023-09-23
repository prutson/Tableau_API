from Tableau_jobs import Tableau_jobs

entry_params = {
    'data_source_name': '<Nome da fonte de dados>',
    'task_type': 'IncrementExtractTask',
    'server_name': '<Server name>',
    'version': '<API server version>',
    'user_name': '<Email>',
    'password': '<Password>',
    'site_url_id': ''
}


job = Tableau_jobs(entry_params)
site_id = job.signIn()
datasource_id = job.getDatasourceID(site_id)
task_id = job.getTaskID(site_id, datasource_id)
print(f'Task ID: {task_id}')
job.runTask(site_id, task_id)
job.signOut()