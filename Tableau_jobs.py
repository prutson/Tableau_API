import requests
import json

class Tableau_jobs:
    def __init__(self, entry_params):
        self.data_source_name = entry_params['data_source_name']
        self.task_type = entry_params['task_type']
        self.server_name = entry_params['server_name']
        self.version = entry_params['version']
        self.user_name  = entry_params['user_name']
        self.password = entry_params['password']
        self.site_url_id  = entry_params['site_url_id']
        
        self.url = f'{self.server_name}/api/{self.version}'
        self.headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        self.payload = {
            'credentials': {
                'name': self.user_name,
                'password': self.password,
                'site': {'contentUrl': self.site_url_id}
            }
        }


    def signIn(self):
        signin_url = f'{self.url}/auth/signin'

        req = requests.post(signin_url, json=self.payload, headers=self.headers, verify=False)
        req.raise_for_status()

        response = json.loads(req.content)

        print('Server singin...')

        # Obtenha o token de autenticação do elemento de credenciais
        token = response['credentials']['token']

        # Obtenha o ID do site a partir do elemento <site>
        site_id = response['credentials']['site']['id']

        # Configure o cabeçalho de autenticação usando o token retornado pelo método de Sign In
        self.headers['X-Tableau-Auth'] = token
        
        return site_id

    def getDatasourceID(self, site_id):
        # Defina a URL correta para executar a programação de atualização de extração
        datasource_url = f"{self.url}/sites/{site_id}/datasources?filter=name:eq:{self.data_source_name}"

        # Envie uma solicitação POST com o corpo da solicitação
        req = requests.get(datasource_url, json=self.payload, headers=self.headers, verify=False)
        response = json.loads(req.content)

        datasource_id = response['datasources']['datasource'][0]['id']

        print('Datasource ID collected...')

        return datasource_id
    
    def getTaskID(self, site_id, datasource_id):
        # Buscando a task

        # Defina a URL para obter informações sobre a programação de atualização de extração do datasource
        refresh_schedule_url = f"{self.url}/sites/{site_id}/tasks/extractRefreshes"

        # Envie uma solicitação GET para obter as informações de programação de atualização de extração
        req = requests.get(refresh_schedule_url, headers=self.headers, verify=False)
        response = json.loads(req.content)

        for i in response['tasks']['task']:
            if i['extractRefresh']['datasource']['id'] == datasource_id and i['extractRefresh']['type'] == self.task_type:
                task_id = i['extractRefresh']['id']
                print('Task ID collected...')

        return task_id
    
    def runTask(self, site_id, task_id):
        # Rodar Task ID

        # Defina a URL correta para executar a programação de atualização de extração
        refresh_schedule_url = f"{self.url}/sites/{site_id}/tasks/extractRefreshes/{task_id}/runNow"

        request_body = {}

        # Envie uma solicitação POST com o corpo da solicitação
        req = requests.post(refresh_schedule_url, headers=self.headers, json=request_body, verify=False)
        response = json.loads(req.content)
            
        print(f'Response: {response}')

        print('Task ID running...')

    def signOut(self):
        signout_url = f"{self.url}/auth/signout"
        req = requests.post(signout_url, data=b'', headers=self.headers, verify=False)
        
        req.raise_for_status()

        print('Server logout...')


