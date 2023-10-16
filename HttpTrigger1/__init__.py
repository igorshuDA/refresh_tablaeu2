import logging

import azure.functions as func
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying

def get_connection():
    tableau_server_config = {
        'tableau_prod': {
            'server': 'https://prod-useast-b.online.tableau.com',
            'api_version': '3.21',
            'personal_access_token_name': 'test_token',
            'personal_access_token_secret': 'ljYcp5f7R8WZ5fTTtHD5eA==:QT8CEKM7yGqSvFwuNghLrv5MYR8c2QRR',
            'site_name': 'granulate',
            'site_url': 'granulate'
        }
    }
    connection = TableauServerConnection(tableau_server_config)
    connection.sign_in()
    print('create connection successfully')
    return connection


def get_data_source_ids(connection, ids):
    df = querying.get_datasources_dataframe(connection)
    selected_rows = df[df['name'].isin(ids)]
    selected_ids = selected_rows['id'].to_list()
    return selected_ids


def refresh_data_source(connection, source_ids):
    for source_id in source_ids:
        connection.update_data_source_now(source_id)
        print(f'refresh {source_id} successfully')

def refresh_tablaeu_main():
    conn = get_connection()
    data_source_names = ['SalesForce & ClickUp BI Datasource']
    data_source_ids = get_data_source_ids(conn, data_source_names)
    refresh_data_source(conn, data_source_ids)

def main(req: func.HttpRequest) -> func.HttpResponse:
    print('Igor2"')
    
    logging.info('Python HTTP trigger function processed a request.')
    refresh_tablaeu_main()
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
