import logging
import re

import requests
import trino

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('requests').setLevel(logging.DEBUG)
logging.getLogger('urllib3').setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def response_hook(response, *args, **kwargs):
    if re.search(r"/v1/statement", response.url):
        logging.info(f"Intercepted response for URL: {response.url}")
        logging.info(f"Response Status: {response.status_code}")
        try:
            #
            json_data = response.json()
            if 'data' in json_data:
                del json_data['data']
            logging.info(f"Response: {json_data}")
        except ValueError as e:
            logging.error(f"Failed to parse JSON response, error: {e}")
            logging.info(f"Response Body: {response.text}")
    return response


if __name__ == '__main__':
    session = requests.Session()
    session.hooks['response'] = [response_hook]

    conn = trino.dbapi.connect(
        host='localhost',
        port=8080,
        user='user',
        catalog='tpch',
        schema='sf1',
        http_scheme='http',
        # http_session=session
    )

    query = "SELECT * FROM lineitem limit 10"
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    print("fetch data total:", len(rows))
