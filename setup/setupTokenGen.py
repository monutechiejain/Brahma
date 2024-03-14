
from common.constants import PROFILE_PROPERTIES
from dao import GlobalConfigurationDAO, LiveAccountsDAO, ClientConfigurationDAO
from configparser import ConfigParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests
import traceback

def configSetUp():
    # FETCH ENV
    env = PROFILE_PROPERTIES['ACTIVE_ENV']

    # GLOBAL CONFIGURATION SETUP
    file_path_global = "resources/"+env+"/brahma.properties"
    global_configuration_dict = populateConfiguration(file_path_global)
    GlobalConfigurationDAO.insertConfigurations(PROFILE_PROPERTIES['SCHEMA_NAME'], global_configuration_dict)
    print("\n Global Configurations :: \n"+str(global_configuration_dict))

    # FETCH CUSTOMERS
    df_live_strategy = LiveAccountsDAO.fetchCustomers()
    customers_list = df_live_strategy['CLUSTER_ID'].tolist()
    print("\n Customers List :: \n"+str(customers_list))

    # POPULATE CLIENT CONFIGS AND PERFORM OPERATIONS ON EACH CUSTOMER
    for customer in customers_list:
        file_path_client = 'resources/'+env+'/client_config/'+customer+'.properties'
        client_configuration_dict = populateConfiguration(file_path_client)
        ClientConfigurationDAO.insertOrUpdateConfigurations(customer, client_configuration_dict, skip_update_list = ['Z_API_KEY', 'TOKEN'])
        print("\n Client Configuration :: \n"+str(client_configuration_dict))

        # TODO: TOKEN GENERATION: REMOVE WHEN PERMANENT FIX IN PLACE OR CENTRALISED TOKEN GEN
        tokenGenerationSetup(client_configuration_dict, customer)
    ####################################################################################################################

def tokenGeneration(username, password, pin, client_id, secret_key, redirect_uri, schema_name):
    from fyers_api import accessToken
    try:
        app_id = client_id[:-4]  # "L9NY****W" (don't change this app_id variable)
        session = accessToken.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )

        s = requests.Session()

        data1 = f'{{"fy_id":"{username}","password":"{password}","app_id":"2","imei":"","recaptcha_token":""}}'
        r1 = s.post("https://api.fyers.in/vagator/v1/login", data=data1)
        print(r1)
        assert r1.status_code == 200, f"Error in r1:\n {r1.json()}"

        request_key = r1.json()["request_key"]

        data2 = f'{{"request_key":"{request_key}","identity_type":"pin","identifier":"{pin}","recaptcha_token":""}}'
        r2 = s.post("https://api.fyers.in/vagator/v1/verify_pin", data=data2)
        print(r2)
        assert r2.status_code == 200, f"Error in r2:\n {r2.json()}"

        headers = {"authorization": f"Bearer {r2.json()['data']['access_token']}", "content-type": "application/json; charset=UTF-8"}
        data3 = f'{{"fyers_id":"{username}","app_id":"{app_id}","redirect_uri":"{redirect_uri}","appType":"100","code_challenge":"","state":"abcdefg","scope":"","nonce":"","response_type":"code","create_cookie":true}}'
        r3 = s.post("https://api.fyers.in/api/v2/token", headers=headers, data=data3)
        print(r3)

        assert r3.status_code == 308, f"Error in r3:\n {r3.json()}"

        parsed = urlparse(r3.json()["Url"])
        auth_code = parse_qs(parsed.query)["auth_code"][0]
        session.set_token(auth_code)
        response = session.generate_token()
        token = response["access_token"]
        print("USER : "+username)
        print("CLIENT_ID : " + client_id)
        print("Token: "+token)

        # UPDATE DB WITH LATEST TOKEN
        ClientConfigurationDAO.updateConfiguration(schema_name, 'Z_API_KEY', client_id)
        ClientConfigurationDAO.updateConfiguration(schema_name, 'TOKEN', token)
        # write_file(token)
        print("Got the access token!!!")
        # fyers = fyersModel.FyersModel(client_id=client_id, token=token, log_path=os.getcwd())
        # print(fyers.get_profile())
        return True
    except Exception as e:
        traceback.print_exc()
        return False


def tokenGenerationSetup(Configuration, customer):

    # TOKEN GENERATION FOR PUNNESH
    schema_name = customer
    if Configuration['BROKER'] == 'fyers' and 'BRAHMA_V1_PRE_PRD_PT' in schema_name:
        # PUNEESH
        username = "XP18168"  # fyers_id
        password = "Puneesh@123"  # fyers_password
        pin = "0880"  # your integer pin
        client_id = "0X4AWOIEQN-100"  # "L9NY****W-100" (Client_id here refers to APP_ID of the created app)
        secret_key = "NVHR98NAIL"  # app_secret key which you got after creating the app
        redirect_uri = "https://myaccount.fyers.in/"  # redircet_uri you entered while creating APP.
        tokenGeneration(username, password, pin, client_id, secret_key, redirect_uri, schema_name)

    # TOKEN GENERATION FOR PUNNESH
    if Configuration['BROKER'] == 'fyers' and 'BRAHMA_V1_PRE_PRD_KG' in schema_name:
        # KG
        username = "XK15235"  # fyers_id
        password = "Lucky@276977"  # fyers_password
        pin = "1966"  # your integer pin
        client_id = "SZLF2Q8U9J-100"  # "L9NY****W-100" (Client_id here refers to APP_ID of the created app)
        secret_key = "RVXGJX4B39"  # app_secret key which you got after creating the app
        redirect_uri = "https://myaccount.fyers.in/"  # redircet_uri you entered while creating APP.
        tokenGeneration(username, password, pin, client_id, secret_key, redirect_uri, schema_name)
    ####################################################################################################################


def populateConfiguration(file_path):
    config_server_dict = {}
    config = ConfigParser()
    path = Path(__file__).parent.parent / file_path
    with path.open() as f:
        config.read_string('[config]\n' + f.read())

    for k, v in config['config'].items():
        config_server_dict[k.upper()] = v

    return config_server_dict


if __name__ == "__main__":
    configSetUp()
