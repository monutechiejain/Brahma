
from entities.GlobalConfigurationTable import GlobalConfigurationTable
from config.database.Sqlalchemy import Sqlalchemy
import logging
import pandas as pd

def insertConfigurations(schema_name, global_configuartion_dict):
    '''
    Insert Configurations for IA
    :return:
    '''

    ########################################### Delete Configurations ##################################################
    deleteConfigurations(schema_name)
    ####################################################################################################################

    session = Sqlalchemy(schema_name)

    # Adding Constant Values
    for key, value in iter(global_configuartion_dict.items()):
        config_param = GlobalConfigurationTable(key=key,
                                          value=value
                                          )
        session.add(config_param)

    session.commit()
    session.close()

def getConfiguration(schema_name, key):
    '''
    GET CONFIGURATION KEY
    :param key:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    configurations_result = session.query(GlobalConfigurationTable).filter(GlobalConfigurationTable.key == key)
    session.close()
    return configurations_result[0].value

def getConfigurations(schema_name):
    '''
    GET CONFIGURATION KEY
    :param key:
    :return:
    '''

    Configuration = {}

    ############################################### 1. DB CONFIGURATIONS ###############################################
    session = Sqlalchemy(schema_name)
    for conf in session.query(GlobalConfigurationTable).all():
        Configuration[conf.key] = conf.value
    session.close()

    return Configuration

def fetchAllConfiguartions(schema_name):
    '''
    GET CONFIGURATION KEY
    :param key:
    :return:
    '''

    Configuration = {}

    ############################################### 1. DB CONFIGURATIONS ###############################################
    session = Sqlalchemy(schema_name)
    global_configurations_result = session.query(GlobalConfigurationTable)
    df_configurations_result = pd.read_sql(global_configurations_result.statement, session.bind)
    session.close()
    return df_configurations_result

def updateConfiguration(schema_name, key, value):
    session = Sqlalchemy(schema_name)
    session.query(GlobalConfigurationTable).filter(GlobalConfigurationTable.key == key). \
        update({'value': value})
    session.commit()
    session.close()

def insertOrUpdateConfigurations(schema_name, configuration_dict, skip_update_list=None):
    '''
    Insert OR Update if exists Params in Cobfiguration Table
    Also Use same trans id from Coefficient Table
    :return:
    '''
    session = Sqlalchemy(schema_name)

    for configuration_key, configuration_value in iter(configuration_dict.items()):

        if skip_update_list is None or (configuration_key not in skip_update_list) :
            configuration_row = session.query(GlobalConfigurationTable).filter(GlobalConfigurationTable.key == configuration_key).first()
            if not configuration_row:
                model_param = GlobalConfigurationTable(key=configuration_key,
                                                 value=configuration_value
                                                   )
                session.add(model_param)
            else:
                session.query(GlobalConfigurationTable).filter(GlobalConfigurationTable.key == configuration_key). \
                    update({'key': str(configuration_key),
                            'value': str(configuration_value)
                            })

            session.commit()
    session.close()

def deleteConfigurations(schema_name):
    '''
    Delete Configurations for IA
    :return:
    '''
    session = Sqlalchemy(schema_name)

    session.query(GlobalConfigurationTable).delete()
    session.commit()
    session.close()
    print("Deleted Configurations.")
    logging.info("Deleted Configurations.")

def resetConfigurations(schema_name, global_configuration_dict):
    deleteConfigurations(schema_name)
    insertOrUpdateConfigurations(global_configuration_dict)
    print("Reset Configurations Successfully!!")


if __name__ == "__main__":

    # Insert in Configuration table
    #insertConfigurations()

    # Reset Configurations
    resetConfigurations()

    # GET Configuration
    #print(getConfiguration('MAX_MF_NIFTY'))

    # UPDATE CONFIGURATION
    #updateConfiguration('TOTAL_INITIAL_MARGIN', '1200000')
    #updateConfiguration('TOTAL_INITIAL_MARGIN', '2200000')
    #updateConfiguration('IS_NEXT_WEEK_ENABLED_EARLY', 'N')

    # INSERT OR UPDATE CONFIGURATION
    # configuration_dict = {'MIN_MARGIN_CHK_PCT' : '1',
    #                       'MARGIN_ONE_MF_NIFTY': '60000',
    #                       'MARGIN_ONE_MF_BANKNIFTY': '60000'}
    # insertOrUpdateConfigurations(configuration_dict)

    # FILTER LEVEL OPTION CHAIN
    #updateConfiguration('FILTER_LEVEL_OPTION_CHAIN', '15')


    # getConfigurations()
    print(getConfigurations())


