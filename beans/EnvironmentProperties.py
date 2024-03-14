class EnvironmentProperties:
    mysql_username = None
    mysql_password = None
    mysql_host = None
    mysql_port = None
    env_config_refresh_time = None
    strategyName=None
    mysql_db_name=None

    def __init__(self, **kwargs):
        for temp in kwargs:
            setattr(EnvironmentProperties, temp, kwargs[temp])
