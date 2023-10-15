from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import cx_Oracle
import psycopg2
from amukhsimov_jupyter_templates_bigdata import encode_text, decode_text
#from delta import *

load_dotenv(os.join(JUPYTER_DIR, 'env', '.env'))
load_dotenv(os.join(JUPYTER_DIR, 'env', 'creds.env'))

FC_TEST_IP = os.getenv("FC_TEST_IP", None)
FC_TEST_PORT = os.getenv("FC_TEST_PORT", None)
FC_TEST_SID = os.getenv("FC_TEST_SID", None)
FC_TEST_USERNAME = os.getenv("FC_TEST_USERNAME", None)
FC_TEST_PASSWORD = os.getenv("FC_TEST_PASSWORD", None)


class Connector:
    def __init__(self):
        pass

    def get_spark_connector(self, app_name: str):
        return SparkSession.builder.appName(app_name).getOrCreate()

    def fc_test_creds(self, env, master_password):
        if env == "test":
            ip = decode_text(FC_TEST_IP, master_password)
            port = decode_text(FC_TEST_PORT, master_password)
            sid = decode_text(FC_TEST_SID, master_password)
            username = decode_text(FC_TEST_USERNAME, master_password)
            password = decode_text(FC_TEST_PASSWORD, master_password)
        else:
            raise ValueError(f"env '{env}' is unsupported for flexcube")
        return ip, port, sid, username, password

    def get_fc_connector(self, env, master_password):
        """
        :param env: 'prod'|'preprod'
        :return:
        """
        ip, port, sid, username, password = self.fc_test_creds(env, master_password)
        dsn_tns_iabs = cx_Oracle.makedsn(ip, port, service_name=sid)
        return cx_Oracle.connect(user=username, password=password, dsn=dsn_tns_iabs, encoding="UTF-8")

    def get_fc_jdbc(self, env, master_password):
        """
        :param env: 'test'
        :return:
        """
        ip, port, sid, username, password = self.fc_test_creds(env, master_password)
        return f"jdbc:oracle:thin:@{ip}:{port}:{sid}"

    # def get_microservices_connector(self, env, database, master_password):
    #     """
    #     :param env: 'prod'|'preprod'
    #     :return:
    #     """
    #     ip, port, username, password = self._get_microservices_creds(env, master_password)

    #     return psycopg2.connect(host=ip, port=port, database=database, user=username, password=password)

    # def get_microservices_jdbc(self, env, database, master_password):
    #     """
    #     :param env: 'prod'|'preprod'
    #     :return:
    #     """
    #     ip, port, username, password = self._get_microservices_creds(env, master_password)
    #     return f"jdbc:postgresql://{ip}:{port}/{database}"

connector = Connector()

# Establish connections
spark = connector.get_spark_connector(app_name=f"{mynameis}_shell")
# fc_test_jdbc = connector.get_iabs_jdbc(env='test', master_password=master_password)
# fc_ip, fc_port, fc_sid, fc_username, fc_password = connector.get_fc_creds(env=env, master_password=master_password)

# connection_fc = connector.get_iabs_connector(env=iabs_env, master_password=master_password)
