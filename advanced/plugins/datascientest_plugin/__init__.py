from airflow.plugins_manager import AirflowPlugin

from datascientest_plugin.operators.MySQLToMongoOperator import MySQLToMongoOperator


class DatascientestPlugin(AirflowPlugin):
    name = 'datascientest_plugin'
    operators = [MySQLToMongoOperator]
    sensors = []
    hooks = []
    executors = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
