from gretel_client import configure_session, ClientConfig, get_project, create_project
from gretel_client.projects import Project

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class GretelHook(BaseHook):
    conn_name_attr = "gretel_conn_id"
    default_conn_name = "gretel_default"
    conn_type = "gretel"
    hook_name = "Gretel"

    def __init__(self, gretel_conn_id: str = "gretel_default") -> None:
        super().__init__()
        self.gretel_conn_id = gretel_conn_id
        self.connection = self.get_connection(self.gretel_conn_id)
        self.extras = self.connection.extra_dejson
        self.api_key = self.extras.get("api_key")
        self.project_id  = self.extras.get("project_id")
        if self.api_key is None:
            raise AirflowException("No Gretel api key provided")
        self.client_config = ClientConfig(api_key=self.api_key)
        configure_session(self.client_config)

    def get_conn(self) -> ClientConfig:
        return self.client_config

    def get_project(self, **kwargs) -> Project:
        if self.project_id and not kwargs.get("project_id"):
            return get_project(name=self.project_id)
        else:
            return get_project(**kwargs)

    @property
    def default_project(self) -> Project:
        if not self.project_id:
            raise Exception("No default project specified")
        return get_project(name=self.project_id)

    def create_project(self, *args, **kwargs) -> Project:
        return create_project(*args, **kwargs)
