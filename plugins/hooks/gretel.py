from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from gretel_client import ClientConfig, configure_session, create_project, get_project
from gretel_client.projects import Project


class GretelHook(BaseHook):
    """Gretel Airflow hook for managing API connections.

    A Gretel connections follow the HTTP URI convention. Eg

        http://api.gretel.cloud?api_key=INSERT_API_KEY&project_id=INSERT_PROJECT_ID

    You can find your api key by navigating to the Gretel Console.
    """
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
            raise AirflowException("No Gretel API key provided")
        self.client_config = ClientConfig(api_key=self.api_key)
        configure_session(self.client_config)

    def get_conn(self) -> ClientConfig:
        """Get the ``ClientConfig`` configured for the seesion.

        For the ``ClientConfig`` API please reference

            https://python.docs.gretel.ai/en/stable/config.html
        """
        return self.client_config

    def get_project(self, **kwargs) -> Project:
        """Returns a ``Project`` for the configured connection.

        ``*args`` and ``**kwargs`` are passed through to the
        ``get_project`` method on the Gretel SDK. For more
        documentation please refer to

            https://python.docs.gretel.ai/en/stable/projects/projects.html#gretel_client.projects.projects.get_project
        """
        if self.project_id and not kwargs.get("project_id"):
            return get_project(name=self.project_id)
        else:
            return get_project(**kwargs)

    @property
    def default_project(self) -> Project:
        """Returns the default ``Project`` for the configured connection."""
        if not self.project_id:
            raise Exception("No default project specified")
        return get_project(name=self.project_id)

    def create_project(self, *args, **kwargs) -> Project:
        """Create a new Gretel Project.

        For more documentation please reference

            https://python.docs.gretel.ai/en/stable/projects/projects.html#gretel_client.projects.projects.create_project
        """
        return create_project(*args, **kwargs)
