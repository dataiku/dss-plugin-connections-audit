from dataiku.runnables import Runnable
import dataiku
from dataiku import Dataset
import os, re
from dataiku.core import intercom

def get_connection(connection_name, connections):
    connection = connections.get(connection_name, None)
    if connection is None:
        connection = {'datasets':[], 'notebooks':[], 'no':len(connections)}
        connections[connection_name] = connection
    return connection

class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()

        if self.config.get('allProjects', False):
            self.project_keys = self.client.list_project_keys()
        else:
            self.project_keys = [project_key]

    def get_progress_target(self):
        return (len(self.project_keys), 'NONE')

    def run(self, progress_callback):
        clobber = self.config.get("clobber", False)
        prefix = self.config.get("prefix")

        connections = set()

        done = 0
        for project_key in self.project_keys:
            project = self.client.get_project(project_key)

            for dataset_name in Dataset.list(project_key=project_key):
                d = project.get_dataset(dataset_name)
                connection_name = d.get_definition().get('params', {}).get('connection', None)
                if connection_name is not None:
                    connections.add(connection_name)

            sql_notebooks = intercom.backend_json_call("sql-notebooks/list/", data={"projectKey": project_key})
            for sql_notebook in sql_notebooks:
                connection_name = sql_notebook.get('connection', None)
                if connection_name is not None:
                    m = re.search('@virtual\(([^\)]+)\):(.*)', connection_name)
                    if m is not None:
                        connection_name = 'hive-%s' % m.group(2)

                    connections.add(connection_name)

            meta = project.get_metadata()

            # Update tags list
            if clobber:
                tags = [x for x in meta["tags"] if not x.startswith(prefix)]
            else:
                tags = meta["tags"]
            tags.extend(["%s%s" % (prefix, connection) for connection in list(connections)])

            meta["tags"] = tags
            project.set_metadata(meta)

            done += 1
            progress_callback(done)