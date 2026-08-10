"""
One-time setup script: creates the Databricks secret scopes and stores the
API keys needed by the AI Job Hunting Copilot capstone. Run this locally
(with the Databricks CLI configured) or from a notebook - never commit
the resulting secret values anywhere.

Usage:
    python setup_secrets.py
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

w = WorkspaceClient()

# w.secrets.create_scope(scope="database")
# w.secrets.put_secret(
#     scope="database",
#     key="lakebase-url",
#     string_value=getpass.getpass("Paste your lakebase url")
# )

# w.secrets.create_scope(scope="jobs")
w.secrets.put_secret(
    scope="jobs",
    key="adzuna-app-id",
    string_value=getpass.getpass("Paste your Adzuna App ID: ")
)

w.secrets.put_secret(
    scope="jobs",
    key="adzuna-app-key",
    string_value=getpass.getpass("Paste your Adzuna App Key: ")
)

w.secrets.put_secret(
    scope="jobs",
    key="usajobs-api-key",
    string_value=getpass.getpass("Paste your USAJobs API Key: ")
)

w.secrets.put_secret(
    scope="jobs",
    key="usajobs-user-agent",
    string_value=getpass.getpass("Paste your USAJobs registered email (User-Agent): ")
)

w.secrets.put_acl(
    scope="database",
    principal="users",
    permission=workspace.AclPermission.READ,
)

w.secrets.put_acl(
    scope="jobs",
    principal="users",
    permission=workspace.AclPermission.READ,
)

