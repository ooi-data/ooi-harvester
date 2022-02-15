import os
from typing import Union, List, Optional, cast
from toolz import curry
import datetime
import textwrap
import prefect
from prefect import Flow, Task  # noqa
from github import Github
from ooi_harvester.utils.parser import parse_exception
from ooi_harvester.settings.main import harvest_settings

TrackedObjectType = Union["Flow", "Task"]


def get_issue(flow_name, flow_run_id, task_name, exc_dict, now):
    issue_title = f"🛑 Processing failed: {exc_dict['type']}"
    issue_body_template = textwrap.dedent(
        """\
    ## Overview

    `{exc_type}` found in `{task_name}` task during run ended on {now}.

    ## Details

    Flow name: `{flow_name}`
    Flow run: `{flow_run_id}`
    Task name: `{task_name}`
    Error type: `{exc_type}`
    Error message: {exc_value}


    <details>
    <summary>Traceback</summary>

    ```
    {exc_traceback}
    ```

    </details>
    """
    ).format
    issue_body = issue_body_template(
        exc_type=exc_dict['type'],
        task_name=task_name,
        now=now,
        flow_name=flow_name,
        flow_run_id=flow_run_id,
        exc_value=exc_dict['value'],
        exc_traceback=exc_dict['traceback'],
    )
    return {'title': issue_title, 'body': issue_body}


def github_task_issue_formatter(
    task_obj: Task,
    state: "prefect.engine.state.State",
    now: datetime.datetime,
) -> dict:
    result = state.result
    flow_run_id = prefect.context.get("flow_run_id")
    flow_name = prefect.context.get("flow_name")
    task_name = task_obj.name
    if isinstance(state.result, Exception):
        exc_dict = parse_exception(result)
        issue = get_issue(flow_name, flow_run_id, task_name, exc_dict, now)
    else:
        raise TypeError(
            f"Invalid result type of {type(result)}, must be an Exception."
        )

    return issue


@curry
def github_issue_notifier(
    task_obj: Task,
    old_state: "prefect.engine.state.State",
    new_state: "prefect.engine.state.State",
    gh_org: str,
    gh_repo: Optional[str] = None,
    gh_pat: Optional[str] = None,
    assignees: List[str] = [],
    labels: List[str] = [],
) -> "prefect.engine.state.State":
    """
    Github issue state handler for failed task
    """
    GH_PAT = harvest_settings.github.pat or gh_pat
    flow_run_id = gh_repo or prefect.context.get("flow_run_id")
    if new_state.is_failed():
        now = datetime.datetime.utcnow().isoformat()

        issue = github_task_issue_formatter(task_obj, new_state, now)
        issue.setdefault("assignees", assignees)
        issue.setdefault("labels", labels)

        gh = Github(GH_PAT)
        repo = gh.get_repo(os.path.join(gh_org, flow_run_id))
        repo.create_issue(**issue)
    return new_state
