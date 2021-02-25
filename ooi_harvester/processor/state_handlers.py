import os
import datetime
import yaml
import textwrap

from prefect.engine import state

from github import Github

from ..config import (
    PROCESS_STATUS_PATH_STR,
    PROCESS_COMMIT_MESSAGE_TEMPLATE,
    STATUS_EMOJIS,
    GH_MAIN_BRANCH,
    GH_DATA_ORG,
    GH_PAT,
)

from ..utils.parser import parse_exception


def get_issue(flow_name, task_name, exc_dict, now):
    issue_title = f"🛑 Processing failed: {exc_dict['type']}"
    issue_body_template = textwrap.dedent(
        """\
    ## Overview

    `{exc_type}` found in `{task_name}` task during run ended on {now}.

    ## Details

    Flow name: `{flow_name}`
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
        exc_value=exc_dict['value'],
        exc_traceback=exc_dict['traceback'],
    )
    return {'title': issue_title, 'body': issue_body}


def process_status_update(flow, old_state, new_state):
    _ = old_state
    gh = Github(GH_PAT)
    flow_name = flow.name
    repo = gh.get_repo(os.path.join(GH_DATA_ORG, flow_name))
    contents = repo.get_contents(PROCESS_STATUS_PATH_STR, ref=GH_MAIN_BRANCH)
    status_json = yaml.load(contents.decoded_content, Loader=yaml.SafeLoader)
    now = datetime.datetime.utcnow().isoformat()
    if new_state.is_failed():
        status_json["status"] = "failed"
        status_json["last_updated"] = now
    elif new_state.is_successful():
        status_json["status"] = "success"
        status_json["last_updated"] = now

    if new_state.is_finished():
        if isinstance(new_state, state.Failed):
            for task, signal in new_state.result.items():
                task_name = task.name
                exc_dict = parse_exception(signal.result)
                issue = get_issue(flow_name, task_name, exc_dict, now)
                # TODO: Read assignee(s) from config
                repo.create_issue(
                    title=issue['title'],
                    body=issue['body'],
                    assignee='lsetiawan',
                    labels=['process'],
                )
        commit_message = PROCESS_COMMIT_MESSAGE_TEMPLATE(
            status_emoji=STATUS_EMOJIS[status_json["status"]],
            status=status_json["status"],
            request_dt=now,
        )
        repo.update_file(
            path=contents.path,
            message=commit_message,
            content=yaml.dump(status_json),
            sha=contents.sha,
            branch=GH_MAIN_BRANCH,
        )

    return new_state
