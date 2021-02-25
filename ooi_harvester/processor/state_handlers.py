import os
import datetime
import yaml
import textwrap

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


def get_issue(flow_name, task_name, exc_dict):
    issue_title = f"🛑 Processing failed: {exc_dict['type']}"
    issue_body = textwrap.dedent(
        f"""\
    ## Overview

    `{exc_dict['type']}` found in `{task_name}` task during run ended on {now}.

    ## Details

    Flow name: `{flow_name}`
    Task name: `{task_name}`
    Error type: `{exc_dict['type']}`
    Error message: {exc_dict['value']}


    <details>
    <summary>Traceback</summary>

    ```
    {exc_dict['traceback']}
    ```

    </details>
    """
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
        for task, signal in new_state.result.items():
            task_name = task.name
            exc_dict = parse_exception(signal.result)
            issue = get_issue(flow_name, task_name, exc_dict)
            # TODO: Read assignee(s) from config
            repo.create_issue(
                title=issue['title'],
                body=issue['body'],
                assignee='lsetiawan',
                labels=['process'],
            )
    elif new_state.is_successful():
        status_json["status"] = "success"
        status_json["last_updated"] = now

    if status_json["status"] != "pending":
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
