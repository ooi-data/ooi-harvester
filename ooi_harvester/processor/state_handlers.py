import datetime
import yaml

from ..config import (
    PROCESS_STATUS_PATH_STR,
    PROCESS_COMMIT_MESSAGE_TEMPLATE,
    STATUS_EMOJIS,
    GH_MAIN_BRANCH,
)
from ..utils.github import get_repo


def process_status_update(flow, old_state, new_state):
    _ = old_state
    repo = get_repo(flow.name)
    contents = repo.get_contents(PROCESS_STATUS_PATH_STR, ref=GH_MAIN_BRANCH)
    status_json = yaml.load(contents.decoded_content, Loader=yaml.SafeLoader)
    now = datetime.datetime.utcnow().isoformat()
    if new_state.is_failed():
        # TODO: Also create issue when failed!
        status_json["status"] = "failed"
        status_json["last_updated"] = now
    elif new_state.is_successful():
        status_json["status"] = "success"
        status_json["last_updated"] = now

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
