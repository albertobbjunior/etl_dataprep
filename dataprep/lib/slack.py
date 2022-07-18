import os
from typing import Optional, Tuple

import requests

SLACK_API_URL = "https://slack.com/api/chat.postMessage"
SLACK_TOKEN =os.environ.get("SLACK_TOKEN")
DEFAULT_SLACK_CHANNEL = "#team-predictions-etl-process"


def send_slack_message(
    script_name: str,
    success: bool,
    error_info: Optional[str] = None,
    channel: str = DEFAULT_SLACK_CHANNEL,
):
    """
    Sends a slack message to the team monitoring channel

    :param script_name: Name of the script
    :param success: bool indicating whether the script has run successfully
    :param error_info: str with raised error
    :param channel: Slack channel
    """
    if os.environ['USER'] == 'blg_ds':
        if success:
            header, body = compose_success_msg(script_name)
        else:
            header, body = compose_failed_msg(script_name, error_info=error_info)

        msg_blocks = compose_message(header, body)
        json_msg = construct_json_msg(msg_blocks, channel)
        headers = get_headers()
        print(headers)
        response = requests.post(SLACK_TOKEN, json=json_msg, headers=headers)
    
        return response
       


def compose_message(header: str, body: str) -> str:
    """
    Composes body of the message to be sent to the slack channel

    :param header: str with the header
    :param body: str with the body
    """
    msg_blocks = """[
    {{
        "type": "section",
        "text": {{
            "type": "mrkdwn",
            "text": "{}"
        }}
    }},
    {{
        "type": "context",
        "elements": [
            {{
                "type": "plain_text",
                "text": "{}"
            }}
        ]
    }}
]""".format(
        header, body
    )

    return msg_blocks


def compose_success_msg(script_name: str) -> Tuple[str, str]:
    """
    Composes body of the message to be sent to the slack channel when the script runs successfully

    :param script_name: Name of the script
    """
    return f":white_check_mark: :partyparrot:   Script was run successfully: *{script_name}*", "YEAH"


def compose_failed_msg(script_name: str, error_info: str) -> Tuple[str, str]:
    """
    Composes body of the message to be sent to the slack channel when the script fails

    :param script_name: Name of the script
    :param error_info: str with raised error
    """
    header = f":x: :sadparrot:   @channel There was a fatal error while running script: *{script_name}*."
    body = f"Error: {error_info}"
    return header, body


def construct_json_msg(blocks: str, channel: str) -> dict:
    """
    Constructs json message

    :param blocks: blocks
    :param channel: slack channel
    """
    return {"channel": channel, "blocks": blocks}


def get_headers() -> dict:
    """
    Fetches header

    :return: Dict with header
    """
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": "Bearer {}".format(SLACK_TOKEN),
    }
