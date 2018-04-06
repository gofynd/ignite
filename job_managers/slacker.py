import json
from slackclient import SlackClient
from settings import project_config

SLACK_TOKEN = project_config.get('SLACK_TOKEN')
slack_client = SlackClient(SLACK_TOKEN)


def send_slack(text=None, attachments=None):
    SLACK_CHANNEL = project_config.get('SLACK_CHANNEL')
    SLACK_USERNAME = project_config.get('SLACK_USERNAME')
    out = slack_client.api_call("chat.postMessage",
                                channel=SLACK_CHANNEL,
                                # text=text,
                                attachments=attachments,
                                username=SLACK_USERNAME)
    if out:
        return True
    return False


# Reference link : https://api.slack.com/docs/messages/builder
def prepare_attachment(title, job_name, extra_fileds, footer):
    fields = [
        {
            "title": "Job Name",
            "value": job_name,
            "short": False
        }
    ]

    fields = fields + extra_fileds

    attachment_message = json.dumps([
        {
            "color": "#36a64f",
            # "pretext": job_name + " - Job Completed",
            "title": title,
            "title_link": "xxx",
            "fields": fields,
            # "footer": 'Total time taken(seconds): ' + str(footer)
        }
    ])
    return attachment_message


def do_slack(title, job_name, extra_fileds, footer):
    if project_config.get("slack_enabled"):
        title = "JOB NAME - {} - Dataproc Job Status".format(job_name)
        slack_attachment = prepare_attachment(title,
                                              job_name,
                                              extra_fileds,
                                              footer)
        response = send_slack(attachments=slack_attachment)
        return response
