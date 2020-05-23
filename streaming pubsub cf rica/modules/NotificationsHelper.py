from datetime import datetime
from json import dumps
import requests
import logging


class NotificationsHelper:

    def __init__(self, chat_url, event_id, project_id, file_name, error_bucket, error_folder, message):
        self.chat_url = chat_url
        self.event_id = event_id
        self.project_id = project_id
        self.file_name = file_name
        self.error_bucket = error_bucket
        self.message = message
        self.error_folder = error_folder
        self.current_time = datetime.now().strftime("%d-%b-%Y %H:%M:%S.%f")

    def notify_chatroom(self):
        body = {
            "cards": [
                {
                    "header": {
                        "title": "Google Cloud Function Failed",
                        "subtitle": "Event ID " + self.event_id,
                        "imageUrl": "https://img.icons8.com/flat_round/64/000000/error--v3.png"
                    },
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "keyValue": {
                                        "topLabel": "Project ID",
                                        "content": self.project_id
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Cloud Function",
                                        "content": "generic file loader"
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Error Bucket",
                                        "content": self.error_bucket
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Error Folder",
                                        "content": self.error_folder
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "File Name",
                                        "content": self.file_name
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Execution Date Time",
                                        "content": str(self.current_time)
                                    }
                                },

                                {
                                    "textParagraph": {
                                        "text": "<b>Message</b><br> <font color=\"#ff0000\">" + self.message +
                                                "</font></br>"
                                    }
                                }

                            ]
                        },

                    ]
                }
            ]
        }
        try:
            r = requests.post(url=self.chat_url, data=dumps(body), headers={'Content-Type': 'application/json'})
        except Exception as e:
            logging.error('Failed to send message' + e)
        finally:
            logging.info('Chatroom Notification')
