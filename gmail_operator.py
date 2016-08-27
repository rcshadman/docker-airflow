# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import logging
import httplib2
import json
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
import base64
import os
from email.mime.text import MIMEText
from apiclient import errors
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import mimetypes

class GmailAPIOperator(BaseOperator):
    """
    Base Gmail Operator.
    All derived Gmail operators reference from Googles GMail API's official REST API documentation
    at ----. Before using any GMail API operators you need
    to get an authentication token at ----
    In the future additional Gmail operators will be derived from this class as well.

    :param
    :type
    """
    @apply_defaults
    def __init__(self,
                 credentials_file,
                 api='gmail',
                 api_version="v1",
                 client_secret = 'client_secret.json',
                 app_name = "Airflow",
                 scope = 'https://www.googleapis.com/auth/gmail.readonly',
                 *args,
                 **kwargs):
        super(GmailAPIOperator, self).__init__(*args, **kwargs)
        self.request = None
        self.methods = None
        self.scope = scope
        self.client_secret = client_secret
        self.credentials_file = credentials_file
        self.app_name = app_name
        self.api_version = api_version
        self.api = api

    def prepare_request(self):
        """
        Used by the execute function. Set the discovery service for  Gmail's
        REST API call.

        Override in child class. Each GmailAPI child operator is responsible for having
        a prepare_request method call which sets self.method, self.url, and self.body.
        """
        pass
    def execute(self, context):
        def call_methods(cl, methods):
            for method in methods:
                if isinstance(method, tuple):
                    method, kwargs = method
                    cl = getattr(cl, method)(**kwargs)
                else:
                    cl = getattr(cl, method)
        return cl
        #TODO: Fix this shit
        """
        Execute service object
        :param table: The name of the source table
        :type table: str
        """
        self.get_credentials()
        self.service = discovery.build(self.api,
                                       self.api_version,
                                       http=self.credentials.authorize(
                                           httplib2.Http()
                                       )
                                       )
        self.prepare_request()
        try:
            logging.info(self.service)
            logging.info(self.methods)
            request = call_methods(self.service, self.methods)
            response = request.execute()
            logging.info(response)
        except errors.HttpError, error:
            logging.error('GMail API call failed: %s', error)
            raise AirflowException('GMail API call failed: %s', error)
    def get_credentials(self):
        """Gets valid user credentials from storage.

            If nothing has been stored, or if the stored credentials are invalid,
            the OAuth2 flow is completed to obtain the new credentials.

            Returns:
                Credentials, the obtained credential.
            """
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, self.credentials_file)

        store = oauth2client.file.Storage(credential_path)
        self.credentials = store.get()
        if not self.credentials or self.credentials.invalid:
            flow = client.flow_from_clientsecrets(self.client_secret, self.scope)
            flow.user_agent = self.app_name
            flags = tools.argparser.parse_args(args=['--noauth_local_webserver'])
            self.credentials = tools.run_flow(flow, store, flags)
            logging.info('Storing credentials to ' + credential_path)



class GmailAPISendMailOperator(GmailAPIOperator):
    """
    Send mail using GMail API
    More info:

    :param room_id: Room in which to send notification on HipChat
    :type room_id: str
    """
    template_fields = ('message', 'to')
    ui_color = '#2980b9'

    @apply_defaults
    def __init__(self, to, sender, subject, message, scope ='https://www.googleapis.com/auth/gmail.compose', *args, **kwargs):
        super(GmailAPISendMailOperator, self).__init__(*args, **kwargs)
        self.to = to
        self.sender = sender
        self.subject = subject
        self.message = message
        self.scope = scope
        self.methods = None

    def prepare_request(self):
        """Create a message for an email.

         Args:
           sender: Email address of the sender.
           to: Email address of the receiver.
           subject: The subject of the email message.
           message_text: The text of the email message.
         """
        message = MIMEText(self.message)
        message['to'] = self.to
        message['from'] = self.sender
        message['subject'] = self.subject
        self.request = {'raw': base64.urlsafe_b64encode(message.as_string())}
        #self.request = json.dumps(dict(
        #    (unicode(k).encode('utf-8'), unicode(v).encode('utf-8')) for k, v in self.request if v))
        #logging.info(self.request)
        self.methods = [
            'users',
            'messages',
            ('send',{
                    'userId' : 'me',
                    'body': self.request
                 })
        ]