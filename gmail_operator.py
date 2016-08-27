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

import oauth2client
import mimetypes
import logging
import httplib2
import base64
import os

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from email.mime.text import MIMEText
from apiclient import errors
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

class GmailAPIOperator(BaseOperator):
    """
    Base Gmail Operator.
    All derived Gmail operators reference from Googles GMail API's official REST API documentation
    """
    @apply_defaults
    def __init__(self,
                 credentials_file = 'credentials.json',
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
                    cl = getattr(cl, method)()
            return cl
        self.get_credentials()
        self.service = discovery.build(self.api,
                                       self.api_version,
                                       http=self.credentials.authorize(
                                           httplib2.Http()
                                       )
                                       )
        self.prepare_request()
        try:
            request = call_methods(self.service, self.methods)
            response = request.execute()
            logging.info(response)
        except errors.HttpError, error:
            raise AirflowException('Gmail API call failed: %s', error)
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
    """
    template_fields = ('to','subject', 'message')
    template_ext = ('.html',)
    ui_color = '#e6faf9'

    @apply_defaults
    def __init__(self, to, sender, subject, message = None, html_content = None, attachment=None, scope='https://www.googleapis.com/auth/gmail.compose', *args, **kwargs):
        super(GmailAPISendMailOperator, self).__init__(*args, **kwargs)
        self.to = to
        self.sender = sender
        self.subject = subject
        self.message = message
        self.html_content = html_content
        self.scope = scope
        self.attachment = attachment

    def prepare_request(self):
        """Create a message for an email.
         """
        def attach_file(file, message):
            """
                Attachment upload
             """
            content_type, encoding = mimetypes.guess_type(file)
            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'
            main_type, sub_type = content_type.split('/', 1)
            if main_type == 'text':
                fp = open(file, 'rb')
                msg = MIMEText(fp.read(), _subtype=sub_type)
                fp.close()
            elif main_type == 'image':
                fp = open(file, 'rb')
                msg = MIMEImage(fp.read(), _subtype=sub_type)
                fp.close()
            elif main_type == 'audio':
                fp = open(file, 'rb')
                msg = MIMEAudio(fp.read(), _subtype=sub_type)
                fp.close()
            else:
                fp = open(file, 'rb')
                msg = MIMEBase(main_type, sub_type)
                msg.set_payload(fp.read())
                fp.close()
            filename = os.path.basename(file)
            msg.add_header('Content-Disposition', 'attachment', filename=filename)
            message.attach(msg)
        message = MIMEMultipart()
        if self.html_content:
            html_msg = MIMEText(self.html_content, 'html')
            message.attach(html_msg)
        else:
            plain_msg = MIMEText(self.message, 'plain')
            message.attach(plain_msg)
        if self.attachment:
            if isinstance(self.attachment, list):
                for file in self.attachment:
                    attach_file(file, message)
            else:
                attach_file(self.attachment, message)
        if isinstance(self.to, list):
            self.to = ", ".join(self.to)
        message['to'] = self.to
        message['from'] = self.sender
        message['subject'] = self.subject
        self.request = {'raw': base64.urlsafe_b64encode(message.as_string())}
        self.methods = [
            'users',
            'messages',
            ('send',{
                    'userId' : 'me',
                    'body': self.request
                 })
        ]