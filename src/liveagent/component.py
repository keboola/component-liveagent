import dateparser
import logging
import sys
from kbc.env_handler import KBCEnvHandler
from liveagent.utils import Parameters
from liveagent.client import LiveAgentClient
from liveagent.result import LiveAgentWriter

# configuration variables
KEY_API_TOKEN = '#token'
KEY_ORGANIZATION = 'organization'
KEY_OBJECTS = 'objects'
KEY_DATE = 'date'
KEY_DATE_FROM = 'from'
KEY_DATE_UNTIL = 'until'
KEY_INCREMENTAL = 'incremental_load'

MANDATORY_PARS = [KEY_API_TOKEN, KEY_ORGANIZATION, KEY_OBJECTS]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'
SUPPORTED_ENDPOINTS = ["agents", "calls", "departments", "tags", "tickets"]


class Component(KBCEnvHandler):

    def __init__(self, debug=False):

        super().__init__(mandatory_params=MANDATORY_PARS, log_level='DEBUG')
        logging.info(f'Running version {APP_VERSION}.')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

        self.parameters = Parameters()
        self.parameters.token = self.cfg_params[KEY_API_TOKEN]
        self.parameters.objects = self.cfg_params[KEY_OBJECTS]
        self.parameters.organization = self.cfg_params[KEY_ORGANIZATION]
        self.parameters.date_object = self.cfg_params.get(KEY_DATE, {})
        self.parameters.incremental = self.cfg_params.get(bool(KEY_INCREMENTAL), True)

        self.checkObjects()
        self.parseDates()

        self.client = LiveAgentClient(self.parameters.token, self.parameters.organization,
                                      self.parameters.date_from, self.parameters.date_until)

    def parseDates(self):

        date_from = self.parameters.date_object.get(KEY_DATE_FROM, '30 days ago')
        date_until = self.parameters.date_object.get(KEY_DATE_UNTIL, 'now')

        date_from_parsed = dateparser.parse(date_from)
        date_until_parsed = dateparser.parse(date_until)

        if any([date_from_parsed is None, date_until_parsed is None]):
            logging.error("Date values could not be parsed. Please, refer to documentation for correct specification.")
            sys.exit(1)

        else:
            self.parameters.date_from = date_from_parsed.strftime('%Y-%m-%d %H:%M:%S')
            self.parameters.date_until = date_until_parsed.strftime('%Y-%m-%d %H:%M:%S')

            logging.debug(f"Date from: {self.parameters.date_from}.")
            logging.debug(f"Date until: {self.parameters.date_until}.")

    def checkObjects(self):

        if self.parameters.objects == []:
            logging.error("No objects to download were specified.")
            sys.exit(1)

        _unsupported = []
        for obj in self.parameters.objects:

            if obj not in SUPPORTED_ENDPOINTS:
                _unsupported += [obj]

        if len(_unsupported) > 0:
            logging.error(f"Unsupported endpoints specified: {_unsupported}. Must be one of {SUPPORTED_ENDPOINTS}.")
            sys.exit(1)

    def run(self):

        _objects = self.parameters.objects
        _incremental = self.parameters.incremental

        logging.info(f"Downloading data from {self.parameters.date_from} to {self.parameters.date_until}.")

        for obj in _objects:

            if obj not in ['tickets_messages', 'tickets']:

                logging.info(f"Downloading data about {obj}.")

                _writer = LiveAgentWriter(self.tables_out_path, obj, _incremental)
                _api_results = eval(f'self.client.get{obj.capitalize()}()')
                _writer.writerows(_api_results)

        if 'tickets' in _objects or 'tickets_messages' in _objects:

            logging.info("Download data about tickets.")

            _writer_tickets = LiveAgentWriter(self.tables_out_path, 'tickets', _incremental)
            _api_results = self.client.getTickets()
            _writer_tickets.writerows(_api_results)

            if 'tickets_messages' in _objects:

                ticket_ids = [t['id'] for t in _api_results]

                _writer_messages = LiveAgentWriter(self.tables_out_path, 'tickets-messages', _incremental)
                _writer_content = LiveAgentWriter(self.tables_out_path, 'tickets-messages-content',
                                                  _incremental)

                _out_messages = []
                _out_contents = []

                for tid in ticket_ids:
                    _messages = self.client.getTicketMessages(tid)

                    for msg in _messages:
                        msg['ticket_id'] = tid
                        msg_id = msg['id']

                        _out_messages = [msg]

                        for cont in msg['messages']:
                            cont['message_id'] = msg_id
                            _out_contents += [cont]

                _writer_messages.writerows(_out_messages)
                _writer_content.writerows(_out_contents)
