import dateparser
import logging
import sys
from kbc.env_handler import KBCEnvHandler
from liveagent.utils import Parameters
from liveagent.client import LiveAgentClient
from liveagent.result import LiveAgentWriter

# configuration variables
KEY_API_TOKEN = '#token'
KEY_API_TOKEN_V1 = '#token_v1'
KEY_ORGANIZATION = 'organization'
KEY_OBJECTS = 'objects'
KEY_DATE = 'date'
KEY_DATE_FROM = 'from'
KEY_DATE_UNTIL = 'until'
KEY_INCREMENTAL = 'incremental_load'
KEY_DEBUG = 'debug'

MANDATORY_PARS = [KEY_API_TOKEN, KEY_ORGANIZATION, KEY_OBJECTS]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.1.3'
SUPPORTED_ENDPOINTS = ["agents", "calls", "companies", "contacts", "departments", "tags", "tickets"]
SUPPORTED_ENDPOINTS_V1 = ["agent_report", "agent_availability", "conversations"]


class Component(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARS, log_level='INFO')
        sys.tracebacklimit = 0
        logging.info(f'Running version {APP_VERSION}.')

        if self.cfg_params.get(KEY_DEBUG) is True:
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            sys.tracebacklimit = 3

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

        self.parameters = Parameters()
        self.parameters.token = self.cfg_params[KEY_API_TOKEN]
        self.parameters.token_v1 = self.cfg_params.get(KEY_API_TOKEN_V1, None)
        self.parameters.objects = self.cfg_params[KEY_OBJECTS]
        self.parameters.organization = self.cfg_params[KEY_ORGANIZATION]
        self.parameters.date_object = self.cfg_params.get(KEY_DATE, {})
        self.parameters.incremental = self.cfg_params.get(bool(KEY_INCREMENTAL), True)

        self.check_objects()
        self.parse_dates()

        self.client = LiveAgentClient(self.parameters.token, self.parameters.token_v1, self.parameters.organization,
                                      self.parameters.date_from, self.parameters.date_until)

    def parse_dates(self):

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

            # self.parameters.date_from_iso = date_from_parsed.strftime('%Y-%m-%d')
            # self.parameters.date_to_iso = date_until_parsed.strftime('%Y-%m-%d')

            self.parameters.date_chunks = self.split_dates_to_chunks(date_from_parsed, date_until_parsed, 0, '%Y-%m-%d')

            logging.debug(f"Date from: {self.parameters.date_from}.")
            logging.debug(f"Date until: {self.parameters.date_until}.")

    def check_objects(self):

        if self.parameters.objects == []:
            logging.error("No objects to download were specified.")
            sys.exit(1)

        _unsupported = []
        for obj in self.parameters.objects:

            if obj not in SUPPORTED_ENDPOINTS and obj not in SUPPORTED_ENDPOINTS_V1:
                _unsupported += [obj]

            if obj in SUPPORTED_ENDPOINTS_V1:
                if self.parameters.token_v1 is None or self.parameters.token_v1 == '':
                    logging.error(f"Missing API V1 token for API V1 endpoint {obj}.")
                    sys.exit(1)

        if len(_unsupported) > 0:
            logging.error(f"Unsupported endpoints specified: {_unsupported}. Must be one of {SUPPORTED_ENDPOINTS}.")
            sys.exit(1)

    def run(self):

        _objects = self.parameters.objects
        _incremental = self.parameters.incremental

        logging.info(f"Downloading data from {self.parameters.date_from} to {self.parameters.date_until}.")

        for obj in _objects:

            logging.info(f"Downloading data about {obj}.")

            _writer = LiveAgentWriter(self.tables_out_path, obj, _incremental)

            if obj not in ['tickets_messages', 'tickets', *SUPPORTED_ENDPOINTS_V1]:

                _api_results = eval(f'self.client.get_{obj}()')
                _writer.writerows(_api_results)

            elif obj == 'agent_availability':

                _api_results = self.client.get_agent_availability(self.parameters.date_from, self.parameters.date_until)
                _writer.writerows(_api_results)

            elif obj == 'conversations':

                _api_results = self.client.get_conversations(self.parameters.date_from, self.parameters.date_until)
                _writer.writerows(_api_results)

            elif obj == 'agent_report':

                for dt in self.parameters.date_chunks:
                    date = dt['start_date']
                    start = date + ' 00:00:00'
                    end = date + ' 23:59:59'
                    _api_results = self.client.get_agent_report(date_from=start, date_to=end)
                    _writer.writerows(_api_results, parentDict={'date': date})

            elif obj in ['tickets_messages', 'tickets']:
                pass

            else:
                logging.error(f"Unknown object {obj}.")
                sys.exit(1)

        if 'tickets' in _objects or 'tickets_messages' in _objects:

            logging.info("Download data about tickets.")

            _writer_tickets = LiveAgentWriter(self.tables_out_path, 'tickets', _incremental)
            _api_results = self.client.get_tickets()
            _writer_tickets.writerows(_api_results)

            if 'tickets_messages' in _objects:

                ticket_ids = [t['id'] for t in _api_results]

                _writer_messages = LiveAgentWriter(self.tables_out_path, 'tickets-messages', _incremental)
                _writer_content = LiveAgentWriter(self.tables_out_path, 'tickets-messages-content',
                                                  _incremental)

                _out_messages = []
                _out_contents = []

                for tid in ticket_ids:
                    _messages = self.client.get_ticket_messages(tid)

                    for msg in _messages:
                        msg['ticket_id'] = tid
                        msg_id = msg['id']

                        _out_messages = [msg]

                        for cont in msg['messages']:
                            cont['message_id'] = msg_id
                            _out_contents += [cont]

                _writer_messages.writerows(_out_messages)
                _writer_content.writerows(_out_contents)
