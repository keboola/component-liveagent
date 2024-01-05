import dateparser
import logging
from kbc.env_handler import KBCEnvHandler
from liveagent.utils import Parameters
from liveagent.client import LiveAgentClient, ClientException
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
KEY_FAIL_ON_ERROR = 'fail_on_error'

MANDATORY_PARS = [KEY_API_TOKEN, KEY_ORGANIZATION, KEY_OBJECTS]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.2.1'
SUPPORTED_ENDPOINTS = ["agents", "calls", "companies", "contacts", "departments", "tags", "tickets", "tickets_messages",
                       "tickets_history"]
SUPPORTED_ENDPOINTS_V1 = ["agent_report", "agent_availability", "conversations", "agent_availability_chats",
                          "calls_availability", "ranking_agents_report"]


class UserException(Exception):
    pass


class Component(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARS, log_level='INFO')
        logging.info(f'Running version {APP_VERSION}.')

        if self.cfg_params.get(KEY_DEBUG) is True:
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            raise UserException(e)

        self.parameters = Parameters()
        self.parameters.token = self.cfg_params[KEY_API_TOKEN]
        self.parameters.token_v1 = self.cfg_params.get(KEY_API_TOKEN_V1, None)
        self.parameters.objects = self.cfg_params[KEY_OBJECTS]
        self.parameters.organization = self.cfg_params[KEY_ORGANIZATION]
        self.parameters.date_object = self.cfg_params.get(KEY_DATE, {})
        self.parameters.incremental = self.cfg_params.get(bool(KEY_INCREMENTAL), True)
        self.parameters.fail_on_error = self.cfg_params.get(KEY_FAIL_ON_ERROR, False)

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
            raise UserException(
                "Date values could not be parsed. Please, refer to documentation for correct specification.")

        else:
            self.parameters.date_from = date_from_parsed.strftime('%Y-%m-%d %H:%M:%S')
            self.parameters.date_until = date_until_parsed.strftime('%Y-%m-%d %H:%M:%S')

            # self.parameters.date_from_iso = date_from_parsed.strftime('%Y-%m-%d')
            # self.parameters.date_to_iso = date_until_parsed.strftime('%Y-%m-%d')

            self.parameters.date_chunks = self.split_dates_to_chunks(
                date_from_parsed, date_until_parsed, 0, '%Y-%m-%d')

            logging.debug(f"Date from: {self.parameters.date_from}.")
            logging.debug(f"Date until: {self.parameters.date_until}.")

    def check_objects(self):

        if not self.parameters.objects:
            raise UserException("No objects to download were specified.")

        _unsupported = []
        for obj in self.parameters.objects:

            if obj not in SUPPORTED_ENDPOINTS and obj not in SUPPORTED_ENDPOINTS_V1:
                _unsupported += [obj]

            if obj in SUPPORTED_ENDPOINTS_V1:
                if self.parameters.token_v1 is None or self.parameters.token_v1 == '':
                    raise UserException(f"Missing API V1 token for API V1 endpoint {obj}.")

        if len(_unsupported) > 0:
            raise UserException(
                f"Unsupported endpoints specified: {_unsupported}. Must be one of {SUPPORTED_ENDPOINTS}.")

    def run(self):

        _objects = self.parameters.objects
        _incremental = self.parameters.incremental
        _fail_on_error = self.parameters.fail_on_error

        logging.info(f"Downloading data from {self.parameters.date_from} to {self.parameters.date_until}.")

        for obj in _objects:

            logging.info(f"Downloading {obj} data.")

            _writer = LiveAgentWriter(self.tables_out_path, obj, _incremental, _fail_on_error)

            if obj not in ['tickets_messages', 'tickets', *SUPPORTED_ENDPOINTS_V1]:

                try:
                    _api_results = eval(f'self.client.get_{obj}()')
                except ClientException as c_ex:
                    raise UserException(c_ex) from c_ex
                _writer.writerows(_api_results)

            elif obj == 'agent_availability':

                try:
                    _api_results = self.client.get_agent_availability_tickets(self.parameters.date_from,
                                                                              self.parameters.date_until)
                except ClientException as c_ex:
                    raise UserException(c_ex) from c_ex

                _writer.writerows(_api_results)

            elif obj == 'agent_availability_chats':

                try:
                    _api_results = self.client.get_agent_availability_chats(self.parameters.date_from,
                                                                            self.parameters.date_until)
                except ClientException as c_ex:
                    raise UserException(c_ex) from c_ex

                _writer.writerows(_api_results)

            elif obj == 'calls_availability':

                try:
                    _api_results = self.client.get_calls_availability(self.parameters.date_from,
                                                                      self.parameters.date_until)
                except ClientException as c_ex:
                    raise UserException(c_ex) from c_ex

                _writer.writerows(_api_results)

            elif obj == 'conversations':

                try:
                    _api_results = self.client.get_conversations(self.parameters.date_from)
                except ClientException as c_ex:
                    raise UserException(c_ex) from c_ex
                _writer.writerows(_api_results)

            elif obj == 'agent_report':
                for dt in self.parameters.date_chunks:
                    date = dt['start_date']
                    start = date + ' 00:00:00'
                    end = date + ' 23:59:59'
                    try:
                        _api_results = self.client.get_agent_report(date_from=start, date_to=end)
                    except ClientException as c_ex:
                        raise UserException(c_ex) from c_ex
                    _writer.writerows(_api_results, parentDict={'date': date})

            elif obj == 'ranking_agents_report':
                for dt in self.parameters.date_chunks:
                    date = dt['start_date']
                    start = date + ' 00:00:00'
                    end = date + ' 23:59:59'
                    try:
                        _api_results = self.client.get_ranking_agents_report(date_from=start, date_to=end)
                    except ClientException as c_ex:
                        raise UserException(c_ex) from c_ex
                    _writer.writerows(_api_results, parentDict={'date': date})

            elif obj in ['tickets_messages', 'tickets']:
                pass

            else:
                raise UserException(f"Unknown object {obj}.")

        if 'tickets' in _objects or 'tickets_messages' in _objects:

            logging.info("Downloading ticket data.")

            _writer_tickets = LiveAgentWriter(self.tables_out_path, 'tickets', _incremental)
            try:
                _api_results = self.client.get_tickets()
            except ClientException as c_ex:
                raise UserException(c_ex) from c_ex
            _writer_tickets.writerows(_api_results)

            if 'tickets_messages' in _objects:

                ticket_ids = [t['id'] for t in _api_results]

                logging.info(f"The component will process messages for {len(ticket_ids)} tickets.")

                _writer_messages = LiveAgentWriter(self.tables_out_path, 'tickets_messages', _incremental)
                _writer_content = LiveAgentWriter(self.tables_out_path, 'tickets_messages_content',
                                                  _incremental)

                _out_messages = []
                _out_contents = []

                for tid in ticket_ids:
                    try:
                        _messages = self.client.get_ticket_messages(tid)
                    except ClientException as c_ex:
                        raise UserException(c_ex) from c_ex

                    for msg in _messages:
                        msg['ticket_id'] = tid
                        msg_id = msg['id']

                        _out_messages += [msg]

                        for cont in msg['messages']:
                            cont['message_id'] = msg_id
                            _out_contents += [cont]

                _writer_messages.writerows(_out_messages)
                _writer_content.writerows(_out_contents)
