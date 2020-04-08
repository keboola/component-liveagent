import logging
import re
import sys
from urllib.parse import urljoin
from typing import Dict, List
from kbc.client_base import HttpClientBase
from liveagent.utils import Parameters

LADESK_URL_REGEXP = r'[\w\.]*ladesk.com[/(api)(v3)]*'
LADESK_URL = 'https://{}.ladesk.com/api/v3/'

PAGE_LIMIT = 500
DATE_FILTER_FIELD_CALLS = 'dateCreated'
DATE_FILTER_FIELD_CHATS = 'date_created'
DATE_FILTER_FIELD_COMPS = 'datechanged'
DATE_FILTER_FIELD_CONTS = 'datechanged'
DATE_FILTER_FIELD_TCKTS = 'date_changed'
DATE_FILTER_FIELD_MESGS = 'datecreated'


class LiveAgentClient(HttpClientBase):

    def __init__(self, token: str, organization: str, date_from: str, date_until: str) -> None:

        self.parameters = Parameters()
        self.parameters.token = token
        self.parameters.organization = organization
        self.parameters.date_from = date_from
        self.parameters.date_until = date_until

        self.checkOrganization()
        super().__init__(base_url=self.parameters.url, default_http_header={
            'apikey': self.parameters.token,
            'accept': 'application/json',
            'content-type': 'application/json'
        })

    def checkOrganization(self):

        url_match = re.match(LADESK_URL_REGEXP, self.parameters.organization, flags=re.I)

        if url_match is not None:
            logging.error(''.join(["Organization was not provided in the correct format. Please, provide only ",
                                   "organization name, without \"ladesk.com\".\n",
                                   f"Given: {self.parameters.organization}."]))
            sys.exit(1)

        else:
            self.parameters.url = LADESK_URL.format(str(self.parameters.organization))
            logging.debug(f"Organization URL: {self.parameters.url}.")

    def getAgents(self) -> List:

        return self._getPagedRequest('agents')

    def getCalls(self) -> List:

        par_calls = {
            '_filters': self._createFilterExpression(DATE_FILTER_FIELD_CALLS)
        }

        return self._getPagedRequest('calls', parameters=par_calls, method='cursor')

    def getChats(self) -> List:

        par_chats = {
            '_filters': self._createFilterExpression(DATE_FILTER_FIELD_CHATS)
        }

        return self._getPagedRequest('chats', parameters=par_chats)

    def getCompanies(self) -> List:

        par_companies = {
            '_filters': self._createFilterExpression(DATE_FILTER_FIELD_COMPS)
        }

        return self._getPagedRequest('companies', parameters=par_companies)

    def getContacts(self) -> List:

        par_contacts = {
            '_filters': self._createFilterExpression(DATE_FILTER_FIELD_CONTS)
        }

        return self._getPagedRequest('contacts', parameters=par_contacts)

    def getDepartments(self) -> List:

        return self._getPagedRequest('departments')

    def getTags(self) -> List:

        return self._getPagedRequest('tags')

    def getTickets(self) -> List:

        par_tickets = {
            '_filters': self._createFilterExpression(DATE_FILTER_FIELD_TCKTS)
        }

        return self._getPagedRequest('tickets', parameters=par_tickets)

    def getTicketMessages(self, ticket_id: str) -> List:

        par_messages = {
            '_filters': self._createFilterExpression(DATE_FILTER_FIELD_MESGS)
        }

        return self._getPagedRequest(f'tickets/{ticket_id}/messages', parameters=par_messages)

    def _createFilterExpression(self, filter_field):

        _expr = f"[[\"{filter_field}\",\">=\",\"{self.parameters.date_from}\"]," + \
            f"[\"{filter_field}\",\"<=\",\"{self.parameters.date_until}\"]]"

        # logging.debug(f"Expression: {_expr}.")

        return _expr

    def _getPagedRequest(self, endpoint: str, parameters: Dict = None,
                         result_key: str = None, method: str = 'page') -> List:

        url_endpoint = urljoin(self.base_url, endpoint)

        if method == 'page':

            if parameters is None:
                parameters = {}
            par_endpoint = {**parameters, **{'_perPage': PAGE_LIMIT}}

            results = []
            results_complete = False
            _page = 0

            while results_complete is False:

                _page += 1
                par_page = {**par_endpoint, **{'_page': _page}}

                rsp_page = self.get_raw(url=url_endpoint, params=par_page)

                if rsp_page.status_code == 200:

                    if result_key is None:
                        res_page = rsp_page.json()

                    else:
                        try:
                            res_page = rsp_page.json()[result_key]

                        except KeyError:
                            logging.exception(f"Key {result_key} not found in response.")
                            sys.exit(1)

                    results += res_page

                    if len(res_page) < PAGE_LIMIT:
                        results_complete = True
                        return results

                else:
                    logging.error(''.join([f"Could not download paginated data for endpoint {endpoint}.\n",
                                           f"Received: {rsp_page.status_code} - {rsp_page.text}."]))
                    sys.exit(1)

        elif method == 'cursor':

            results = []
            results_complete = False
            _cursor = None

            while results_complete is False:

                par_page = {**parameters, **{'_cursor': _cursor, '_perPage': PAGE_LIMIT}}
                rsp_page = self.get_raw(url=url_endpoint, params=par_page)

                if rsp_page.status_code == 200:

                    if result_key is None:
                        res_page = rsp_page.json()

                    else:
                        try:
                            res_page = rsp_page.json()[result_key]

                        except KeyError:
                            logging.exception(f"Key {result_key} not found in response.")
                            sys.exit(1)

                    results += res_page

                    _cursor = rsp_page.headers.get('next_page_cursor', None)
                    if _cursor is None:
                        results_complete = True
                        return results

                    else:
                        continue

                else:
                    logging.error(''.join([f"Could not download paginated data for endpoint {endpoint}.\n",
                                           f"Received: {rsp_page.status_code} - {rsp_page.text}."]))
                    sys.exit(1)

        else:
            logging.error(f"Unsupported pagination method {method}.")
            sys.exit(1)
