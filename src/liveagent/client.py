import logging
import re
import sys
from urllib.parse import urljoin
from typing import Dict, List
from kbc.client_base import HttpClientBase
from liveagent.utils import Parameters

LADESK_URL_REGEXP = r'[\w\.]*ladesk.com[/(api)(v3)]*'
LADESK_URL = 'https://{}.ladesk.com/api/'

PAGE_LIMIT = 500
DATE_FILTER_FIELD_CALLS = 'dateCreated'
DATE_FILTER_FIELD_CHATS = 'date_created'
DATE_FILTER_FIELD_COMPS = 'datechanged'
DATE_FILTER_FIELD_CONTS = 'datechanged'
DATE_FILTER_FIELD_TCKTS = 'date_changed'
DATE_FILTER_FIELD_MESGS = 'datecreated'


class LiveAgentClient(HttpClientBase):

    def __init__(self, token_v3: str, token_v1: str, organization: str, date_from: str, date_until: str):

        self.parameters = Parameters()
        self.parameters.token_v3 = token_v3
        self.parameters.token_v1 = token_v1
        self.parameters.organization = organization
        self.parameters.date_from = date_from
        self.parameters.date_until = date_until

        self.check_organization()
        super().__init__(base_url=self.parameters.url, default_http_header={
            'apikey': self.parameters.token_v3,
            'accept': 'application/json',
            'content-type': 'application/json'
        })

    def check_organization(self):

        url_match = re.match(LADESK_URL_REGEXP, self.parameters.organization, flags=re.I)

        if url_match is not None:
            logging.error(''.join(["Organization was not provided in the correct format. Please, provide only ",
                                   "organization name, without \"ladesk.com\".\n",
                                   f"Given: {self.parameters.organization}."]))
            sys.exit(1)

        else:
            self.parameters.url = LADESK_URL.format(str(self.parameters.organization))
            logging.debug(f"Organization URL: {self.parameters.url}.")

    def get_agents(self) -> List:

        return self._get_paged_request('v3/agents')

    def get_calls(self) -> List:

        par_calls = {
            '_filters': self._create_filter_expresssion(DATE_FILTER_FIELD_CALLS)
        }

        return self._get_paged_request('v3/calls', parameters=par_calls, method='cursor')

    def get_chats(self) -> List:

        par_chats = {
            '_filters': self._create_filter_expresssion(DATE_FILTER_FIELD_CHATS)
        }

        return self._get_paged_request('v3/chats', parameters=par_chats)

    def get_companies(self) -> List:

        par_companies = {
            '_filters': self._create_filter_expresssion(DATE_FILTER_FIELD_COMPS)
        }

        return self._get_paged_request('v3/companies', parameters=par_companies)

    def get_contacts(self) -> List:

        par_contacts = {
            '_filters': self._create_filter_expresssion(DATE_FILTER_FIELD_CONTS)
        }

        return self._get_paged_request('v3/contacts', parameters=par_contacts)

    def get_departments(self) -> List:

        return self._get_paged_request('v3/departments')

    def get_tags(self) -> List:

        return self._get_paged_request('v3/tags')

    def get_tickets(self) -> List:

        par_tickets = {
            '_filters': self._create_filter_expresssion(DATE_FILTER_FIELD_TCKTS)
        }

        return self._get_paged_request('v3/tickets', parameters=par_tickets)

    def get_ticket_messages(self, ticket_id: str) -> List:

        par_messages = {
            '_filters': self._create_filter_expresssion(DATE_FILTER_FIELD_MESGS)
        }

        return self._get_paged_request(f'v3/tickets/{ticket_id}/messages', parameters=par_messages)

    def get_agent_report(self, date_from: str, date_to: str) -> List:

        columns = 'id,contactid,firstname,lastname,worktime,answers,answers_ph,newAnswerAvgTime,' + \
            'newAnswerAvgTimeSla,nextAnswerAvgTime,nextAnswerAvgTimeSla,calls,calls_ph,missed_calls,' + \
            'missed_calls_ph,call_seconds,call_seconds_ph,chats,chats_ph,chat_answers,chat_answers_ph,' + \
            'missed_chats,missed_chats_ph,chat_pickup,chatPickupAvgTime,chatAvgTime,not_ranked,not_ranked_p,' + \
            'not_ranked_ph,rewards,rewards_p,rewards_ph,punishments,punishments_p,punishments_ph,created_tickets,' + \
            'resolved_tickets,u_chats,u_calls,notes,firstAssignAvgTime,firstAssignAvgTimeSla,firstResolveAvgTime,' + \
            'firstResolveAvgTimeSla,calls_outgoing,call_outgoing_seconds,call_outgoing_avg_time,' + \
            'call_pickup_avg_time,call_avg_time,calls_internal,call_internal_avg_time,call_internal_seconds,o_calls'

        par_agent_report = {
            'date_from': date_from,
            'date_to': date_to,
            'apikey': self.parameters.token_v1,
            'columns': columns
        }

        return self._get_paged_request('reports/agents', parameters=par_agent_report,
                                       method='limit', result_key='agents')

    def get_agent_availability(self, date_from: str, date_to: str) -> List:

        columns = 'id,userid,firstname,lastname,contactid,departmentid,department_name,hours_online,from_date,to_date'

        par_agent_availability = {
            'date_from': date_from,
            'date_to': date_to,
            'apikey': self.parameters.token_v1,
            'columns': columns
        }

        return self._get_paged_request('reports/tickets/agentsavailability', result_key='agentsavailability',
                                       parameters=par_agent_availability, method='limit')

    def get_conversations(self, date_from: str, date_to: str) -> List:

        par_conversations = {
            'datefrom': date_from,
            'dateto': date_to,
            'apikey': self.parameters.token_v1,
            'channel_type': 'E,B,M,I,C,W,F,A,T,Q,S'
        }

        return self._get_paged_request('conversations', result_key='conversations',
                                       parameters=par_conversations, method='limit',
                                       limit_param='limit', offset_param='offset')

    def _create_filter_expresssion(self, filter_field):

        _expr = f"[[\"{filter_field}\",\">=\",\"{self.parameters.date_from}\"]," + \
            f"[\"{filter_field}\",\"<=\",\"{self.parameters.date_until}\"]]"

        # logging.debug(f"Expression: {_expr}.")

        return _expr

    def _get_paged_request(self, endpoint: str, parameters: Dict = None,
                           result_key: str = None, method: str = 'page', limit_size: int = 1000,
                           limit_param: str = 'limitcount', offset_param: str = 'limitfrom') -> List:

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

        elif method == 'limit':

            results = []
            results_complete = False
            limit = limit_size
            offset = 0

            while results_complete is False:
                pass

                par_page = {**parameters, **{limit_param: limit, offset_param: offset}}
                rsp_page = self.get_raw(url=url_endpoint, params=par_page)

                if rsp_page.status_code == 200:

                    _js = rsp_page.json()
                    _res = _js['response'][result_key]

                    results += _res

                    if len(_res) < limit:
                        results_complete = True
                        return results

                    else:
                        offset += limit

                else:
                    logging.error(''.join([f"Could not download paginated data for endpoint {endpoint}.\n",
                                           f"Received: {rsp_page.status_code} - {rsp_page.text}."]))
                    sys.exit(1)

        else:
            logging.error(f"Unsupported pagination method {method}.")
            sys.exit(1)
