import os
import csv
import json

FIELDS_AGENTS = ['id', 'name', 'email', 'role', 'avatar_url', 'online_status', 'status', 'gender']
FIELDS_R_AGENTS = FIELDS_AGENTS
PK_AGENTS = ['id']
JSON_AGENTS = []

FIELDS_CALLS = ['id', 'ticketId', 'type', 'fromNumber', 'fromName', 'toNumber', 'toName', 'viaNumber', 'dateCreated',
                'dateAnswered', 'dateFinished', 'callDuration']
FIELDS_R_CALLS = ['id', 'ticket_id', 'type', 'from_number', 'from_name', 'to_number', 'to_name', 'via_number',
                  'date_created', 'date_answered', 'date_finished', 'call_duration']
PK_CALLS = ['id']
JSON_CALLS = []

FIELDS_CHATS = ['id', 'subject', 'preview', 'date_created', 'chat_order', 'status_date_started', 'tags', 'rstatus',
                'firstname', 'lastname', 'system_name', 'avatar_url', 'countrycode', 'city', 'departmentId', 'agentId',
                'status', 'emails']
FIELDS_R_CHATS = ['id', 'subject', 'preview', 'chat_order', 'status_date_started', 'tags', 'rstatus', 'first_name',
                  'last_name', 'system_name', 'avatar_url', 'country_code', 'city', 'department_id', 'agent_id',
                  'status', 'emails']
PK_CHATS = ['id']
JSON_CHATS = ['tags', 'emails']

FIELDS_COMPANIES = ['id', 'name', 'system_name', 'description', 'avatar_url', 'type', 'date_created', 'date_changed',
                    'language', 'city', 'countrycode', 'ip', 'emails', 'phones', 'groups']
FIELDS_R_COMPANIES = ['id', 'name', 'system_name', 'description', 'avatar_url', 'type', 'date_created', 'date_changed',
                      'language', 'city', 'country_code', 'ip', 'emails', 'phones', 'groups']
PK_COMPANIES = ['id']
JSON_COMPANIES = ['emails', 'phones', 'groups']

FIELDS_CONTACTS = ['id', 'company_id', 'firstname', 'lastname', 'system_name', 'description', 'avatar_url', 'gender',
                   'language', 'city', 'countrycode', 'ip', 'emails', 'phones', 'groups', 'custom_fields',
                   'type', 'date_created', 'date_changed']
FIELDS_R_CONTACTS = ['id', 'company_id', 'first_name', 'last_name', 'system_name', 'description', 'avatar_url',
                     'gender', 'language', 'city', 'country_code', 'ip', 'emails', 'phones', 'groups', 'custom_fields',
                     'type', 'date_created', 'date_changed']
PK_CONTACTS = ['id']
JSON_CONTACTS = ['emails', 'phones', 'groups', 'custom_fields']

FIELDS_DEPARTMENTS = ['department_id', 'agent_count', 'name', 'online_status', 'agent_ids', 'mailaccount_id']
FIELDS_R_DEPARTMENTS = FIELDS_DEPARTMENTS
PK_DEPARTMENTS = ['department_id']
JSON_DEPARTMENTS = ['agent_ids']

FIELDS_TAGS = ['id', 'name', 'color', 'background_color', 'is_public']
FIELDS_R_TAGS = FIELDS_TAGS
PK_TAGS = ['id']
JSON_TAGS = []

FIELDS_TICKETS = ['id', 'owner_contactid', 'owner_email', 'owner_name', 'departmentid', 'agentid', 'status', 'tags',
                  'code', 'channel_type', 'date_created', 'date_changed', 'date_resolved', 'date_due',
                  'date_deleted', 'last_activity', 'last_activity_public', 'public_access_urlcode',
                  'subject', 'custom_fields']
FIELDS_R_TICKETS = ['id', 'owner_contact_id', 'owner_email', 'owner_name', 'department_id', 'agent_id', 'status',
                    'tags', 'code', 'channel_type', 'date_created', 'date_changed', 'date_resolved', 'date_due',
                    'date_deleted', 'last_activity', 'last_activity_public', 'public_access_urlcode',
                    'subject', 'custom_fields']
PK_TICKETS = ['id']
JSON_TICKETS = ['tags', 'custom_fields']

FIELDS_TICKETS_MESSAGES = ['id', 'parent_id', 'ticket_id', 'userid', 'user_full_name', 'type', 'status', 'datecreated',
                           'datefinished', 'sort_order', 'mail_msg_id', 'pop3_msg_id']
FIELDS_R_TICKETS_MESSAGES = ['id', 'parent_id', 'ticket_id', 'user_id', 'user_full_name', 'type', 'status',
                             'date_created', 'date_finished', 'sort_order', 'mail_msg_id', 'pop3_msg_id']
PK_TICKETS_MESSAGES = ['id', 'ticket_id']
JSON_TICKETS_MESSAGES = []

FIELDS_TICKETS_MESSAGES_CONTENT = ['id', 'message_id', 'userid', 'type', 'datecreated', 'format',
                                   'message', 'visibility']
FIELDS_R_TICKETS_MESSAGES_CONTENT = ['id', 'message_id', 'user_id', 'type', 'date_created', 'format',
                                     'message', 'visibility']
PK_TICKETS_MESSAGES_CONTENT = ['id', 'message_id']
JSON_TICKETS_MESSAGES_CONTENT = []

FIELDS_AGENT_AVAILABILITY = ['id', 'userid', 'from_date', 'to_date', 'firstname', 'lastname',
                             'contactid', 'departmentid', 'department_name', 'hours_online']
FIELDS_R_AGENT_AVAILABILITY = ['id', 'agent_id', 'date_from', 'date_to', 'first_name', 'last_name',
                               'contact_id', 'department_id', 'department_name', 'hours_online']
PK_AGENT_AVAILABILITY = ['id', 'agent_id']
JSON_AGENT_AVAILABILITY = []

FIELDS_AGENT_REPORT = ['id', 'date', 'contactid', 'firstname', 'lastname', 'worktime', 'answers', 'answers_ph',
                       'newAnswerAvgTime', 'newAnswerAvgTimeSla', 'nextAnswerAvgTime', 'nextAnswerAvgTimeSla',
                       'calls', 'calls_ph', 'missed_calls', 'missed_calls_ph', 'call_seconds', 'call_seconds_ph',
                       'chats', 'chats_ph', 'chat_answers', 'chat_answers_ph', 'missed_chats', 'missed_chats_ph',
                       'chat_pickup', 'chatPickupAvgTime', 'chatAvgTime', 'not_ranked', 'not_ranked_p',
                       'not_ranked_ph', 'rewards', 'rewards_p', 'rewards_ph', 'punishments', 'punishments_p',
                       'punishments_ph', 'created_tickets', 'resolved_tickets', 'u_chats', 'u_calls', 'notes',
                       'firstAssignAvgTime', 'firstAssignAvgTimeSla', 'firstResolveAvgTime', 'firstResolveAvgTimeSla',
                       'calls_outgoing', 'call_outgoing_seconds', 'call_outgoing_avg_time', 'call_pickup_avg_time',
                       'call_avg_time', 'calls_internal', 'call_internal_avg_time', 'call_internal_seconds', 'o_calls']
FIELDS_R_AGENT_REPORT = ['agent_id', 'date', 'contact_id', 'first_name', 'last_name', 'work_time', 'answers',
                         'answers_ph', 'new_answer_avg_time', 'new_answer_avg_time_sla', 'next_answer_avg_time',
                         'next_answer_avg_time_sla', 'calls', 'calls_ph', 'missed_calls', 'missed_calls_ph',
                         'call_seconds', 'call_seconds_ph', 'chats', 'chats_ph', 'chat_answers', 'chat_answers_ph',
                         'missed_chats', 'missed_chats_ph', 'chat_pickup', 'chat_pickup_avg_time', 'chat_avg_time',
                         'not_ranked', 'not_ranked_p', 'not_ranked_ph', 'rewards', 'rewards_p', 'rewards_ph',
                         'punishments', 'punishments_p', 'punishments_ph', 'created_tickets', 'resolved_tickets',
                         'u_chats', 'u_calls', 'notes', 'first_assign_avg_time', 'first_assign_avg_time_sla',
                         'first_resolve_avg_time', 'first_resolve_avg_time_sla', 'calls_outgoing',
                         'call_outgoing_seconds', 'call_outgoing_avg_time', 'call_pickup_avg_time', 'call_avg_time',
                         'calls_internal', 'call_internal_avg_time', 'call_internal_seconds', 'o_calls']
PK_AGENT_REPORT = ['agent_id', 'date']
JSON_AGENT_REPORT = []


class LiveAgentWriter:

    def __init__(self, tableOutPath, tableName, incremental):

        self.paramPath = tableOutPath
        self.paramTableName = tableName
        self.paramTable = tableName + '.csv'
        self.paramTablePath = os.path.join(self.paramPath, self.paramTable)
        self.paramFields = eval(f'FIELDS_{tableName.upper().replace("-", "_")}')
        self.paramJsonFields = eval(f'JSON_{tableName.upper().replace("-", "_")}')
        self.paramPrimaryKey = eval(f'PK_{tableName.upper().replace("-", "_")}')
        self.paramFieldsRenamed = eval(f'FIELDS_R_{tableName.upper().replace("-", "_")}')
        self.paramIncremental = incremental

        self.createManifest()
        self.createWriter()

    def createManifest(self):

        template = {
            'incremental': self.paramIncremental,
            'primary_key': self.paramPrimaryKey,
            'columns': self.paramFieldsRenamed
        }

        path = self.paramTablePath + '.manifest'

        with open(path, 'w') as manifest:

            json.dump(template, manifest)

    def createWriter(self):

        self.writer = csv.DictWriter(open(self.paramTablePath, 'w'), fieldnames=self.paramFields,
                                     restval='', extrasaction='ignore', quotechar='\"', quoting=csv.QUOTE_ALL)

    def writerows(self, listToWrite, parentDict=None):

        for row in listToWrite:

            row_f = self.flatten_json(x=row)

            if self.paramJsonFields != []:
                for field in self.paramJsonFields:
                    row_f[field] = json.dumps(row[field])

            _dictToWrite = {}

            for key, value in row_f.items():

                if key in self.paramFields:
                    _dictToWrite[key] = value

                else:
                    continue

            if parentDict is not None:
                _dictToWrite = {**_dictToWrite, **parentDict}

            self.writer.writerow(_dictToWrite)

    def flatten_json(self, x, out=None, name=''):
        if out is None:
            out = dict()

        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '_')
        else:
            out[name[:-1]] = x

        return out
