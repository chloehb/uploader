import os
import sys
import yaml
import uuid
import base64
import logging
import requests
import numpy as np
import pandas as pd
import datetime as dt
from requests_oauthlib import OAuth2Session
from urllib3.exceptions import ConnectionError, NewConnectionError
import uploader.upload.utils as utl

aw_path = 'aw'
config_path = os.path.join(utl.config_file_path, aw_path)


def _populate_aw_result(result, r):
    """Fill ``result`` with platform_id / status / error from a Google
    Ads ``mutate`` response. ``r`` is the ``requests.Response``
    returned by ``AwApi.mutate_service``.
    """
    try:
        body = r.json() if r is not None else {}
    except (ValueError, AttributeError):
        body = {}
    if isinstance(body, list):
        body = body[0] if body else {}
    if not isinstance(body, dict):
        body = {}
    if 'error' in body:
        err = body.get('error') or {}
        result['status'] = 'failed'
        result['error_code'] = str(err.get('code', '')) or None
        result['error_message'] = (
            err.get('message') or 'Unknown error from Google Ads')
        return
    rows = body.get('results') or []
    if rows:
        first = rows[0] or {}
        resource_name = first.get('resourceName')
        if resource_name:
            result['platform_id'] = resource_name.rsplit('/', 1)[-1]
        result['status'] = 'created'
    else:
        result['status'] = 'failed'
        result['error_message'] = (
            'Google Ads mutate returned no results')


class AwApi(object):
    version = 22
    base_url = 'https://googleads.googleapis.com/v{}/customers/'.format(version)
    refresh_url = 'https://www.googleapis.com/oauth2/v3/token'
    access_url = '{}:listAccessibleCustomers'.format(base_url[:-1])
    report_url = '/googleAds:searchStream'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.df = pd.DataFrame()
        self.config = None
        self.configfile = None
        self.client_id = None
        self.client_secret = None
        self.developer_token = None
        self.refresh_token = None
        self.client_customer_id = None
        self.config_list = []
        self.adwords_client = None
        self.client = None
        self.login_customer_id = None
        self.cam_dict = {}
        self.ag_dict = {}
        self.ad_dict = {}
        self.v = 'v201809'
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        logging.info('Loading Adwords config file: {}'.format(config))
        self.configfile = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        self.adwords_client = None
        """
        self.adwords_client = (adwords.AdWordsClient.
                               LoadFromStorage(self.configfile))
        """

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = yaml.safe_load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.config = self.config['adwords']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.developer_token = self.config['developer_token']
        self.refresh_token = self.config['refresh_token']
        self.client_customer_id = self.config['client_customer_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.developer_token, self.refresh_token,
                            self.client_customer_id]
        if 'login_customer_id' in self.config:
            self.login_customer_id = self.config['login_customer_id']
        else:
            self.login_customer_id = ''

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AW config file.'.format(item))
                sys.exit(0)

    def refresh_client_token(self, extra, attempt=1):
        try:
            token = self.client.refresh_token(self.refresh_url, **extra)
        except requests.exceptions.ConnectionError as e:
            attempt += 1
            if attempt > 100:
                logging.warning('Max retries exceeded: {}'.format(e))
                token = None
            else:
                logging.warning('Connection error retrying 60s: {}'.format(e))
                token = self.refresh_client_token(extra, attempt)
        return token

    def get_client(self):
        token = {'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.refresh_client_token(extra)
        self.client = OAuth2Session(self.client_id, token=token)
        header = self.get_headers()
        return header

    def get_headers(self):
        login_customer_id = str(self.login_customer_id).replace('-', '')
        header = {"Content-Type": "application/json",
                  "developer-token": self.developer_token,
                  "Authorization": "Bearer {}".format(self.refresh_token)}
        if login_customer_id:
            header["login-customer-id"] = login_customer_id
        return header

    def get_report_url(self, url_type=None):
        if not url_type:
            url_type = self.report_url
        cid = self.client_customer_id.replace('-', '')
        url = '{}{}{}'.format(self.base_url, cid, url_type)
        return url

    def request_report(self, report):
        if self.login_customer_id:
            logging.info('Requesting Report.')
            headers = self.get_client()
            report_url = self.get_report_url()
            try:
                r = self.client.post(report_url, json=report, headers=headers)
            except (ConnectionError, NewConnectionError) as e:
                logging.warning('Connection error, retrying: \n{}'.format(e))
                r = self.request_report(report)
        else:
            logging.warning('No login customer id, attempting to find.')
            r = self.find_correct_login_customer_id(report)
        return r

    def find_correct_login_customer_id(self, report):
        headers = self.get_client()
        r = self.client.get(self.access_url, headers=headers)
        response = r.json()
        if 'resourceNames' not in response:
            logging.warning(response)
        customer_ids = response['resourceNames']
        for customer_id in customer_ids:
            customer_id = customer_id.replace('customers/', '')
            logging.info('Attempting customer id: {}'.format(customer_id))
            self.login_customer_id = customer_id
            r = self.request_report(report)
            if r.json() == [] or 'results' in r.json()[0]:
                self.config['login_customer_id'] = self.login_customer_id
                with open(self.configfile, 'w') as f:
                    yaml.dump({'adwords': self.config}, f)
                return r
        logging.warning('Could not find customer ID exiting.')
        sys.exit(0)

    @staticmethod
    def get_operation(operand, operator='ADD'):
        operation = [{
            'operator': operator,
            'operand': x
        } for x in operand]
        return operation

    def mutate_service(self, service, operand, operation='create',
                       update_mask=None):
        """
        Makes request to create or update an object (service) in
        Google Ads.

        :param service: String value of the object to mutate
        :param operand: Dictionary of the object
        :param operation: Mutate operation key ('create' or 'update')
        :param update_mask: Comma-joined field mask for updates
        :return: Response to the request
        """
        url = self.get_report_url(url_type='/{}'.format(service))
        url = '{}:mutate'.format(url)
        op = {operation: operand}
        if update_mask:
            op['updateMask'] = update_mask
        operand = {'operations': [op]}
        headers = self.get_client()
        r = self.client.post(url, json=operand, headers=headers)
        if 'error' in r.json():
            logging.warning('Could not upload: {}'.format(r.json()))
        return r

    def probe_account(self):
        """(ok, message) — one-row GAQL read to verify the customer
        is reachable, for the live pre-flight checks."""
        try:
            r = self.request_report(
                {'query': 'SELECT customer.id FROM customer LIMIT 1'})
            body = r.json() if r is not None else {}
            if isinstance(body, dict) and body.get('error'):
                err = body['error']
                return False, str(err.get('message') or err)
            return True, ''
        except SystemExit:
            return False, 'No accessible customer id for this login.'
        except Exception as e:
            return False, str(e)

    mutate_services_by_level = {'Campaign': 'campaigns',
                                'Adset': 'adGroups', 'Ad': 'adGroupAds'}

    def update_statuses(self, object_level, platform_ids, activate=True):
        """Set ENABLED/PAUSED on existing objects via a mutate update.
        Ad ids are the adGroupAds resource tail (``adgroupid~adid``).
        Returns one dict per id: {'platform_id', 'status'
        ('updated'|'failed'), 'error_code', 'error_message'}."""
        service = self.mutate_services_by_level.get(object_level)
        status = 'ENABLED' if activate else 'PAUSED'
        cid = str(self.client_customer_id or '').replace('-', '')
        results = []
        for pid in platform_ids:
            result = {'platform_id': pid, 'status': 'updated',
                      'error_code': None, 'error_message': None}
            if not service:
                result['status'] = 'failed'
                result['error_message'] = (
                    'Unknown Google Ads level: {}'.format(object_level))
                results.append(result)
                continue
            resource = 'customers/{}/{}/{}'.format(cid, service, pid)
            operand = {'resourceName': resource, 'status': status}
            try:
                r = self.mutate_service(
                    service, operand, operation='update',
                    update_mask='status')
                body = r.json() if r is not None else {}
                if not isinstance(body, dict):
                    body = body[0] if body else {}
                err = (body or {}).get('error')
                if err:
                    result['status'] = 'failed'
                    result['error_code'] = (
                        str(err.get('code', '')) or None)
                    result['error_message'] = (
                        err.get('message')
                        or 'Unknown error from Google Ads')
            except Exception as e:
                result['status'] = 'failed'
                result['error_message'] = str(e)
            results.append(result)
        return results

    def upload_creative(self, file_path):
        """Upload a local image as a Google Ads image asset
        (``assets:mutate``; the old ``MediaService`` is gone) and
        return its ids. Wire format unverified — validate on a real
        account before relying on it in live ad creation.
        """
        with open(file_path, 'rb') as f:
            data = base64.b64encode(f.read()).decode('ascii')
        operand = {'name': os.path.basename(file_path),
                   'type': 'IMAGE',
                   'imageAsset': {'data': data}}
        r = self.mutate_service('assets', operand)
        try:
            body = r.json() if r is not None else {}
        except (ValueError, AttributeError):
            body = {}
        if isinstance(body, list):
            body = body[0] if body else {}
        results = (body or {}).get('results') or []
        resource_name = results[0].get('resourceName') if results else None
        asset_id = (resource_name.rsplit('/', 1)[-1]
                    if resource_name else None)
        return {'mediaId': asset_id, 'referenceId': resource_name}

    def get_id_dict(self, service='campaign', parent=None, page_len=100,
                    fields=None, nest=None, selector_fields=True):
        if selector_fields:
            selector_fields = ['id', 'status']
        else:
            selector_fields = []
        for x in [fields, parent]:
            if x:
                selector_fields.extend(list(x.keys()))
        gaql_fields = ['{}.{}'.format(service, x) for x in selector_fields]
        base_query = f"SELECT {', '.join(gaql_fields)} FROM {service}"
        body = {
            "query": base_query,
        }
        r = self.request_report(body)
        resp_val = [x.capitalize() if idx != 0 else x for idx, x in
                    enumerate(service.split('_'))]
        resp_val = ''.join(resp_val)
        id_dict = {}
        results = r.json()
        if results:
            for x in r.json()[0]['results']:
                name_val = 'name'
                id_val = 'id'
                if name_val not in x[resp_val]:
                    name_val = 'resourceName'
                    id_val = name_val
                name = x[resp_val][name_val]
                cur_id = x[resp_val][id_val]
                id_dict[name] = {'id': cur_id, 'name': name}
                if parent:
                    parent_key = list(parent.keys())[0]
                    if parent_key not in x[resp_val]:
                        parent_key = [x.capitalize() if idx != 0 else x for
                                      idx, x in
                                      enumerate(parent_key.split('_'))]
                        parent_key = ''.join(parent_key)
                    parent_val = x[resp_val][parent_key]
                    id_dict[name]['parent'] = parent_val
        """
        while more_pages:

            id_dict = self.get_dict_from_page(id_dict, page,
                                              list(parent.values())[0],
                                              list(fields.values()), nest)
            start_index += page_len
            selector['paging']['startIndex'] = str(start_index)
            more_pages = start_index < int(page['totalNumEntries'])
        """
        return id_dict

    @staticmethod
    def get_dict_from_page(id_dict, page, parent, fields=None, nest=None):
        resp_fields = [parent]
        if fields:
            resp_fields += fields
        id_dict.update({x[nest]['id'] if nest else x['id']:
                       {'parent' if y == parent else y.replace('.', ''):
                        x[nest][y] if nest and y in x[nest] else x[y]
                        for y in resp_fields
                        if y in x or (nest and y in x[nest])}
                        for x in page['entries'] if 'entries'})
        return id_dict

    def set_budget(self, name, budget, start_date, end_date):
        start = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        end = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        total_days = (end - start).days + 1
        daily_budget = budget / total_days
        name = '{}-{}'.format(name, uuid.uuid4())
        budget = {
            'name': '{}-{}'.format(name, uuid.uuid4()),
            "amountMicros": int(daily_budget * 1000000),
        }
        r = self.mutate_service('campaignBudgets', budget)
        budget_id = r.json()['results'][0]['resourceName']
        return budget_id

    def get_campaign_id_dict(self):
        # parent = {'BaseCampaignId': 'baseCampaignId'}
        fields = {'name': 'name'}
        cam_dict = self.get_id_dict(service='campaign', fields=fields)
        return cam_dict

    def get_adgroup_id_dict(self):
        parent = {'campaign': 'campaign'}
        fields = {'name': 'name'}
        ag_dict = self.get_id_dict(service='ad_group', fields=fields,
                                   parent=parent)
        return ag_dict

    def get_ad_dict(self):
        parent = {'ad_group': 'ad_group'}
        fields = [
            'ad.display_url', 'ad.final_urls',
            'ad.tracking_url_template',
            'ad.expanded_text_ad.headline_part1',
            'ad.expanded_text_ad.headline_part2',
            'ad.expanded_text_ad.headline_part3',
            'ad.expanded_text_ad.description',
            'ad.expanded_text_ad.description2']
        fields = {x: x for x in fields}
        ad_dict = self.get_id_dict(service='ad_group_ad', parent=parent,
                                   fields=fields, selector_fields=False)
        return ad_dict

    def set_id_dict(self, aw_object='all'):
        if aw_object in ['campaign', 'adgroup', 'ad', 'all']:
            self.cam_dict = self.get_campaign_id_dict()
        if aw_object in ['adgroup', 'ad', 'all']:
            self.ag_dict = self.get_adgroup_id_dict()
        if aw_object in ['ad', 'all']:
            self.ad_dict = self.get_ad_dict()

    @staticmethod
    def get_id(dict_o, match, dict_two=None, match_two=None, parent_id=None):
        id_list = []
        for k, v in dict_o.items():
            if v['name'] == match:
                if not parent_id or (parent_id and v['parent']):
                    id_list.append(v['id'])
        if dict_two is not None:
            id_list = [k for k, v in dict_two.items() if v['name'] == match_two
                       and v['parent'] == id_list[0]]
        return id_list

    def check_exists(self, name, aw_object, object_dict, parent_id=None):
        if not object_dict:
            self.set_id_dict(aw_object)
        if self.get_id(object_dict, name, parent_id):
            logging.warning('{} already in account.  '
                            'This {} was not uploaded.'.format(name, aw_object))
            return True

    def create_campaign(self, campaign, service='campaigns'):
        budget_id = self.set_budget(campaign.name, campaign.budget,
                                    campaign.startDate, campaign.endDate)
        campaign.cam_dict['campaignBudget'] = budget_id
        campaigns = self.mutate_service(service, campaign.cam_dict)
        """
        campaign.id = campaigns['value'][0]['id']
        self.add_targets(campaign, service='CampaignCriterionService',
                         positive='CampaignCriterion',
                         negative='NegativeCampaignCriterion',
                         id_name='campaignId')
        """
        return campaigns

    def create_adgroup(self, ag, service='adGroups'):
        """
        https://developers.google.com/google-ads/api/reference/rpc/v22/AdGroup

        :param ag:
        :param service:
        :return:
        """
        ad_groups = self.mutate_service(service, ag.ag_dict)
        """
        ag.id = ad_groups['value'][0]['id']
        self.add_targets(ag)
        """
        return ad_groups

    def add_targets(self, aw_object, service='AdGroupCriterionService',
                    positive='BiddableAdGroupCriterion',
                    negative='NegativeAdGroupCriterion', id_name='adGroupId'):
        targets = [{'xsi_type': positive, 'operator': 'ADD',
                    'dict': aw_object.target_dict},
                   {'xsi_type': negative, 'operator': 'ADD',
                    'dict': aw_object.negative_target_dict},
                   {'dict': aw_object.bid_dict, 'operator': 'SET',
                    'bidModifier': 0.0}]
        for target in targets:
            if target['dict']:
                base_operand = {x: target[x] for x in target
                                if x not in ['dict', 'operator']}
                base_operand[id_name] = aw_object.id
                operand = [{'criterion': x} for x in target['dict']]
                [x.update(base_operand) for x in operand]
                self.mutate_service(service, operand, target['operator'])

    def create_ad(self, ad):
        ads = self.mutate_service('adGroupAds', ad.operand)
        return ads


class CampaignUpload(object):
    name = 'name'
    status = 'status'
    sd = 'startDate'
    ed = 'endDate'
    budget = 'budget'
    method = 'deliveryMethod'
    freq = 'frequencyCap'
    channel = 'advertisingChannelType'
    channel_sub = 'advertisingChannelSubType'
    network = 'networkSetting'
    strategy = 'biddingStrategy'
    settings = 'settings'
    language = 'language'
    location = 'location'
    platform = 'platform'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        df = self.apply_targets(df)
        date_cols = [self.sd, self.ed]
        df = utl.data_to_type(df, date_col=[self.sd, self.ed])
        for col in date_cols:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        self.config = df.to_dict(orient='index')
        for k in self.config:
            for item in [self.freq, self.network, self.strategy]:
                self.config[k][item] = self.config[k][item].split('|')

    def apply_targets(self, df):
        targets = [self.language, self.location]
        bid_adjust = [self.platform]
        df = TargetConfig().load_targets(df, targets,
                                         bid_adjust_names=bid_adjust)
        return df

    def set_campaign(self, campaign):
        cam = Campaign(self.config[campaign])
        return cam

    def upload_all_campaigns(self, api):
        total_camp = str(len(self.config))
        results = []
        for idx, c_id in enumerate(self.config):
            logging.info('Uploading campaign {} of {}.  '
                         'Campaign Name: {}'.format(idx + 1, total_camp, c_id))
            results.append(self.upload_campaign(api, c_id))
        logging.info('Campaigns finished uploading.')
        return results

    def upload_campaign(self, api, campaign_id):
        campaign = self.set_campaign(campaign_id)
        result = {
            'source_name': campaign.name,
            'object_level': 'Campaign',
            'uploader_type': 'Adwords',
            'platform_id': None,
            'parent_platform_id': None,
            'status': None,
            'error_code': None,
            'error_message': None,
        }
        if campaign.check_exists(api):
            existing = api.get_id(api.cam_dict, campaign.name)
            if existing:
                result['platform_id'] = str(existing[0])
            result['status'] = 'skipped_exists'
            return result
        r = api.create_campaign(campaign)
        _populate_aw_result(result, r)
        return result


class Campaign(object):
    __slots__ = ['name', 'status', 'startDate', 'endDate', 'budget',
                 'deliveryMethod', 'frequencyCap', 'advertisingChannelType',
                 'advertisingChannelSubType', 'networkSetting',
                 'biddingStrategy', 'settings', 'id', 'cam_dict', 'location',
                 'language', 'platform', 'target_dict', 'negative_target_dict',
                 'bid_dict']

    def __init__(self, cam_dict):
        for k in cam_dict:
            setattr(self, k, cam_dict[k])
        self.frequencyCap = self.set_freq(self.frequencyCap)
        self.networkSetting = self.set_net(self.networkSetting)
        self.biddingStrategy = self.set_strat(self.biddingStrategy)
        self.cam_dict = self.create_cam_dict()

    def create_cam_dict(self):
        """
        Creates a dictionary that can be uploaded to the platform
        https://developers.google.com/google-ads/api/reference/rpc/v22/Campaign

        :return: The dictionary to upload
        """
        ad_channel_type = (self.advertisingChannelType
                           if self.advertisingChannelType else 'SEARCH')
        cam_dict = {
            'name': '{}'.format(self.name),
            'status': '{}'.format(self.status),
            'advertisingChannelType': '{}'.format(ad_channel_type),
            'endDate': '{}'.format(self.endDate),
            'networkSettings': self.networkSetting,
        }
        params = [(self.startDate, 'startDate'), (self.settings, 'settings'),
                  (self.frequencyCap, 'frequency_caps', 'dict'),
                  (self.advertisingChannelSubType, 'advertisingChannelSubType')]
        for param in params:
            if param[0]:
                if len(param) == 3:
                    cam_dict[param[1]] = param[0]
                else:
                    cam_dict[param[1]] = '{}'.format(param[0])
        for k, v in self.biddingStrategy.items():
            cam_dict[k] = v
        return cam_dict

    @staticmethod
    def set_freq(freq):
        """
        Takes the frequency list and creates the dictionary for upload

        https://developers.google.com/google-ads/api/reference/rpc/v22/FrequencyCapEntry

        :param freq: List of values for frequency
        :return:  The dict accepted to upload
        """
        if freq:
            freq_level = freq[2]
            if freq_level == 'ADGROUP':
                freq_level = 'AD_GROUP'
            if len(freq) > 3:
                time_length = freq[3]
            else:
                time_length = 1
            freq_key = {
                'event_type': 'IMPRESSION',
                'time_unit': freq[1],
                'level': freq_level,
                'time_length': time_length
            }
            freq = {'key': freq_key, 'cap': freq[0]}
        return freq

    def set_net(self, network):
        """
        Takes the network settings list and creates the dictionary for upload

        https://developers.google.com/google-ads/api/reference/rpc/v22/Campaign#network_settings

        :param network: List of values for network settings
        :return: The dict accepted to upload
        """
        google_search = 'targetGoogleSearch'
        keys = [google_search, 'targetSearchNetwork',
                'targetContentNetwork', 'targetPartnerSearchNetwork',
                'targetYoutube', 'targetGoogleTvNetwork']
        net_dict = {}
        for key in keys:
            net_dict[key] = 'false'
        is_search = (not self.advertisingChannelType or
                     self.advertisingChannelType == 'SEARCH')
        if is_search:
            network.append(google_search)
        if network:
            for net in network:
                if net:
                    net_dict[net] = 'true'
        return net_dict

    @staticmethod
    def set_strat(strategy):
        """
        Takes the strategy list and creates the dictionary for upload
        https://developers.google.com/google-ads/api/reference/rpc/v22/Campaign#bidding_strategy

        :param strategy: List of values for strategies
        :return: The dict accepted to upload
        """
        strategy_key = strategy[0]
        if '_' in strategy_key:
            strategy_list = strategy_key.split('_')
            strategy_list = [x.lower() if idx == 0 else x.capitalize()
                             for idx, x in enumerate(strategy_list)]
            strategy_key = ''.join(strategy_list)
        strategy_value = {}
        if len(strategy) > 1:
            value_key = 'cpcBidCeilingMicros'
            strategy_value = {value_key: strategy[1]}
        strategy = {strategy_key: strategy_value}
        return strategy

    def check_exists(self, api):
        """
        Gets list of campaigns from platform and stops upload if name exists

        :param api: Instance of AwApi
        :return:
        """
        if not api.cam_dict:
            api.set_id_dict('campaign')
        cid = api.get_id(api.cam_dict, self.name)
        if cid:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True


class AdGroupUpload(object):
    name = 'name'
    cam_name = 'campaign_name'
    status = 'status'
    bid_type = 'bid_type'
    bid_val = 'bid'
    age_range = 'age_range'
    gender = 'gender'
    keyword = 'keyword'
    topic = 'topic'
    placement = 'placement'
    affinity = 'affinity'
    in_market = 'in_market'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_adgroup_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        df = self.apply_targets(df)
        self.config = df.to_dict(orient='index')

    def bar_split(self, df):
        for col in [self.age_range, self.gender]:
            df[col] = df[col].str.split('|')
        return df

    def apply_targets(self, df):
        targets = [self.keyword, self.placement, self.topic, self.affinity,
                   self.in_market]
        negative_targets = [self.age_range, self.gender]
        df = TargetConfig().load_targets(df, targets, negative_targets)
        return df

    def set_adgroup(self, adgroup_id):
        ag = AdGroup(self.config[adgroup_id])
        return ag

    def upload_all_adgroups(self, api):
        tot_ag = str(len(self.config))
        results = []
        for idx, ag_id in enumerate(self.config):
            logging.info('Uploading adgroup {} of {}.'.format(idx + 1, tot_ag))
            results.append(self.upload_adgroup(api, ag_id))
        logging.info('{} adgroups uploaded.'.format(tot_ag))
        return results

    def upload_adgroup(self, api, ag_id):
        ag = self.set_adgroup(ag_id)
        logging.info('Adgroup name: {}'.format(ag.name))
        result = {
            'source_name': ag.name,
            'object_level': 'Adset',
            'uploader_type': 'Adwords',
            'platform_id': None,
            'parent_platform_id': None,
            'status': None,
            'error_code': None,
            'error_message': None,
        }
        if ag.check_exists(api):
            existing = api.get_id(api.ag_dict, ag.name)
            if existing:
                result['platform_id'] = str(existing[0])
            result['status'] = 'skipped_exists'
            return result
        r = api.create_adgroup(ag)
        _populate_aw_result(result, r)
        return result


class TargetConfig(object):
    def __init__(self, target_file='aw_target_upload.xlsx', df=None):
        self.target_file = target_file
        self.df = df
        self.file_path = os.path.join(config_path, 'target')
        self.target_dict = {
            AdGroupUpload.keyword: {
                'fnc': Target.format_keywords,
                'api_name': 'Keyword'},
            AdGroupUpload.placement: {
                'fnc': Target.format_placement,
                'api_name': 'Placement', 'api_id': 'url'},
            AdGroupUpload.topic: {
                'map_file': 'aw_verticals.csv',
                'api_name': 'Vertical',
                'api_id': 'verticalId'},
            AdGroupUpload.affinity: {
                'map_file': 'aw_affinity.csv',
                'api_name': 'CriterionUserInterest',
                'api_id': 'userInterestId'},
            AdGroupUpload.in_market: {
                'map_file': 'aw_inmarket.csv',
                'api_name': 'CriterionUserInterest',
                'api_id': 'userInterestId'},
            AdGroupUpload.age_range: {
                'map_file': 'aw_ages.csv',
                'map_name': 'Age range',
                'api_name': 'AgeRange'},
            AdGroupUpload.gender: {
                'map_file': 'aw_genders.csv',
                'map_name': 'Gender',
                'api_name': 'Gender'},
            CampaignUpload.language: {
                'map_file': 'aw_languagecodes.csv',
                'map_name': 'Language name',
                'api_name': 'Language'},
            CampaignUpload.location: {
                'map_file': 'aw_locations.csv',
                'map_name': 'Canonical Name',
                'map_id': 'Criteria ID',
                'api_name': 'Location'},
            CampaignUpload.platform: {
                'map_file': 'aw_platforms.csv',
                'map_name': 'Platform name',
                'api_name': 'Platform'}}
        if self.target_file:
            self.load_file()

    def load_file(self):
        self.df = pd.read_excel(os.path.join(config_path, self.target_file))
        self.df = self.df.fillna('')

    def load_targets(self, upload_df, target_names, negative_target_names=None,
                     bid_adjust_names=None):
        if not negative_target_names:
            negative_target_names = []
        if not bid_adjust_names:
            bid_adjust_names = []
        for target_name in (target_names + negative_target_names +
                            bid_adjust_names):
            params = self.target_dict[target_name]
            target = Target(target_name, target_dict=params, df=self.df)
            upload_df = target.format_target(upload_df)
        upload_df = self.combine_target(upload_df, target_names, 'target_dict')
        upload_df = self.combine_target(upload_df, negative_target_names,
                                        'negative_target_dict')
        upload_df = self.combine_target(upload_df, bid_adjust_names, 'bid_dict')
        return upload_df

    @staticmethod
    def combine_target(upload_df, target_names, col_name):
        upload_df[col_name] = np.empty((len(upload_df), 0)).tolist()
        for target_name in target_names:
            upload_df.apply(lambda x: x[col_name].extend(x[target_name])
                            if str(x[target_name]) != 'nan'
                            else x[col_name], axis=1)
        return upload_df


class Target(object):
    def __init__(self, target_type, fnc=None, map_file=None, df=None,
                 map_id='Criterion ID', map_name='Category', api_id='id',
                 api_name=None, target_file=None, target_dict=None):
        self.target_file = target_file
        self.target_type = target_type
        self.fnc = fnc
        self.map_file = map_file
        self.map_id = map_id
        self.map_name = map_name
        self.api_id = api_id
        self.api_name = api_name
        self.df = df
        self.target_dict = target_dict
        if self.target_dict:
            for k in self.target_dict:
                setattr(self, k, self.target_dict[k])
        if self.map_file:
            self.map_file = os.path.join(config_path, 'target', self.map_file)
        if not self.fnc:
            self.fnc = self.format_vertical
        if self.target_file:
            self.df = self.load_target_file()

    def load_target_file(self):
        self.df = pd.read_excel(os.path.join(config_path, self.target_file))
        self.df = self.df.fillna('')
        return self.df

    def format_target(self, df):
        cols = list(set([x for x in df[self.target_type].tolist() if x]))
        if self.map_file:
            self.df = self.map_cols(self.df, cols=cols, map_file=self.map_file,
                                    id_col=self.map_id, val_col=self.map_name)
        target_map = self.fnc(self.df, cols=cols)
        target_map = self.format_map(target_map)
        df[self.target_type] = df[self.target_type].map(target_map)
        return df

    def format_map(self, target_map):
        for t in target_map:
            if self.target_type == AdGroupUpload.keyword:
                target_map[t] = [{'xsi_type': self.api_name,
                                  'matchType': x[0], 'text': x[1]}
                                 for x in target_map[t] if x and x != ['']]
            else:
                target_map[t] = [{'xsi_type': self.api_name, self.api_id: x}
                                 for x in target_map[t] if x and x != ['']]
        return target_map

    @staticmethod
    def format_keywords(df, cols):
        for col in cols:
            df[col] = 'BROAD|' + df[col]
            for kw_t in [('[', 'EXACT|'), ('"', 'PHRASE|')]:
                df[col] = np.where(df[col].str.contains(kw_t[0], regex=False),
                                   df[col].str.replace('BROAD|', kw_t[1],
                                                       regex=False), df[col])
            for r in ['[', ']', '"']:
                df[col] = df[col].str.replace(r, '', regex=False)
            df[col] = df[col].replace('BROAD|', '', regex=False)
            df[col] = df[col].str.split('|')
        keyword_config = df[cols].to_dict(orient='list')
        return keyword_config

    @staticmethod
    def map_cols(df, cols, map_file, id_col, val_col):
        vdf = pd.read_csv(map_file)
        vdf = vdf[[val_col, id_col]].set_index(val_col)
        vdf = vdf.to_dict(orient='dict')[id_col]
        for col in cols:
            df[col] = df[col].map(vdf)
        return df

    @staticmethod
    def format_vertical(df, cols):
        vertical_config = df[cols].to_dict(orient='list')
        vertical_config = {x: [int(y) for y in vertical_config[x]
                               if str(y) != 'nan'] for x in vertical_config}
        return vertical_config

    @staticmethod
    def format_placement(df, cols):
        placement_config = df[cols].to_dict(orient='list')
        return placement_config


class AdGroup(object):
    __slots__ = ['name', 'campaign_name', 'status', 'bid_type', 'bid',
                 'keyword', 'topic', 'placement', 'ag_dict', 'target_dict',
                 'negative_target_dict', 'id', 'cid', 'operand', 'parent',
                 'age_range', 'gender', 'affinity', 'in_market', 'bid_dict']

    def __init__(self, ag_dict):
        self.name = None
        self.campaign_name = None
        self.status = None
        self.bid_type = None
        self.bid = None
        self.age_range = None
        self.gender = None
        self.keyword = None
        self.topic = None
        self.placement = None
        self.affinity = None
        self.in_market = None
        self.ag_dict = None
        self.target_dict = None
        self.negative_target_dict = None
        self.bid_dict = None
        self.id = None
        self.cid = None
        self.operand = None
        self.parent = None
        for k in ag_dict:
            setattr(self, k, ag_dict[k])
        self.ag_dict = self.create_adgroup_dict()
        if self.parent:
            self.set_operand()

    def create_adgroup_dict(self):
        if not self.bid_type:
            self.bid_type = 'cpcBidMicros'
        bid_amount = '{}'.format(self.bid * 1000000)
        ag_dict = {'name': '{}'.format(self.name),
                   'status': '{}'.format(self.status),
                   self.bid_type: bid_amount}
        return ag_dict

    def check_exists(self, api):
        self.set_operand(api)
        ag_id = api.get_id(api.cam_dict, self.campaign_name,
                           api.ag_dict, self.name)
        if ag_id:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True

    def set_parent(self, api):
        if not api.ag_dict:
            api.set_id_dict('adgroup')
        parent_list = api.get_id(api.cam_dict, self.campaign_name)
        if len(parent_list) == 0:
            logging.warning('Campaign {} not in account.  Could not upload '
                            'ad group'.format(self.campaign_name))
        cid = api.client_customer_id.replace('-', '')
        parent = 'customers/{}/campaigns/{}'.format(cid, parent_list[0])
        self.parent = parent

    def set_operand(self, api=None):
        if api:
            self.set_parent(api)
        self.operand = self.ag_dict
        self.operand['campaign'] = '{}'.format(self.parent)


class AdUpload(object):
    ag_name = 'ad_group_name'
    cam_name = 'campaign_name'
    name = 'name'
    type = 'AdType'
    headline1 = 'headlinePart1'
    headline2 = 'headlinePart2'
    headline3 = 'headlinePart3'
    description = 'description'
    description2 = 'description2'
    business_name = 'businessName'
    final_url = 'finalUrls'
    track_url = 'trackingUrlTemplate'
    display_url = 'displayUrl'
    marketing_image = 'marketingImage'
    image = 'image'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_ad_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        df = self.check_urls(df)
        self.config = df.to_dict(orient='index')
        for k in self.config:
            for item in [self.final_url]:
                self.config[k][item] = self.config[k][item].split('|')

    def set_ad(self, ad_id):
        ad = Ad(self.config[ad_id])
        return ad

    def check_urls(self, df):
        url_cols = [self.final_url, self.track_url]
        df = utl.data_to_type(df, str_col=url_cols)
        for col in url_cols:
            df[col] = np.where(df[col].str[:4] != 'http',
                               'http://' + df[col], df[col])
        return df

    def upload_all_creatives(self, api):
        creatives = set(self.config[x][self.marketing_image] for x in
                        self.config if self.config[x][self.marketing_image])
        img_cre = set(self.config[x][self.image] for x in
                      self.config if self.config[x][self.image])
        creatives = creatives.union(img_cre)
        cu = CreativeUpload()
        cu.upload_all(api, list(creatives))
        self.creative_filename_to_id(cu)
        return cu

    def creative_filename_to_id(self, cu):
        for k in self.config:
            for img in [self.marketing_image, self.image]:
                if self.config[k][img]:
                    self.config[k][img] = cu.get_id(
                        self.config[k][img], CreativeUpload.media_id)

    def upload_all_ads(self, api):
        cu = self.upload_all_creatives(api)
        total_ad = str(len(self.config))
        results = []
        for idx, ad_id in enumerate(self.config):
            logging.info('Uploading ad {} of {}.  '
                         'Ad Row: {}'.format(idx + 1, total_ad, ad_id + 2))
            results.append(self.upload_ad(api, ad_id, cu))
        logging.info('{} ads uploaded.'.format(total_ad))
        return results

    def upload_ad(self, api, ad_id, cu):
        ad = self.set_ad(ad_id)
        result = {
            'source_name': ad.name,
            'object_level': 'Ad',
            'uploader_type': 'Adwords',
            'platform_id': None,
            'parent_platform_id': None,
            'status': None,
            'error_code': None,
            'error_message': None,
        }
        if ad.check_exists(api, cu):
            result['status'] = 'skipped_exists'
            return result
        r = api.create_ad(ad)
        _populate_aw_result(result, r)
        return result


class Ad(object):
    def __init__(self, ad_dict, cu=None):
        self.cu = cu
        self.ad_group_name = None
        self.campaign_name = None
        self.name = None
        self.headlinePart1 = None
        self.headlinePart2 = None
        self.headlinePart3 = None
        self.description = None
        self.description2 = None
        self.businessName = None
        self.shortHeadline = None
        self.longHeadline = None
        self.finalUrls = None
        self.trackingUrlTemplate = None
        self.urlData = None
        self.displayUrl = None
        self.AdType = None
        self.marketingImage = None
        self.image = None
        self.parent = None
        self.operand = None
        for k in ad_dict:
            setattr(self, k, ad_dict[k])
        self.ad_dict = self.create_ad_dict()
        if self.parent:
            self.set_operand()

    def __eq__(self, other):
        return self.operand == other.operand

    def __ne__(self, other):
        return not self.__eq__(other)

    def set_media_id_from_ref(self):
        if self.image and self.cu and isinstance(self.image, dict):
            media_id = self.cu.media_id_for_reference(
                self.image.get('referenceId'))
            if media_id is not None:
                self.image['mediaId'] = media_id

    def create_ad_dict(self):
        self.set_media_id_from_ref()
        if self.marketingImage and not str(self.marketingImage).isdigit():
            self.marketingImage = self.marketingImage['mediaId']
        if self.image and not str(self.image).isdigit():
            self.image = self.image['mediaId']
        final_ad_dict = {
            'finalUrls': self.finalUrls,
            'trackingUrlTemplate': '{}'.format(self.trackingUrlTemplate),
            'name': self.name
        }
        ad_dict = {}
        if not self.AdType:
            self.AdType = 'responsiveSearchAd'
        if self.AdType == 'responsiveSearchAd':
            headlines = [self.headlinePart1, self.headlinePart2,
                         self.headlinePart3, self.shortHeadline,
                         self.longHeadline]
            headlines = [{'text': '{}'.format(x)} for x in headlines if x]
            ad_dict['headlines'] = headlines
            descriptions = [self.description, self.description2]
            descriptions = [{'text': '{}'.format(x)} for x in descriptions if x]
            ad_dict['descriptions'] = descriptions
        if self.AdType == 'ExpandedTextAd':
            ad_dict['headlinePart1'] = '{}'.format(self.headlinePart1)
            ad_dict['headlinePart2'] = '{}'.format(self.headlinePart2)
            ad_dict['description'] = '{}'.format(self.description)
            if self.headlinePart3:
                ad_dict['headlinePart3'] = '{}'.format(self.headlinePart3)
            if self.description2:
                ad_dict['description2'] = '{}'.format(self.description2)
        if self.AdType == 'ResponsiveDisplayAd':
            ad_dict['businessName'] = '{}'.format(self.businessName)
            ad_dict['shortHeadline'] = '{}'.format(self.shortHeadline)
            ad_dict['longHeadline'] = '{}'.format(self.longHeadline)
            ad_dict['description'] = '{}'.format(self.description)
            ad_dict['marketingImage'] = {'mediaId': self.marketingImage}
        if self.AdType == 'ImageAd':
            ad_dict['image'] = {'mediaId': self.image}
            ad_dict['name'] = '{}'.format(self.name)
            ad_dict['displayUrl'] = '{}'.format(self.displayUrl)
        if ad_dict:
            final_ad_dict[self.AdType] = ad_dict
        return final_ad_dict

    def check_exists(self, api, cu):
        self.set_operand(api)
        if self in [Ad(api.ad_dict[x], cu) for x in api.ad_dict]:
            logging.warning('Ad already in account and not uploaded.  '
                            'Operator as follows: \n {}.'.format(self.operand))
            return True

    def set_parent(self, api):
        if not api.ad_dict:
            api.set_id_dict('all')
        parent = api.get_id(api.ag_dict, self.ad_group_name)
        if len(parent) == 0:
            logging.warning('Ad Group {} not in account.  Could not upload '
                            'ad'.format(self.ad_group_name))
        cid = api.client_customer_id.replace('-', '')
        parent = 'customers/{}/adGroups/{}'.format(cid, parent[0])
        self.parent = parent

    def set_operand(self, api=None):
        if api:
            self.set_parent(api)
        self.operand = {
            'adGroup': self.parent,
            'ad': self.ad_dict,
        }


class CreativeUpload(utl.BaseCreativeStore):
    """Google Ads creative store. Persists filename -> image-asset id
    in ``aw_creative_ids.csv`` and resolves it for ad creation. The
    per-file upload (``assets:mutate``) lives on ``AwApi`` so this
    class is the shared find-new / persist bookkeeping only.
    """
    file_name = 'file_name'
    media_id = 'mediaId'
    reference_id = 'referenceId'
    fn_col = file_name
    id_cols = (media_id, reference_id)

    def __init__(self, id_file_name='aw_creative_ids.csv',
                 creative_path='creative/'):
        super().__init__(id_file_name, creative_path)

    def _upload_one(self, api, file_path):
        return api.upload_creative(file_path)

    def media_id_for_reference(self, reference_id):
        """Reverse-resolve an asset's mediaId from its referenceId."""
        for rec in self.records.values():
            if rec.get(self.reference_id) == reference_id:
                return rec.get(self.media_id)
        return None
