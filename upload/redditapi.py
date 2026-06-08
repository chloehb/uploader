"""Reddit Ads API uploader. Mirrors the awapi/dcapi class shape so
the relation system, name-create flow, and run telemetry pick it
up without special-casing."""
import json
import logging
import os
import sys
import time

import pandas as pd
import requests
from requests_oauthlib import OAuth2Session

import uploader.upload.utils as utl


reddit_path = 'reddit'
config_path = os.path.join(utl.config_file_path, reddit_path)
base_url = 'https://ads-api.reddit.com/api/v3'


def _apply_row(instance, row):
    """Copy excel-row keys onto a slotted upload object, logging
    keys that aren't declared in ``__slots__``."""
    for k, v in row.items():
        try:
            setattr(instance, k, v)
        except AttributeError as e:
            logging.warning(f'AttributeError: {e}')


def _to_iso(value):
    """Reddit wants ISO-8601 timestamps; plan-derived flight dates
    arrive as MM/DD/YYYY strings (or excel datetimes)."""
    if value is None or value == '':
        return None
    try:
        return pd.to_datetime(value).strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, TypeError):
        return None


def _populate_reddit_result(result, response):
    """Fill ``result`` from a Reddit Ads create response. Success:
    ``{"data": {"id": ...}}``. Failure: ``{"errors": [...]}``."""
    try:
        body = response.json() if response is not None else {}
    except (ValueError, AttributeError):
        body = {}
    if not isinstance(body, dict):
        body = {}
    data = body.get('data') or {}
    if isinstance(data, dict) and data.get('id'):
        result['platform_id'] = data['id']
        result['status'] = 'created'
        return
    errs = body.get('errors') or []
    err = (errs[0] or {}) if isinstance(errs, list) and errs else {}
    result['status'] = 'failed'
    result['error_code'] = str(err.get('code', '')) or None
    result['error_message'] = (
        err.get('message') or 'Unknown error from Reddit Ads')


class RedditApi(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.business_id = None
        self.ad_account_id = None
        self.config_list = None
        self.client = None
        self.cam_dict = {}
        self.adgroup_dict = {}
        self.ad_dict = {}
        self.creative_dict = {}
        self.r = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning(
                'Reddit config file not in vendor matrix. Aborting.')
            sys.exit(0)
        logging.info(f'Loading Reddit config file: {config}')
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(f'{self.config_file} not found. Aborting.')
            sys.exit(0)
        self.client_id = self.config.get('client_id', '')
        self.client_secret = self.config.get('client_secret', '')
        self.access_token = self.config.get('access_token', '')
        self.refresh_token = self.config.get('refresh_token', '')
        self.refresh_url = self.config.get(
            'refresh_url', 'https://www.reddit.com/api/v1/access_token')
        self.business_id = self.config.get('business_id', '')
        self.ad_account_id = self.config.get('ad_account_id', '')
        self.config_list = [
            self.config, self.client_id, self.client_secret,
            self.refresh_token, self.refresh_url, self.ad_account_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(
                    f'{item} not in Reddit config file. Aborting.')
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.client.refresh_token(self.refresh_url, **extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def _entity_url(self, segment):
        return f'{base_url}/ad_accounts/{self.ad_account_id}/{segment}'

    def create_entity(self, entity, entity_name=''):
        url = self._entity_url(entity_name)
        return self._post(url, body={'data': entity.upload_dict})

    def _post(self, url, body=None):
        self.get_client()
        try:
            self.r = self.client.post(url, json=body or {})
        except requests.exceptions.SSLError as e:
            logging.warning(f'Reddit SSLError: {e}')
            time.sleep(30)
            self.r = self._post(url, body=body)
        return self.r

    def _get(self, url, params=None):
        self.get_client()
        return self.client.get(url, params=params or {})

    @staticmethod
    def get_id(dict_o, match, match_name='name'):
        return [k for k, v in dict_o.items() if v.get(match_name) == match]

    def _list(self, entity_name, params=None):
        url = self._entity_url(entity_name)
        items = {}
        next_after = None
        while True:
            page_params = dict(params or {})
            if next_after:
                page_params['after'] = next_after
            r = self._get(url, params=page_params)
            try:
                body = r.json()
            except ValueError:
                break
            for row in body.get('data') or []:
                rid = row.get('id')
                if rid:
                    items[rid] = row
            page = body.get('pagination') or {}
            next_after = page.get('next_after')
            if not next_after:
                break
        return items

    def set_id_dict(self, kind=None, filter_id=None):
        if kind == 'campaign':
            self.cam_dict = self._list('campaigns')
        elif kind == 'adgroup':
            params = {'campaign_id': filter_id} if filter_id else None
            self.adgroup_dict = self._list('ad_groups', params=params)
        elif kind == 'ad':
            params = {'ad_group_id': filter_id} if filter_id else None
            self.ad_dict = self._list('ads', params=params)
        elif kind == 'creative':
            self.creative_dict = self._list('creatives')

    def get_funding_instruments(self):
        """Funding instruments on the ad account, labelled by currency
        when Reddit returns no display name. Feeds the campaign
        config's ``funding_instrument_id`` picker — the one id every
        Reddit campaign requires that previously had to be hunted in
        the Reddit UI."""
        rows = self._list('funding_instruments')
        return [{'id': fid,
                 'name': ((row or {}).get('name')
                          or (row or {}).get('currency') or fid)}
                for fid, row in (rows or {}).items()]

    def get_creatives(self):
        """Creatives on the ad account, for ad-config lookup."""
        rows = self._list('creatives')
        return [{'id': cid, 'name': (row or {}).get('name', cid)}
                for cid, row in (rows or {}).items()]

    def upload_creative(self, file_path):
        """Upload a local asset to the ad-account ``media`` endpoint and
        return its media id. Wire format unverified — validate on a
        real account before relying on it in live trafficking.
        """
        self.get_client()
        url = self._entity_url('media')
        with open(file_path, 'rb') as f:
            r = self.client.post(url, files={'file': f})
        try:
            body = r.json() if r is not None else {}
        except (ValueError, AttributeError):
            body = {}
        if not isinstance(body, dict):
            body = {}
        media_id = (body.get('data') or {}).get('id') or body.get('id')
        return {'id': media_id}


class CampaignUpload(object):
    file_name = 'campaign_upload.xlsx'
    name = 'name'
    objective = 'objective'
    status = 'configured_status'
    funding_instrument_id = 'funding_instrument_id'
    spend_cap = 'spend_cap'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file=''):
        if not config_file:
            config_file = self.file_name
        file_name = os.path.join(config_path, config_file)
        if not os.path.exists(file_name):
            logging.warning(f'Reddit campaign config missing: {file_name}')
            return False
        df = pd.read_excel(file_name)
        df = df.dropna(subset=[self.name]).fillna('')
        self.config = df.to_dict(orient='index')
        return True

    def upload_all_campaigns(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, c_id in enumerate(self.config):
            cam = Campaign(self.config[c_id], api=api)
            logging.info(
                f'Uploading Reddit campaign {idx + 1} of {total}: '
                f'{cam.name}')
            results.append(self.upload_campaign(api, cam))
        return results

    @staticmethod
    def upload_campaign(api, campaign):
        result = _new_result('Campaign', campaign.name)
        if not campaign.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = 'Missing required campaign fields'
            return result
        if campaign.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = campaign.id
            return result
        _populate_reddit_result(
            result, api.create_entity(campaign, entity_name='campaigns'))
        if result['status'] == 'created':
            campaign.id = result['platform_id']
        return result


def _new_result(object_level, source_name, parent_id=None):
    return {
        'source_name': source_name,
        'object_level': object_level,
        'uploader_type': 'Reddit',
        'platform_id': None,
        'parent_platform_id': str(parent_id) if parent_id else None,
        'status': None,
        'error_code': None,
        'error_message': None,
    }


class Campaign(object):
    __slots__ = ['name', 'objective', 'configured_status',
                 'funding_instrument_id', 'spend_cap',
                 'effective_status', 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.objective = 'TRAFFIC'
        self.configured_status = 'PAUSED'
        self.funding_instrument_id = None
        self.spend_cap = None
        self.effective_status = None
        _apply_row(self, row_dict)
        self.api = api
        self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        if not self.name:
            return {}
        d = {
            'name': str(self.name),
            'objective': str(self.objective or 'TRAFFIC'),
            'configured_status': str(self.configured_status or 'PAUSED'),
        }
        if self.funding_instrument_id:
            d['funding_instrument_id'] = str(self.funding_instrument_id)
        if self.spend_cap:
            d['spend_cap'] = float(self.spend_cap)
        return d

    def check_exists(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        found = api.get_id(api.cam_dict, self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdGroupUpload(object):
    """Reddit's mid-tier object (Adset in LQ parlance)."""
    file_name = 'adset_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    configured_status = 'configured_status'
    bid_strategy = 'bid_strategy'
    budget_type = 'budget_type'
    budget_value = 'budget_value'
    start_time = 'start_time'
    end_time = 'end_time'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file=''):
        if not config_file:
            config_file = self.file_name
        file_name = os.path.join(config_path, config_file)
        if not os.path.exists(file_name):
            logging.warning(f'Reddit adgroup config missing: {file_name}')
            return False
        df = pd.read_excel(file_name)
        df = df.dropna(subset=[self.name]).fillna('')
        self.config = df.to_dict(orient='index')
        return True

    def upload_all_adgroups(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, ag_id in enumerate(self.config):
            ag = AdGroup(self.config[ag_id], api=api)
            logging.info(
                f'Uploading Reddit adgroup {idx + 1} of {total}: '
                f'{ag.name}')
            results.append(self.upload_adgroup(api, ag))
        return results

    @staticmethod
    def upload_adgroup(api, adgroup):
        result = _new_result('Adset', adgroup.name, adgroup.campaignId)
        if not adgroup.campaignId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Campaign {adgroup.campaign!r} not found')
            return result
        if not adgroup.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = 'Missing required ad group fields'
            return result
        if adgroup.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = adgroup.id
            return result
        _populate_reddit_result(
            result, api.create_entity(adgroup, entity_name='ad_groups'))
        if result['status'] == 'created':
            adgroup.id = result['platform_id']
        return result


class AdGroup(object):
    __slots__ = ['name', 'campaign', 'campaignId', 'configured_status',
                 'bid_strategy', 'budget_type', 'budget_value',
                 'start_time', 'end_time', 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.campaignId = None
        self.configured_status = 'PAUSED'
        self.bid_strategy = 'AUTOMATIC'
        self.budget_type = 'DAILY'
        self.budget_value = None
        self.start_time = None
        self.end_time = None
        _apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.resolve_campaign(self.api)
        self.upload_dict = self.create_adgroup_dict()

    def resolve_campaign(self, api):
        cam = Campaign({'name': self.campaign}, api=api)
        cam.check_exists(api)
        self.campaignId = cam.id

    def create_adgroup_dict(self):
        if not (self.name and self.campaignId):
            return {}
        d = {
            'name': str(self.name),
            'campaign_id': str(self.campaignId),
            'configured_status': str(self.configured_status or 'PAUSED'),
            'bid_strategy': str(self.bid_strategy or 'AUTOMATIC'),
            'budget_type': str(self.budget_type or 'DAILY'),
        }
        if self.budget_value:
            d['budget_value'] = float(self.budget_value)
        for col, value in ((AdGroupUpload.start_time, self.start_time),
                           (AdGroupUpload.end_time, self.end_time)):
            iso = _to_iso(value)
            if iso:
                d[col] = iso
        return d

    def check_exists(self, api):
        if not api.adgroup_dict:
            api.set_id_dict('adgroup', filter_id=self.campaignId)
        found = api.get_id(api.adgroup_dict, self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdUpload(object):
    file_name = 'ad_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    adgroup = 'ad_group'
    creative = 'creative'
    configured_status = 'configured_status'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file=''):
        if not config_file:
            config_file = self.file_name
        file_name = os.path.join(config_path, config_file)
        if not os.path.exists(file_name):
            logging.warning(
                'Reddit ad config missing: {}'.format(file_name))
            return False
        df = pd.read_excel(file_name)
        df = df.dropna(subset=[self.name]).fillna('')
        self.config = df.to_dict(orient='index')
        return True

    def upload_all_ads(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, a_id in enumerate(self.config):
            ad = Ad(self.config[a_id], api=api)
            logging.info(
                f'Uploading Reddit ad {idx + 1} of {total}: {ad.name}')
            results.append(self.upload_ad(api, ad))
        return results

    @staticmethod
    def upload_ad(api, ad):
        result = _new_result('Ad', ad.name, ad.adGroupId)
        if not ad.adGroupId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = f'Ad group {ad.ad_group!r} not found'
            return result
        if not ad.creativeId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = f'Creative {ad.creative!r} not found'
            return result
        if ad.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = ad.id
            return result
        _populate_reddit_result(
            result, api.create_entity(ad, entity_name='ads'))
        if result['status'] == 'created':
            ad.id = result['platform_id']
        return result


class Ad(object):
    __slots__ = ['name', 'campaign', 'ad_group', 'adGroupId',
                 'creative', 'creativeId', 'configured_status',
                 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.ad_group = None
        self.adGroupId = None
        self.creative = None
        self.creativeId = None
        self.configured_status = 'PAUSED'
        _apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.resolve_ids(self.api)
        self.upload_dict = self.create_ad_dict()

    def resolve_ids(self, api):
        if self.ad_group:
            ag = AdGroup({'name': self.ad_group,
                          'campaign': self.campaign}, api=api)
            ag.check_exists(api)
            self.adGroupId = ag.id
        if self.creative:
            cre = Creative({'name': self.creative}, api=api)
            self.creativeId = cre.id

    def create_ad_dict(self):
        if not (self.name and self.adGroupId and self.creativeId):
            return {}
        return {
            'name': str(self.name),
            'ad_group_id': str(self.adGroupId),
            'creative_id': str(self.creativeId),
            'configured_status': str(self.configured_status or 'PAUSED'),
        }

    def check_exists(self, api):
        if not api.ad_dict:
            api.set_id_dict('ad', filter_id=self.adGroupId)
        found = api.get_id(api.ad_dict, self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class Creative(object):
    """Resolve an existing Reddit creative by name. New-creative
    upload via the media + ``creatives`` chain is a follow-up."""
    __slots__ = ['name', 'id', 'api']

    def __init__(self, cre_dict, api=None):
        self.id = None
        self.name = None
        _apply_row(self, cre_dict)
        self.api = api
        if self.api:
            self.set_id(self.api)

    def set_id(self, api):
        if not api.creative_dict:
            api.set_id_dict('creative')
        if not self.name:
            return
        found = api.get_id(api.creative_dict, self.name)
        if found:
            self.id = found[0]


class CreativeUpload(utl.BaseCreativeStore):
    """Reddit creative store: filename -> uploaded-media id in
    ``reddit_creative_ids.csv``, resolved for ad creation. Per-file
    upload lives on ``RedditApi.upload_creative``; this class is the
    shared find-new / persist bookkeeping only.
    """
    id_cols = ('id',)

    def __init__(self, id_file_name='reddit_creative_ids.csv',
                 creative_path='creative/'):
        super().__init__(id_file_name, creative_path)

    def _upload_one(self, api, file_path):
        return api.upload_creative(file_path)
