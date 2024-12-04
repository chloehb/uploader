import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import uploader.upload.utils as utl
from requests_oauthlib import OAuth2Session

dcm_path = 'dcm'
config_path = os.path.join(utl.config_file_path, dcm_path)

base_url = 'https://www.googleapis.com/dfareporting'


class DcApi(object):
    version = '4'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.usr_id = None
        self.report_id = None
        self.config_list = None
        self.client = None
        self.lp_dict = {}
        self.site_dict = {}
        self.cam_dict = {}
        self.place_dict = {}
        self.tag_dict = {}
        self.ad_dict = {}
        self.directory_site_dict = {}
        self.df = pd.DataFrame()
        self.r = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading DC config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.usr_id = self.config['usr_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url, self.usr_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DC config file.'
                                'Aborting.'.format(item))
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

    def create_url(self, entity=None):
        vers_url = '/v{}'.format(self.version)
        usr_url = '/userprofiles/{}/'.format(self.usr_id)
        full_url = (base_url + vers_url + usr_url)
        if entity:
            full_url += entity
        return full_url

    @staticmethod
    def get_id(dict_o, match, dict_two=None, match_two=None, parent_id=None,
               match_name='name', parent_name='parent'):
        if parent_id:
            id_list = [k for k, v in dict_o.items() if v[match_name] == match
                       and str(v[parent_name]) == str(parent_id)]
        else:
            id_list = [k for k, v in dict_o.items() if v[match_name] == match]
        if dict_two is not None:
            id_list = [k for k, v in dict_two.items() if v[match_name] == match_two
                       and v[parent_name] == id_list[0]]
        return id_list

    def get_id_dict(self, entity=None, parent=None, fields=None, nest=None,
                    resp_entity=None, request_filter=None,
                    request_method='get'):
        url = self.create_url(entity)
        id_dict = {}
        if request_filter:
            params = request_filter
        else:
            params = {}
        next_page = True
        next_page_token = None
        while next_page:
            if next_page_token:
                params['pageToken'] = next_page_token
            r = self.make_request(url, method=request_method, params=params)
            id_dict = self.get_dict_from_page(
                id_dict=id_dict, page=r.json(), parent=list(parent.values())[0],
                fields=list(fields.values()), nest=nest, entity=resp_entity)
            if 'nextPageToken' in r.json():
                next_page_token = r.json()['nextPageToken']
            else:
                next_page = False
        return id_dict

    @staticmethod
    def get_dict_from_page(id_dict, page, parent, fields=None, nest=None,
                           entity=None):
        resp_fields = [parent]
        if fields:
            resp_fields += fields
        if entity and entity in page:
            for x in page[entity]:
                if nest:
                    if entity == 'placementTags':
                        if nest not in x:
                            x[nest] = [{}]
                        x[nest] = x[nest][0]
                        x[nest]['id'] = x[parent]
                    x = x[nest]
                new_dict = {}
                new_key = x['id']
                for y in resp_fields:
                    dict_key = 'parent' if y == parent else y.replace('.', '')
                    if y in x:
                        dict_val = x[y]
                        new_dict[dict_key] = dict_val
                id_dict.update({new_key: new_dict})
        return id_dict

    def get_lp_id_dict(self):
        parent = {'advertiserId': 'advertiserId'}
        fields = {'id': 'id', 'url': 'url'}
        lp_dict = self.get_id_dict(entity='advertiserLandingPages',
                                   parent=parent, fields=fields,
                                   resp_entity='landingPages')
        return lp_dict

    def get_cam_id_dict(self):
        parent = {'advertiserId': 'advertiserId'}
        fields = {'id': 'id', 'name': 'name'}
        cam_dict = self.get_id_dict(entity='campaigns', parent=parent,
                                    fields=fields, resp_entity='campaigns')
        return cam_dict

    def get_place_id_dict(self, campaign_id):
        parent = {'campaignId': 'campaignId'}
        fields = {'id': 'id', 'name': 'name'}
        request_filter = {'campaignIds': campaign_id}
        place_dict = self.get_id_dict(entity='placements', parent=parent,
                                      fields=fields, resp_entity='placements',
                                      request_filter=request_filter)
        return place_dict

    def get_tag_id_dict(self, campaign_id):
        parent = {'placementId': 'placementId'}
        fields = {'clickTag': 'clickTag'}
        request_filter = {'campaignId': campaign_id}
        entity = 'placements/generatetags'
        place_dict = self.get_id_dict(
            entity=entity, parent=parent, fields=fields, nest='tagDatas',
            resp_entity='placementTags', request_filter=request_filter,
            request_method='post')
        return place_dict

    def get_site_id_dict(self):
        parent = {'accountId': 'accountId'}
        fields = {'id': 'id', 'name': 'name'}
        site_dict = self.get_id_dict(entity='sites', parent=parent,
                                     fields=fields, resp_entity='sites')
        return site_dict

    def get_directory_site_id_dict(self, name):
        entity_name = 'directorySites'
        parent = {'advertiserId': 'advertiserId'}
        fields = {'id': 'id', 'url': 'url'}
        request_filter = {'searchString': name}
        site_dict = self.get_id_dict(entity=entity_name, fields=fields,
                                     resp_entity=entity_name, parent=parent,
                                     request_filter=request_filter)
        return site_dict

    def set_id_dict(self, dcm_object=None, filter_id=None):
        if dcm_object == 'landing_page':
            self.lp_dict = self.get_lp_id_dict()
        if dcm_object == 'campaign':
            self.cam_dict = self.get_cam_id_dict()
        if dcm_object == 'placement':
            self.place_dict = self.get_place_id_dict(filter_id)
        if dcm_object == 'site':
            self.site_dict = self.get_site_id_dict()
        if dcm_object == 'directorySites':
            self.directory_site_dict = self.get_directory_site_id_dict(
                filter_id)
        if dcm_object == 'tags':
            self.tag_dict = self.get_tag_id_dict(filter_id)

    def create_entity(self, entity, entity_name=''):
        url = self.create_url(entity_name)
        r = self.make_request(url, method='post', body=entity.upload_dict)
        if 'error' in r.json():
            msg = '{} not uploaded. \n Response: {} \n Body: {}'.format(
                entity_name, r.json(), entity.upload_dict)
            logging.warning(msg)
        return r

    def make_request(self, url, method, params=None, body=None):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, params, body)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url, method, params, body)
        return self.r

    def raw_request(self, url, method, params=None, body=None):
        if not params:
            params = {}
        if body:
            if method == 'get':
                self.r = self.client.get(url, params=params, json=body)
            elif method == 'post':
                self.r = self.client.post(url, params=params, json=body)
        else:
            if method == 'get':
                self.r = self.client.get(url, params=params)
            elif method == 'post':
                self.r = self.client.post(url, params=params)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)


class CampaignUpload(object):
    name = 'name'
    advertiserId = 'advertiserId'
    defaultLandingPage = 'defaultLandingPage'
    sd = 'startDate'
    ed = 'endDate'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        df = utl.data_to_type(df, date_col=[self.sd, self.ed])
        for col in [self.sd, self.ed]:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        self.config = df.to_dict(orient='index')

    def set_campaign(self, campaign_id, api=None):
        cam = Campaign(self.config[campaign_id], api=api)
        return cam

    def upload_all_campaigns(self, api):
        total_camp = str(len(self.config))
        for idx, c_id in enumerate(self.config):
            logging.info('Uploading campaign {} of {}.  '
                         'Campaign Name: {}'.format(idx + 1, total_camp, c_id))
            self.upload_campaign(api, c_id)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    def upload_campaign(self, api, campaign_id):
        campaign = self.set_campaign(campaign_id, api)
        if not campaign.check_exists(api):
            api.create_entity(campaign, entity_name='campaigns')


class Campaign(object):
    __slots__ = ['name', 'advertiserId', 'archived', 'defaultLandingPageId',
                 'startDate', 'endDate', 'upload_dict', 'api', 'id', 'upload',
                 'defaultLandingPage']

    def __init__(self, cam_dict, api=None, upload=True):
        self.defaultLandingPageId = None
        self.id = None
        self.upload = upload
        for k in cam_dict:
            setattr(self, k, cam_dict[k])
        self.api = api
        if self.api:
            self.get_landing_page_id(self.api)
        if self.upload:
            self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        cam_dict = {
            'name': '{}'.format(self.name),
            'archived': '{}'.format('false'),
            'startDate': '{}'.format(self.startDate),
            'endDate': '{}'.format(self.endDate),
            'advertiserId': int(self.advertiserId),
            'defaultLandingPageId': int(self.defaultLandingPageId)
        }
        return cam_dict

    def get_landing_page_id(self, api):
        lp = LandingPage({'name': self.defaultLandingPage,
                          'advertiserId': self.advertiserId,
                          'url': self.defaultLandingPage}, api=api)
        self.defaultLandingPageId = lp.id

    def get_id(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        campaign_id = api.get_id(api.cam_dict, self.name)
        return campaign_id

    def set_id(self, api):
        campaign_id = self.get_id(api)
        if campaign_id:
            self.id = campaign_id[0]

    def check_exists(self, api):
        self.set_id(api)
        if self.id:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True


class LandingPage(object):
    __slots__ = ['name', 'id', 'url', 'advertiserId', 'upload_dict', 'api']

    def __init__(self, lp_dict, api=None):
        self.id = None
        for k in lp_dict:
            setattr(self, k, lp_dict[k])
        self.api = api
        self.upload_dict = self.create_lp_dict()
        if self.api:
            self.get_landing_page_id(self.api)

    def create_lp_dict(self):
        lp_dict = {
            'name': '{}'.format(self.name),
            'url': '{}'.format(self.url),
            'advertiserId': '{}'.format(self.advertiserId)
        }
        return lp_dict

    def get_landing_page_id(self, api):
        if not api.lp_dict:
            api.set_id_dict('landing_page')
        lp_ids = api.get_id(api.lp_dict, self.name,
                            parent_id=self.advertiserId, match_name='url')
        if lp_ids:
            self.id = lp_ids[0]
        else:
            logging.info('Landing page does not exist. Uploading')
            self.upload(api)

    def upload(self, api):
        logging.info('Uploading landing page with {}'.format(self.upload_dict))
        r = api.create_entity(self, entity_name='advertiserLandingPages')
        self.id = r.json()['id']


class PlacementUpload(object):
    file_name = 'adset_upload.xlsx'
    name = 'name'
    campaignId = 'campaignId'
    campaign = 'campaign'
    compatibility = 'compatibility'
    site = 'site'
    siteId = 'siteId'
    size = 'size'
    paymentSource = 'paymentSource'
    tagFormats = 'tagFormats'
    startDate = 'startDate'
    endDate = 'endDate'
    pricingType = 'pricingType'
    width = 'width'
    height = 'height'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file=''):
        if not config_file:
            config_file = self.file_name
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        df = self.format_size(df)
        df = utl.data_to_type(df, date_col=[self.startDate, self.endDate])
        for col in [self.startDate, self.endDate]:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        self.config = df.to_dict(orient='index')

    def format_size(self, df):
        if self.size not in df.columns:
            df[self.size] = '1'
        df[self.size] = df[self.size].str.split('x')
        df[self.size] = df[self.size].apply(
            lambda x: ['1', '1'] if len(x) == 1 else x)
        df[self.width] = df[self.size].apply(lambda x: x[0])
        df[self.height] = df[self.size].apply(lambda x: x[1])
        return df

    def set_placement(self, placement_id, api=None):
        placement = Placement(self.config[placement_id], api=api)
        return placement

    def upload_all_placements(self, api):
        total_placements = str(len(self.config))
        for idx, p_id in enumerate(self.config):
            placement = self.set_placement(p_id, api)
            logging.info('Uploading placement {} of {}.  '
                         'Placement Name: {}'.format(idx + 1, total_placements,
                                                     placement.name))
            self.upload_placement(api, placement)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    @staticmethod
    def upload_placement(api, placement):
        if not placement.check_exists(api):
            api.create_entity(placement, entity_name='placements')

    @staticmethod
    def generate_dcm_tags(api, campaign_id):
        """
        Grabs tags for placements with the specified campaign_id.  Returns as
        a dictionary.

        https://developers.google.com/doubleclick-advertisers/rest/v4/placements/generatetags # noqa

        :param api: instance of authenticated DcApi
        :param campaign_id: id of the campaign to pull placements for
        :return: api.tag_dict dictionary with tags and placement ids
        """
        api.set_id_dict(dcm_object='tags', filter_id=campaign_id)
        return api.tag_dict


class Placement(object):
    __slots__ = ['name', 'campaignId', 'compatibility', 'siteId', 'size',
                 'width', 'height', 'paymentSource', 'tagFormats', 'startDate',
                 'endDate', 'pricingType', 'upload_dict', 'api', 'upload',
                 'site', 'campaign']

    def __init__(self, cam_dict, api=None, upload=True):
        self.siteId = None
        self.campaignId = None
        self.upload = upload
        for k in cam_dict:
            setattr(self, k, cam_dict[k])
        self.api = api
        if self.api:
            self.get_site_id(self.api)
            self.get_campaign_id(self.api)
        if self.upload:
            self.upload_dict = self.create_p_dict()

    def create_p_dict(self):
        if not self.tagFormats:
            self.tagFormats = 'PLACEMENT_TAG_STANDARD'
        if not self.compatibility:
            self.compatibility = 'DISPLAY'
        p_dict = {
            'name': '{}'.format(self.name),
            'campaignId': int(self.campaignId),
            'compatibility': '{}'.format(self.compatibility),
            'siteId': '{}'.format(self.siteId),
            'size': {
                'height': '{}'.format(self.height),
                'width': '{}'.format(self.width)
            },
            'paymentSource': '{}'.format(self.paymentSource),
            'tagFormats': ['{}'.format(self.tagFormats)],
            'pricingSchedule': {
                'startDate': '{}'.format(self.startDate),
                'endDate': '{}'.format(self.endDate),
                'pricingType': '{}'.format('PRICING_TYPE_CPM')
            },
        }
        return p_dict

    def get_site_id(self, api):
        site = Site({'name': '{}'.format(self.site)}, api=api)
        self.siteId = site.id

    def check_exists(self, api):
        if not api.place_dict:
            api.set_id_dict(dcm_object='placement', filter_id=self.campaignId)
        pid = api.get_id(api.place_dict, self.name)
        if pid:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True

    def get_campaign_id(self, api):
        campaign = Campaign({'name': self.campaign}, upload=False)
        campaign.set_id(api)
        self.campaignId = campaign.id


class DirectorySite(object):
    __slots__ = ['name', 'id', 'api', 'upload_dict', 'url']

    def __init__(self, lp_dict, api=None):
        self.id = None
        for k in lp_dict:
            setattr(self, k, lp_dict[k])
        self.url = ''
        self.api = api
        self.upload_dict = self.create_site_dict()
        if self.api:
            self.get_landing_page_id(self.api)

    def create_site_dict(self):
        url = self.name.lower().replace(' ', '')
        self.url = """https://www.{}.com""".format(url)
        site_dict = {
            'name': '{}'.format(self.name),
            'url': self.url
        }
        return site_dict

    def get_landing_page_id(self, api):
        api.set_id_dict('directorySites',
                        filter_id=self.url.replace('https:', ''))
        url_types = [self.url, self.url.replace('https', 'http')]
        site_ids = []
        for url_type in url_types:
            site_ids = api.get_id(api.directory_site_dict, url_type,
                                  match_name='url')
            if site_ids:
                break
        if site_ids:
            self.id = site_ids[0]
        else:
            logging.info('Directory site does not exist. Uploading')
            self.upload(api)

    def upload(self, api):
        logging.info('Uploading directory site {}'.format(self.upload_dict))
        r = api.create_entity(self, entity_name='directorySites')
        self.id = r.json()['id']


class Site(object):
    __slots__ = ['name', 'id', 'api', 'upload_dict']

    def __init__(self, lp_dict, api=None):
        self.id = None
        for k in lp_dict:
            setattr(self, k, lp_dict[k])
        self.api = api
        self.upload_dict = self.create_site_dict()
        if self.api:
            self.get_landing_page_id(self.api)

    def create_site_dict(self):
        site_dict = {
            'name': '{}'.format(self.name),
        }
        return site_dict

    def get_landing_page_id(self, api):
        if not api.site_dict:
            api.set_id_dict('site')
        site_ids = api.get_id(api.site_dict, self.name)
        if site_ids:
            self.id = site_ids[0]
        else:
            logging.info('Site does not exist. Uploading')
            self.upload(api)

    def upload(self, api):
        logging.info('Uploading site with {}'.format(self.upload_dict))
        ds = DirectorySite(self.upload_dict, self.api)
        self.upload_dict['directorySiteId'] = ds.id
        r = api.create_entity(self, entity_name='sites')
        self.id = r.json()['id']


class AdUpload(object):
    active = 'active'
    campaignId = 'campaignId'
    creativeRotation = 'creativeRotation'
    deliverySchedule = 'deliverySchedule'
    endTime = 'endTime'
    startTime = 'startTime'
    type = 'type'
    placementAssignments = 'placementAssignments'
    creative = 'creative'

    def __init__(self):
        pass


class Asset(object):
    name = 'name'
    type = 'asset_type'

    def __init__(self, name, asset_type):
        self.name = name
        self.asset_type = asset_type
        self.upload_dict = self.create_upload_dict()

    def create_upload_dict(self):
        upload_dict = {'assetIdentifier': {'name': self.name,
                                           'type': self.type}}
        return upload_dict

    def upload(self, api):
        logging.info('Uploading asset with {}'.format(self.upload_dict))
        r = api.create_enity(self, entity='creativeAssets')
        self.id = r.json()['id']
