import os
import sys
import time
import json
import logging
import requests
import pandas as pd
import uploader.upload.utils as utl

szk_path = 'szk'
config_path = os.path.join(utl.config_file_path, szk_path)


login_url = 'https://adapi.sizmek.com/sas/login/login/'


class SzkApi(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.username = None
        self.password = None
        self.api_key = None
        self.campaign_ids = None
        self.config_list = None
        self.headers = None
        self.adv_dict = None
        self.brd_dict = None
        self.cam_dict = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Sizmek config file: {}'.format(config))
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
        self.username = self.config['username']
        self.password = self.config['password']
        self.api_key = self.config['api_key']
        self.campaign_ids = self.config['campaign_ids']
        self.config_list = [self.config, self.username, self.password,
                            self.api_key, self.campaign_ids]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in Sizmek config file.  '
                                'Aborting.'.format(item))
                sys.exit(0)

    def set_headers(self):
        self.headers = {'api-key': self.api_key}
        data = {'username': self.username, 'password': self.password}
        r = requests.post(login_url, data=json.dumps(data),
                          headers=self.headers)
        session_id = r.json()['result']['sessionId']
        self.headers['Authorization'] = session_id

    def make_request(self, url):
        if not self.headers:
            self.set_headers()
        r = requests.get(url, headers=self.headers)
        return r

    def set_id_dict(self, szk_object='all'):
        if szk_object in ['campaign', 'brand', 'advertiser', 'all']:
            self.adv_dict = self.get_id_dict('advertisers')
        if szk_object in ['campaign', 'brand', 'all']:
            self.brd_dict = self.get_id_dict('brands', 'advertiserId')
        if szk_object in ['campaign', 'all']:
            self.cam_dict = self.get_id_dict('campaigns', 'brandId')

    def get_id_dict(self, szk_obj, parent=None):
        url = "https://adapi.sizmek.com/sas/{}?from=0&max=500".format(szk_obj)
        r = self.make_request(url)
        if parent:
            id_dict = [{x['id']: {'name': x['name'], 'parent': x[parent]}}
                       for x in r.json()['result']]
        else:
            id_dict = [{x['id']: {'name': x['name']}}
                       for x in r.json()['result']]
        return id_dict

    def get_campaign_id_dict(self):
        url = "https://adapi.sizmek.com/sas/campaigns?from=0&max=500"
        r = self.make_request(url)
        self.cam_dict = [{x['id']: {'name': x['name'],
                                    'parent': x['advertiserId']}}
                         for x in r.json()['result']]
        return self.cam_dict

    @staticmethod
    def get_id(dict_o, match, dict_two=None, match_two=None, parent_id=None):
        if parent_id:
            id_list = [k for k, v in dict_o.items() if v['name'] == match
                       and v['parent'] == parent_id]
        else:
            id_list = [k for k, v in dict_o.items() if v['name'] == match]
        if dict_two is not None:
            id_list = [k for k, v in dict_two.items() if v['name'] == match_two
                       and v['parent'] == id_list[0]]
        return id_list


class CampaignUpload(object):
    name = 'name'
    advertiser = 'advertiser'
    brand = 'brand'
    trafficking_mode = 'traffickingMode'
    hard_stop_method = 'hardStopMethod'
    target_audience_policy = 'targetAudiencePriorityPolicy'
    creative_manager_access = 'creativeManagerAccess'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='szk_campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        self.config = df.to_dict(orient='index')

    def set_campaign(self, campaign):
        cam = Campaign(self.config[campaign])
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
        campaign = self.set_campaign(campaign_id)
        if not campaign.check_exists(api):
            api.create_campaign(campaign)


class Campaign(object):
    def __init__(self, cam_dict):
        self.name = None
        self.advertiser = None
        self.brand = None
        self.traffickingMode = None
        self.hardStopMethod = None
        self.targetAudiencePriorityPolicy = None
        self.creativeManagerAccess = None
        self.parent = None
        for k in cam_dict:
            setattr(self, k, cam_dict[k])

    def create_cam_dict(self):
        cam_dict = {
            'name': '{}'.format(self.name),
            'type': '{}'.format('Campaign'),
            'brandId': '{}'.format(self.parent),
        }
        return cam_dict

    def check_exists(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        cid = api.get_id(api.cam_dict, self.name)
        if cid:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True

    def set_parent(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        self.parent = api.get_id(api.brd_dict, self.brand,
                                 api.cam_dict, self.name)[0]
