import os
import sys
import yaml
import time
import uuid
import logging
import numpy as np
import pandas as pd
import uploader.utils as utl
from googleads import adwords

config_path = utl.config_file_path


class AwApi(object):
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
        self.adwords_client = (adwords.AdWordsClient.
                               LoadFromStorage(self.configfile))

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

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AW config file.'.format(item))
                sys.exit(0)

    @staticmethod
    def get_operation(operand, operator='ADD'):
        operation = [{
            'operator': operator,
            'operand': x
        } for x in operand]
        return operation

    def mutate_service(self, service, operator):
        svc = self.get_service(service)
        operation = self.get_operation(operator)
        resp = svc.mutate(operation)
        return resp

    def get_service(self, service):
        svc = self.adwords_client.GetService(service, version=self.v)
        return svc

    def get_id_dict(self, service='CampaignService', parent=None, page_len=100,
                    fields=None, nest=None):
        svc = self.get_service(service)
        id_dict = {}
        start_index = 0
        selector_fields = ['Id', 'Status']
        [selector_fields.extend(list(x.keys())) for x in [fields, parent] if x]
        selector = {'fields': selector_fields,
                    'paging': {'startIndex': '{}'.format(start_index),
                               'numberResults': '{}'.format(page_len)}}
        more_pages = True
        while more_pages:
            page = svc.get(selector)
            id_dict = self.get_dict_from_page(id_dict, page,
                                              list(parent.values())[0],
                                              list(fields.values()), nest)
            start_index += page_len
            selector['paging']['startIndex'] = str(start_index)
            more_pages = start_index < int(page['totalNumEntries'])
        return id_dict

    @staticmethod
    def get_dict_from_page(id_dict, page, parent, fields=None, nest=None):
        resp_fields = [parent]
        if fields:
            resp_fields += fields
        if nest:
            id_dict.update({x[nest]['id']: {'parent' if y == parent else
                                            y.replace('.', ''):
                                            x[nest][y] if y in x[nest] else
                                            x[y] for y in resp_fields}
                            for x in page['entries'] if 'entries' in page})
        else:
            id_dict.update({x['id']: {'parent' if y == parent else
                                      y.replace('.', ''):
                                      x[y] for y in resp_fields}
                            for x in page['entries'] if 'entries' in page})
        return id_dict

    def set_budget(self, name, budget, method):
        budget = {
            'name': '{}-{}'.format(name, uuid.uuid4()),
            'amount': {
                'microAmount': '{}'.format(budget * 1000000)
            },
            'deliveryMethod': '{}'.format(method)
        }
        resp = self.mutate_service('BudgetService', [budget])
        budget_id = resp['value'][0]['budgetId']
        return budget_id

    def get_campaign_id_dict(self):
        parent = {'BaseCampaignId': 'baseCampaignId'}
        fields = {'Name': 'name'}
        cam_dict = self.get_id_dict(service='CampaignService', parent=parent,
                                    fields=fields)
        return cam_dict

    def get_adgroup_id_dict(self):
        parent = {'CampaignId': 'campaignId'}
        fields = {'Name': 'name'}
        ag_dict = self.get_id_dict(service='AdGroupService', parent=parent,
                                   fields=fields)
        return ag_dict

    def get_ad_dict(self):
        parent = {'AdGroupId': 'adGroupId'}
        fields = {'HeadlinePart1': 'headlinePart1', 'UrlData': 'urlData',
                  'HeadlinePart2': 'headlinePart2',
                  'Description': 'description',
                  'ExpandedTextAdHeadlinePart3': 'headlinePart3',
                  'ExpandedTextAdDescription2': 'description2',
                  'CreativeTrackingUrlTemplate': 'trackingUrlTemplate',
                  'CreativeFinalUrls': 'finalUrls', 'DisplayUrl': 'displayUrl',
                  'AdType': 'Ad.Type'}
        ad_dict = self.get_id_dict(service='AdGroupAdService',
                                   parent=parent, fields=fields, nest='ad')
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
        if parent_id:
            id_list = [k for k, v in dict_o.items() if v['name'] == match
                       and v['parent'] == parent_id]
        else:
            id_list = [k for k, v in dict_o.items() if v['name'] == match]
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

    def create_campaign(self, campaign, service='CampaignService'):
        budget_id = self.set_budget(campaign.name, campaign.budget,
                                    campaign.deliveryMethod)
        campaign.cam_dict['budget'] = {
                'budgetId': budget_id
            }
        campaigns = self.mutate_service(service, [campaign.cam_dict])
        campaign.id = campaigns['value'][0]['id']
        self.add_targets(campaign, service='CampaignCriterionService',
                         positive='CampaignCriterion',
                         negative='NegativeCampaignCriterion',
                         id_name='campaignId')
        return campaigns

    def create_adgroup(self, ag, service='AdGroupService'):
        ad_groups = self.mutate_service(service, [ag.operand])
        ag.id = ad_groups['value'][0]['id']
        self.add_targets(ag)
        return ad_groups

    def add_targets(self, aw_object, service='AdGroupCriterionService',
                    positive='BiddableAdGroupCriterion',
                    negative='NegativeAdGroupCriterion', id_name='adGroupId'):
        targets = [{'xsi_type': positive, 'dict': aw_object.target_dict},
                   {'xsi_type': negative,
                    'dict': aw_object.negative_target_dict}]
        for target in targets:
            if target['dict']:
                target = [{'xsi_type': target['xsi_type'],
                           id_name: aw_object.id,
                           'criterion': x} for x in target['dict']]
                self.mutate_service(service, target)

    def create_ad(self, ad):
        ads = self.mutate_service('AdGroupAdService', [ad.operand])
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
        for col in [self.sd, self.ed]:
            df[col] = df[col].dt.strftime('%Y%m%d')
        self.config = df.to_dict(orient='index')
        for k in self.config:
            for item in [self.freq, self.network, self.strategy]:
                self.config[k][item] = self.config[k][item].split('|')

    def apply_targets(self, df):
        targets = [self.language, self.location, self.platform]
        df = TargetConfig().load_targets(df, targets)
        return df

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
    __slots__ = ['name', 'status', 'startDate', 'endDate', 'budget',
                 'deliveryMethod', 'frequencyCap', 'advertisingChannelType',
                 'advertisingChannelSubType', 'networkSetting',
                 'biddingStrategy', 'settings', 'id', 'cam_dict', 'location',
                 'language', 'platform', 'target_dict', 'negative_target_dict']

    def __init__(self, cam_dict):
        for k in cam_dict:
            setattr(self, k, cam_dict[k])
        self.frequencyCap = self.set_freq(self.frequencyCap)
        self.networkSetting = self.set_net(self.networkSetting)
        self.biddingStrategy = self.set_strat(self.biddingStrategy)
        self.cam_dict = self.create_cam_dict()

    def create_cam_dict(self):
        cam_dict = {
            'name': '{}'.format(self.name),
            'status': '{}'.format(self.status),
            'advertisingChannelType': '{}'.format(self.advertisingChannelType),
            'biddingStrategyConfiguration': self.biddingStrategy,
            'endDate': '{}'.format(self.endDate),
            'networkSetting': self.networkSetting,
        }
        params = [(self.startDate, 'startDate'), (self.settings, 'settings'),
                  (self.frequencyCap, 'frequencyCap', 'dict'),
                  (self.advertisingChannelSubType, 'advertisingChannelSubType')]
        for param in params:
            if param[0]:
                if len(param) == 3:
                    cam_dict[param[1]] = param[0]
                else:
                    cam_dict[param[1]] = '{}'.format(param[0])
        return cam_dict

    @staticmethod
    def set_freq(freq):
        if freq:
            freq = {
                'impressions': freq[0],
                'timeUnit': freq[1],
                'level': freq[2]
            }
        return freq

    @staticmethod
    def set_net(network):
        net_dict = {
            'targetGoogleSearch': 'false',
            'targetSearchNetwork': 'false',
            'targetContentNetwork': 'false',
            'targetPartnerSearchNetwork': 'false'
        }
        if network:
            for net in network:
                net_dict[net] = 'true'
        return net_dict

    @staticmethod
    def set_strat(strategy):
        strat_dict = {
            'biddingStrategyType': strategy[0]
        }
        if len(strategy) == 1:
            strat_dict['bid'] = {'microAmount': strategy[1]}
        return strat_dict

    def check_exists(self, api):
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
        total_ag = str(len(self.config))
        for idx, ag_id in enumerate(self.config):
            logging.info('Uploading adgroup {} of {}.  '
                         'Adgroup Name: {}'.format(idx + 1, total_ag, ag_id))
            self.upload_adgroup(api, ag_id)
        logging.info('Pausing for 30s while ad groups finish uploading.')
        time.sleep(30)

    def upload_adgroup(self, api, ag_id):
        ag = self.set_adgroup(ag_id)
        if not ag.check_exists(api):
            api.create_adgroup(ag)


class TargetConfig(object):
    def __init__(self, target_file='aw_adgroup_target_upload.xlsx', df=None):
        self.target_file = target_file
        self.df = df
        self.target_dict = {
            AdGroupUpload.keyword: {
                'fnc': Target.format_keywords},
            AdGroupUpload.placement: {
                'fnc': Target.format_placement,
                'api_name': 'Placement', 'api_id': 'url'},
            AdGroupUpload.topic: {
                'map_file': 'config/aw_verticals.csv',
                'api_name': 'Vertical',
                'api_id': 'verticalId'},
            AdGroupUpload.affinity: {
                'map_file': 'config/aw_affinity.csv',
                'api_name': 'CriterionUserInterest',
                'api_id': 'userInterestId'},
            AdGroupUpload.in_market: {
                'map_file': 'config/aw_inmarket.csv',
                'api_name': 'CriterionUserInterest',
                'api_id': 'userInterestId'},
            AdGroupUpload.age_range: {
                'map_file': 'config/aw_ages.csv',
                'map_name': 'Age range',
                'api_name': 'AgeRange'},
            AdGroupUpload.gender: {
                'map_file': 'config/aw_genders.csv',
                'map_name': 'Gender',
                'api_name': 'Gender'},
            CampaignUpload.language: {
                'map_file': 'config/aw_languagecodes.csv',
                'map_name': 'Language name',
                'api_name': 'Language'},
            CampaignUpload.location: {
                'map_file': 'config/aw_locations.csv',
                'map_name': 'Canonical Name',
                'map_id': 'Criteria ID',
                'api_name': 'Location'},
            CampaignUpload.platform: {
                'map_file': 'config/aw_platforms.csv',
                'map_name': 'Platform name',
                'api_name': 'Platform'}}
        if self.target_file:
            self.load_file()

    def load_file(self):
        self.df = pd.read_excel(os.path.join(config_path, self.target_file))
        self.df = self.df.fillna('')

    def load_targets(self, upload_df, target_names, negative_target_names=None):
        if not negative_target_names:
            negative_target_names = []
        for target_name in target_names + negative_target_names:
            params = self.target_dict[target_name]
            target = Target(target_name, target_dict=params, df=self.df)
            upload_df = target.format_target(upload_df)
        upload_df = self.combine_target(upload_df, target_names, 'target_dict')
        upload_df = self.combine_target(upload_df, negative_target_names,
                                        'negative_target_dict')
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
                target_map[t] = [{'xsi_type': self.target_type,
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
                 'age_range', 'gender', 'affinity', 'in_market']

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
        self.id = None
        self.cid = None
        self.operand = None
        self.parent = None
        for k in ag_dict:
            setattr(self, k, ag_dict[k])
        self.ag_dict = self.create_adgroup_dict()
        # self.target_dict, self.negative_target_dict = self.create_target_dict()
        if self.parent:
            self.set_operand()

    def create_adgroup_dict(self):
        bids = [{'xsi_type': self.bid_type,
                 'bid': {'microAmount': '{}'.format(self.bid * 1000000)}, }]
        ag_dict = {
          'name': '{}'.format(self.name),
          'status': '{}'.format(self.status),
          'biddingStrategyConfiguration': {
              'bids': bids
          }
        }
        return ag_dict

    def create_target_dict(self):
        target = []
        negative_target = []
        if not str(self.keyword) == 'nan':
            target.extend({'xsi_type': 'Keyword',
                           'matchType': x[0],
                           'text': x[1]}
                          for x in self.keyword if x and x != [''])
        targets = [(self.topic, 'Vertical', 'verticalId'),
                   (self.placement, 'Placement', 'url'),
                   (self.affinity, 'CriterionUserInterest', 'userInterestId'),
                   (self.in_market, 'CriterionUserInterest', 'userInterestId')]
        target = self.format_target(target, targets)
        negative_targets = [(self.gender, 'Gender', 'id'),
                            (self.age_range, 'AgeRange', 'id')]
        negative_target = self.format_target(negative_target, negative_targets)
        return target, negative_target

    @staticmethod
    def format_target(target, target_list):
        for tar in target_list:
            if not str(tar[0]) == 'nan':
                target.extend({'xsi_type': tar[1], tar[2]: x}
                              for x in tar[0] if x and x != [''])
        return target

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
        self.parent = parent_list[0]

    def set_operand(self, api=None):
        if api:
            self.set_parent(api)
        self.operand = self.ag_dict
        self.operand['campaignId'] = '{}'.format(self.parent)


class AdUpload(object):
    ag_name = 'adGroupName'
    cam_name = 'campaignName'
    type = 'AdType'
    headline1 = 'headlinePart1'
    headline2 = 'headlinePart2'
    headline3 = 'headlinePart3'
    description = 'description'
    description2 = 'description2'
    final_url = 'finalUrls'
    track_url = 'trackingUrlTemplate'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_ad_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.ag_name])
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
        for col in [self.final_url, self.track_url]:
            df[col] = np.where(df[col].str[:4] != 'http',
                               'http://' + df[col], df[col])
        return df

    def upload_all_ads(self, api):
        total_ad = str(len(self.config))
        for idx, ad_id in enumerate(self.config):
            logging.info('Uploading ad {} of {}.  '
                         'Ad Row: {}'.format(idx + 1, total_ad, ad_id + 2))
            self.upload_ad(api, ad_id)
        logging.info('Pausing for 30s while ads finish uploading.')
        time.sleep(30)

    def upload_ad(self, api, ad_id):
        ad = self.set_ad(ad_id)
        if not ad.check_exists(api):
            api.create_ad(ad)


class Ad(object):
    def __init__(self, ad_dict):
        self.adGroupName = None
        self.campaignName = None
        self.headlinePart1 = None
        self.headlinePart2 = None
        self.headlinePart3 = None
        self.description = None
        self.description2 = None
        self.finalUrls = None
        self.trackingUrlTemplate = None
        self.urlData = None
        self.displayUrl = None
        self.AdType = None
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

    def create_ad_dict(self):
        ad_dict = {
                'xsi_type': '{}'.format(self.AdType),
                'finalUrls': self.finalUrls,
                'trackingUrlTemplate': '{}'.format(self.trackingUrlTemplate)
        }
        if self.AdType == 'ExpandedTextAd':
            ad_dict['headlinePart1'] = '{}'.format(self.headlinePart1)
            ad_dict['headlinePart2'] = '{}'.format(self.headlinePart2)
            ad_dict['description'] = '{}'.format(self.description)
            if self.headlinePart3:
                ad_dict['headlinePart3'] = '{}'.format(self.headlinePart3)
            if self.description2:
                ad_dict['description2'] = '{}'.format(self.description2)
        return ad_dict

    def check_exists(self, api):
        self.set_operand(api)
        if self in [Ad(api.ad_dict[x]) for x in api.ad_dict]:
            logging.warning('Ad already in account and not uploaded.  '
                            'Operator as follows: \n {}.'.format(self.operand))
            return True

    def set_parent(self, api):
        if not api.ad_dict:
            api.set_id_dict('all')
        self.parent = api.get_id(api.cam_dict, self.campaignName,
                                 api.ag_dict, self.adGroupName)[0]

    def set_operand(self, api=None):
        if api:
            self.set_parent(api)
        self.operand = {
            'xsi_type': 'AdGroupAd',
            'adGroupId': self.parent,
            'ad': self.ad_dict,
        }
