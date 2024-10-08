import os
import sys
import yaml
import time
import uuid
import logging
import numpy as np
import pandas as pd
import uploader.upload.utils as utl
# from googleads import adwords

aw_path = 'aw'
config_path = os.path.join(utl.config_file_path, aw_path)


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

    def mutate_service(self, service, operand, operator='ADD'):
        svc = self.get_service(service)
        operation = self.get_operation(operand, operator)
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
        id_dict.update({x[nest]['id'] if nest else x['id']:
                       {'parent' if y == parent else y.replace('.', ''):
                        x[nest][y] if nest and y in x[nest] else x[y]
                        for y in resp_fields
                        if y in x or (nest and y in x[nest])}
                        for x in page['entries'] if 'entries'})
        return id_dict

    def set_budget(self, name, budget, method):
        budget = {
            'name': '{}-{}'.format(name, uuid.uuid4()),
            'amount': {
                'microAmount': '{}'.format(int(budget * 1000000))
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
                  'AdType': 'Ad.Type', 'MarketingImage': 'marketingImage',
                  'ShortHeadline': 'shortHeadline',
                  'LongHeadline': 'longHeadline',
                  'BusinessName': 'businessName',
                  'MediaId': 'image', 'ImageCreativeName': 'name'}
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
        tot_ag = str(len(self.config))
        for idx, ag_id in enumerate(self.config):
            logging.info('Uploading adgroup {} of {}.'.format(idx + 1, tot_ag))
            self.upload_adgroup(api, ag_id)
        logging.info('Pausing for 30s while ad groups finish uploading.')
        time.sleep(30)

    def upload_adgroup(self, api, ag_id):
        ag = self.set_adgroup(ag_id)
        logging.info('Adgroup name: {}'.format(ag.name))
        if not ag.check_exists(api):
            api.create_adgroup(ag)


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

    def upload_all_creatives(self, api):
        creatives = set(self.config[x][self.marketing_image] for x in
                        self.config if self.config[x][self.marketing_image])
        img_cre = set(self.config[x][self.image] for x in
                      self.config if self.config[x][self.image])
        creatives = creatives.union(img_cre)
        cu = CreativeUpload()
        cu.upload_all_creatives(api, creatives)
        self.creative_filename_to_id(cu.config)
        return cu

    def creative_filename_to_id(self, table):
        for k in self.config:
            for img in [self.marketing_image, self.image]:
                if self.config[k][img]:
                    media_id = (table[table[CreativeUpload.file_name] ==
                                      self.config[k][img]]
                                [CreativeUpload.media_id].values[0])
                    self.config[k][img] = media_id

    def upload_all_ads(self, api):
        cu = self.upload_all_creatives(api)
        total_ad = str(len(self.config))
        for idx, ad_id in enumerate(self.config):
            logging.info('Uploading ad {} of {}.  '
                         'Ad Row: {}'.format(idx + 1, total_ad, ad_id + 2))
            self.upload_ad(api, ad_id, cu)
        logging.info('Pausing for 30s while ads finish uploading.')
        time.sleep(30)

    def upload_ad(self, api, ad_id, cu):
        ad = self.set_ad(ad_id)
        if not ad.check_exists(api, cu):
            api.create_ad(ad)


class Ad(object):
    def __init__(self, ad_dict, cu=None):
        self.cu = cu
        self.adGroupName = None
        self.campaignName = None
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
        if self.image and self.cu:
            self.image['mediaId'] = \
                (self.cu.config[self.cu.config['referenceId'] ==
                                self.image['referenceId']]['mediaId'].values[0])

    def create_ad_dict(self):
        self.set_media_id_from_ref()
        if self.marketingImage and not str(self.marketingImage).isdigit():
            self.marketingImage = self.marketingImage['mediaId']
        if self.image and not str(self.image).isdigit():
            self.image = self.image['mediaId']
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
        return ad_dict

    def check_exists(self, api, cu):
        self.set_operand(api)
        if self in [Ad(api.ad_dict[x], cu) for x in api.ad_dict]:
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


class CreativeUpload(object):
    id_file_path = 'creative/'
    file_name = 'file_name'
    media_id = 'mediaId'
    reference_id = 'referenceId'

    def __init__(self, config=None, id_file_name='aw_creative_ids.csv'):
        self.config = config
        self.id_file_name = os.path.join(self.id_file_path, id_file_name)
        if self.id_file_name:
            self.config = self.load_config()

    def load_config(self):
        self.config = pd.read_csv(self.id_file_name)
        return self.config

    def upload_all_creatives(self, api, full_creative_list):
        creatives = [x for x in full_creative_list
                     if x not in list(self.config[self.file_name].values)]
        total_creative = len(creatives)
        for idx, creative in enumerate(creatives):
            logging.info('Uploading creative {} of {}.  Creative Name: '
                         '{}'.format(idx+1, total_creative, creative))
            full_creative = os.path.join('creative/', creative)
            if os.path.isfile(full_creative):
                resp = self.upload_creative(api, full_creative)
                cre_dict = {self.media_id: [resp[0][self.media_id]],
                            self.reference_id: [resp[0][self.reference_id]],
                            self.file_name: [creative]}
                self.config = self.config.append(pd.DataFrame(cre_dict))
                self.config = self.config.reset_index(drop=True)
            else:
                logging.warning('{} not found.  '
                                'It was not uploaded'.format(creative))
        self.write_df_to_csv()

    @staticmethod
    def upload_creative(api, filename):
        with open(filename, 'rb') as image_handle:
            image_data = image_handle.read()
        if filename.split('.')[1] == '.zip':
            media = [{
                'xsi_type': 'MediaBundle',
                'data': image_data,
                'type': 'MEDIA_BUNDLE'
            }]
        else:
            media = {
              'type': 'IMAGE',
              'data': image_data,
              'xsi_type': 'Image'
            }
        svc = api.get_service('MediaService')
        resp = svc.upload(media)
        return resp

    @staticmethod
    def dict_to_df(dictionary, first_col, second_col):
        df = pd.Series(dictionary, name=second_col)
        df.index.name = first_col
        df = df.reset_index()
        return df

    def write_df_to_csv(self):
        try:
            self.config.to_csv(self.id_file_name, index=False)
        except IOError:
            logging.warning('{} could not be opened. This dictionary was not '
                            'saved.'.format(self.id_file_name))
