import os
import sys
import time
import json
import logging
import itertools
import numpy as np
import pandas as pd
import uploader.upload.utils as utl
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.targeting import Targeting
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.adobjects.targetingsearch import TargetingSearch
from facebook_business.adobjects.adcreativelinkdata import AdCreativeLinkData
from facebook_business.adobjects.adcreativeobjectstoryspec \
    import AdCreativeObjectStorySpec
from facebook_business.adobjects.adcreativevideodata \
    import AdCreativeVideoData


fb_path = 'fb'
config_path = os.path.join(utl.config_file_path, fb_path)
log = logging.getLogger()


class FbApi(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.df = pd.DataFrame()
        self.config = None
        self.account = None
        self.campaign = None
        self.app_id = None
        self.app_secret = None
        self.access_token = None
        self.act_id = None
        self.config_list = []
        self.date_lists = None
        self.field_lists = None
        self.adset_dict = None
        self.cam_dict = None
        self.ad_dict = None
        self.pixel = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config_file):
        logging.info('Loading Facebook config file: ' + str(config_file))
        self.config_file = os.path.join(config_path, config_file)
        self.load_config()
        self.check_config()
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        self.account = AdAccount(self.config['act_id'])

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(self.config_file + ' not found.  Aborting.')
            sys.exit(0)
        self.app_id = self.config['app_id']
        self.app_secret = self.config['app_secret']
        self.access_token = self.config['access_token']
        self.act_id = self.config['act_id']
        self.config_list = [self.app_id, self.app_secret, self.access_token,
                            self.act_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(item + 'not in FB config file.  Aborting.')
                sys.exit(0)

    def set_id_name_dict(self, fb_object):
        if fb_object == Campaign:
            fields = ['id', 'name']
            self.cam_dict = list(self.account.get_campaigns(fields=fields))
        elif fb_object == AdSet:
            fields = ['id', 'name', 'campaign_id']
            self.adset_dict = list(self.account.get_ad_sets(fields=fields))
        elif fb_object == Ad:
            fields = ['id', 'name', 'campaign_id', 'adset_id']
            self.ad_dict = list(self.account.get_ads(fields=fields))

    def campaign_to_id(self, campaigns):
        if not self.cam_dict:
            self.set_id_name_dict(Campaign)
        cids = [x['id'] for x in self.cam_dict if x['name'] in campaigns]
        return cids

    def adset_to_id(self, adsets, cids):
        as_and_cam = list(itertools.product(adsets, cids))
        if not self.adset_dict:
            self.set_id_name_dict(AdSet)
        asids = [tuple([x['id'], x['campaign_id']]) for x in self.adset_dict
                 if tuple([x['name'], x['campaign_id']]) in as_and_cam]
        return asids

    def create_campaign(self, campaign_name, objective, status, spend_cap):
        if not self.cam_dict:
            self.set_id_name_dict(Campaign)
        if campaign_name in ([x['name'] for x in self.cam_dict]):
            logging.warning(campaign_name + ' already in account.  This ' +
                                            'campaign was not uploaded.')
            return None
        self.campaign = Campaign(parent_id=self.account.get_id_assured())
        self.campaign.update({
            Campaign.Field.name: campaign_name,
            Campaign.Field.objective: objective,
            Campaign.Field.effective_status: status,
            Campaign.Field.spend_cap: int(spend_cap),
            Campaign.Field.special_ad_categories: 'NONE'
        })
        self.campaign.remote_create()

    @staticmethod
    def geo_target_search(geos):
        all_geos = []
        for geo in geos:
            params = {
                'q': geo,
                'type': 'adgeolocation',
                'location_types': [Targeting.Field.country],
            }
            resp = TargetingSearch.search(params=params)
            all_geos.extend(resp)
        return all_geos

    @staticmethod
    def target_search(targets_to_search):
        all_targets = []
        for target in targets_to_search[1]:
            params = {
                'q': target,
                'type': 'adinterest',
            }
            resp = TargetingSearch.search(params=params)
            if not resp:
                logging.warning(target + ' not found in targeting search.  ' +
                                'It was not added to the adset.')
                continue
            if targets_to_search[0] == 'interest':
                resp = [resp[0]]
            new_tar = [dict((k, x[k]) for k in ('id', 'name')) for x in resp]
            all_targets.extend(new_tar)
        return all_targets

    @staticmethod
    def get_matching_saved_audiences(audiences):
        aud_list = []
        for audience in audiences:
            audience = CustomAudience(audience)
            val_aud = audience.remote_read(fields=['targeting'])
            aud_list.append(val_aud)
            aud_list = aud_list[0]['targeting']
        return aud_list

    def get_matching_custom_audiences(self, audiences):
        act_auds = self.account.get_custom_audiences(
                   fields=[CustomAudience.Field.name, CustomAudience.Field.id])
        audiences = [{'id': x['id'], 'name': x['name']} for x in act_auds
                     if x['id'] in audiences]
        return audiences

    def set_target(self, geos, targets, age_min, age_max, gender, device,
                   publisher_platform, facebook_positions):
        targeting = {}
        if geos and geos != ['']:
            targeting[Targeting.Field.geo_locations] = {Targeting.Field.
                                                        countries: geos}
        if age_min:
            targeting[Targeting.Field.age_min] = age_min
        if age_max:
            targeting[Targeting.Field.age_max] = age_max
        if gender:
            targeting[Targeting.Field.genders] = gender
        if device and device != ['']:
            targeting[Targeting.Field.device_platforms] = device
        if publisher_platform and publisher_platform != ['']:
            targeting[Targeting.Field.publisher_platforms] = publisher_platform
        if facebook_positions and facebook_positions != ['']:
            targeting[Targeting.Field.facebook_positions] = facebook_positions
        for target in targets:
            if target[0] == 'interest' or target[0] == 'interest-broad':
                int_targets = self.target_search(target)
                targeting[Targeting.Field.interests] = int_targets
            if target[0] == 'savedaudience':
                aud_target = self.get_matching_saved_audiences(target[1])
                targeting.update(aud_target)
            if target[0] == 'customaudience':
                aud_target = self.get_matching_custom_audiences(target[1])
                targeting[Targeting.Field.custom_audiences] = aud_target
        return targeting

    def create_adset(self, adset_name, cids, opt_goal, bud_type, bud_val,
                     bill_evt, bid_amt, status, start_time, end_time, prom_obj,
                     country, target, age_min, age_max, genders, device, pubs,
                     pos):
        if not self.adset_dict:
            self.set_id_name_dict(AdSet)
        for cid in cids:
            if adset_name in ([x['name'] for x in self.adset_dict
                               if x['campaign_id'] == cid]):
                logging.warning(adset_name + ' already in campaign.  This ' +
                                'ad set was not uploaded.')
                continue
            targeting = self.set_target(country, target, age_min, age_max,
                                        genders, device, pubs, pos)
            params = {
                AdSet.Field.name: adset_name,
                AdSet.Field.campaign_id: cid,
                AdSet.Field.billing_event: bill_evt,
                AdSet.Field.status: status,
                AdSet.Field.targeting: targeting,
                AdSet.Field.start_time: start_time,
                AdSet.Field.end_time: end_time,
            }
            if bid_amt == '':
                params['bid_strategy'] = 'LOWEST_COST_WITHOUT_CAP'
            else:
                params[AdSet.Field.bid_amount] = int(bid_amt)
            if opt_goal in ['CONTENT_VIEW', 'SEARCH', 'ADD_TO_CART',
                            'ADD_TO_WISHLIST', 'INITIATED_CHECKOUT',
                            'ADD_PAYMENT_INFO', 'PURCHASE', 'LEAD',
                            'COMPLETE_REGISTRATION', 'OFFSITE_CONVERSIONS']:
                if not self.pixel:
                    pixel = self.account.get_ads_pixels()
                    self.pixel = pixel[0]['id']
                params[AdSet.Field.promoted_object] = {'pixel_id': self.pixel,
                                                       'custom_event_type':
                                                       opt_goal,
                                                       'page_id': prom_obj}
            elif opt_goal == 'APP_INSTALLS':
                opt_goal = opt_goal.split('|')
                params[AdSet.Field.promoted_object] = {
                    'application_id': opt_goal[1],
                    'object_store_url': opt_goal[2],
                        }
            else:
                params[AdSet.Field.optimization_goal] = opt_goal
                params[AdSet.Field.promoted_object] = {'page_id': prom_obj}
            if bud_type == 'daily':
                params[AdSet.Field.daily_budget] = int(bud_val)
            elif bud_type == 'lifetime':
                params[AdSet.Field.lifetime_budget] = int(bud_val)
            self.account.create_ad_set(params=params)

    def upload_creative(self, creative_class, image_path):
        cre = creative_class(parent_id=self.account.get_id_assured())
        if creative_class == AdImage:
            cre[AdImage.Field.filename] = image_path
            cre.remote_create()
            creative_hash = cre.get_hash()
        elif creative_class == AdVideo:
            cre[AdVideo.Field.filepath] = image_path
            cre.remote_create()
            creative_hash = cre.get_id()
        else:
            creative_hash = None
        return creative_hash

    def get_all_thumbnails(self, vid):
        video = AdVideo(vid)
        thumbnails = video.get_thumbnails()
        if not thumbnails:
            logging.warning('Could not retrieve thumbnail for vid: ' +
                            str(vid) + '.  Retrying in 120s.')
            time.sleep(120)
            thumbnails = self.get_all_thumbnails(vid)
        return thumbnails

    def get_video_thumbnail(self, vid):
        thumbnails = self.get_all_thumbnails(vid)
        thumbnail = [x for x in thumbnails if x['is_preferred'] is True]
        if not thumbnail:
            thumbnail = thumbnails[1]
        else:
            thumbnail = thumbnail[0]
        thumb_url = thumbnail['uri']
        return thumb_url

    @staticmethod
    def request_error(e):
        continue_running = True
        if e._api_error_code == 2:
            logging.warning('Retrying as the call resulted in the following: '
                            + str(e))
        elif e._api_error_code == 100:
            logging.warning('Error: {}'.format(e))
            continue_running = False
        else:
            logging.error('Retrying in 120 seconds as the Facebook API call'
                          'resulted in the following error: ' + str(e))
            time.sleep(120)
        return continue_running

    def create_ad(self, ad_name, asids, title, body, desc, cta, durl, url,
                  prom_obj, ig_id, view_tag, ad_status, creative_hash=None,
                  vid_id=None):
        if not self.ad_dict:
            self.set_id_name_dict(Ad)
        for asid in asids:
            if ad_name in [x['name'] for x in self.ad_dict
                           if x['campaign_id'] == asid[1]
                           and x['adset_id'] == asid[0]]:
                logging.warning(ad_name + ' already in campaign/adset. ' +
                                'This ad was not uploaded.')
                continue
            if vid_id:
                params = self.get_video_ad_params(ad_name, asid, title, body,
                                                  desc, cta, url, prom_obj,
                                                  ig_id, creative_hash, vid_id,
                                                  view_tag, ad_status)
            elif isinstance(creative_hash, list):
                params = self.get_carousel_ad_params(ad_name, asid, title,
                                                     body, desc, cta, durl,
                                                     url, prom_obj, ig_id,
                                                     creative_hash, view_tag,
                                                     ad_status)
            else:
                params = self.get_link_ad_params(ad_name, asid, title, body,
                                                 desc, cta, durl, url,
                                                 prom_obj, ig_id,
                                                 creative_hash, view_tag,
                                                 ad_status)
            dof = {
                "creative_features_spec": {
                    "standard_enhancements": {
                        "enroll_status": "OPT_OUT"
                    }
                }
            }
            params[Ad.Field.creative]['degrees_of_freedom_spec'] = dof
            for attempt_number in range(100):
                try:
                    self.account.create_ad(params=params)
                    break
                except FacebookRequestError as e:
                    continue_running = self.request_error(e)
                    if not continue_running:
                        break

    def get_video_ad_params(self, ad_name, asid, title, body, desc, cta, url,
                            prom_obj, ig_id, creative_hash, vid_id, view_tag,
                            ad_status):
        data = self.get_video_ad_data(vid_id, body, title, desc, cta, url,
                                      creative_hash)
        story = {
            AdCreativeObjectStorySpec.Field.page_id: str(prom_obj),
            AdCreativeObjectStorySpec.Field.video_data: data
        }
        if ig_id and str(ig_id) != 'nan':
            story[AdCreativeObjectStorySpec.Field.instagram_actor_id] = ig_id
        creative = {
            AdCreative.Field.object_story_spec: story
        }
        params = {Ad.Field.name: ad_name,
                  Ad.Field.status: ad_status,
                  Ad.Field.adset_id: asid[0],
                  Ad.Field.creative: creative}
        if view_tag and str(view_tag) != 'nan':
            params['view_tags'] = [view_tag]
        return params

    def get_link_ad_params(self, ad_name, asid, title, body, desc, cta, durl,
                           url, prom_obj, ig_id, creative_hash, view_tag,
                           ad_status):
        data = self.get_link_ad_data(body, creative_hash, durl, desc, url,
                                     title, cta)
        story = {
            AdCreativeObjectStorySpec.Field.page_id: str(prom_obj),
            AdCreativeObjectStorySpec.Field.link_data: data
        }
        if ig_id and str(ig_id) != 'nan':
            story[AdCreativeObjectStorySpec.Field.instagram_actor_id] = ig_id
        creative = {
            AdCreative.Field.object_story_spec: story
        }
        params = {Ad.Field.name: ad_name,
                  Ad.Field.status: ad_status,
                  Ad.Field.adset_id: asid[0],
                  Ad.Field.creative: creative}
        if view_tag and str(view_tag) != 'nan':
            params['view_tags'] = [view_tag]
        return params

    @staticmethod
    def get_video_ad_data(vid_id, body, title, desc, cta, url, creative_hash):
        data = {
            AdCreativeVideoData.Field.video_id: vid_id,
            AdCreativeVideoData.Field.message: body,
            AdCreativeVideoData.Field.title: title,
            AdCreativeVideoData.Field.link_description: desc,
            AdCreativeVideoData.Field.call_to_action: {
                'type': cta,
                'value': {
                    'link': url,
                },
            },
        }
        if creative_hash[:4] == 'http':
            data[AdCreativeVideoData.Field.image_url] = creative_hash
        else:
            data[AdCreativeVideoData.Field.image_hash] = creative_hash
        return data

    @staticmethod
    def get_link_ad_data(body, creative_hash, durl, desc, url, title, cta):
        data = {
            AdCreativeLinkData.Field.message: body,
            AdCreativeLinkData.Field.image_hash: creative_hash,
            AdCreativeLinkData.Field.caption: durl,
            AdCreativeLinkData.Field.description: desc,
            AdCreativeLinkData.Field.link: url,
            AdCreativeLinkData.Field.name: title,
            AdCreativeLinkData.Field.call_to_action: {
                'type': cta,
                'value': {
                    'link': url,
                },
            },
        }
        return data

    @staticmethod
    def get_carousel_ad_data(creative_hash, desc, url, title, cta,
                             vid_id=None):
        data = {
            AdCreativeLinkData.Field.description: desc,
            AdCreativeLinkData.Field.link: url,
            AdCreativeLinkData.Field.name: title,
            AdCreativeLinkData.Field.call_to_action: {
                'type': cta,
                'value': {
                    'link': url,
                },
            },
        }
        if creative_hash[:4] == 'http':
            data['picture'] = creative_hash
        else:
            data[AdCreativeVideoData.Field.image_hash] = creative_hash
        if vid_id:
            data[AdCreativeVideoData.Field.video_id] = vid_id
        return data

    @staticmethod
    def get_individual_carousel_param(param_list, idx):
        if idx < len(param_list):
            param = param_list[idx]
        else:
            logging.warning('{} does not have index {}.  Using last available.'
                            ''.format(param_list, idx))
            param = param_list[-1]
        return param

    def get_carousel_ad_params(self, ad_name, asid, title, body, desc, cta,
                               durl, url, prom_obj, ig_id, creative_hash,
                               view_tag, ad_status):
        data = []
        for idx, creative in enumerate(creative_hash):
            current_description = self.get_individual_carousel_param(desc, idx)
            current_url = self.get_individual_carousel_param(url, idx)
            current_title = self.get_individual_carousel_param(title, idx)
            if len(creative) == 1:
                data_ind = self.get_carousel_ad_data(
                    creative_hash=creative[0], desc=current_description,
                    url=current_url, title=current_title, cta=cta)
            else:
                data_ind = self.get_carousel_ad_data(
                    creative_hash=creative[1], desc=current_description,
                    url=current_url, title=current_title, cta=cta,
                    vid_id=creative[0])
            data.append(data_ind)
        link = {
            AdCreativeLinkData.Field.message: body,
            AdCreativeLinkData.Field.link: url[0],
            AdCreativeLinkData.Field.caption: durl,
            AdCreativeLinkData.Field.child_attachments: data,
            AdCreativeLinkData.Field.call_to_action: {
                'type': cta,
                'value': {
                    'link': url[0],
                },
            },
        }
        story = {
            AdCreativeObjectStorySpec.Field.page_id: str(prom_obj),
            AdCreativeObjectStorySpec.Field.link_data: link
        }
        if ig_id and str(ig_id) != 'nan':
            story[AdCreativeObjectStorySpec.Field.instagram_actor_id] = ig_id
        creative = {
            AdCreative.Field.object_story_spec: story
        }
        params = {Ad.Field.name: ad_name,
                  Ad.Field.status: ad_status,
                  Ad.Field.adset_id: asid[0],
                  Ad.Field.creative: creative}
        if view_tag and str(view_tag) != 'nan':
            params['view_tags'] = [view_tag]
        return params


class CampaignUpload(object):
    name = 'campaign_name'
    objective = 'campaign_objective'
    spend_cap = 'campaign_spend_cap'
    status = 'campaign_status'
    special_ad_cateogry = 'special_ad_category'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.cam_objective = None
        self.cam_status = None
        self.cam_spend_cap = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='campaign_upload.xlsx'):
        config_file = os.path.join(config_path, config_file)
        df = pd.read_excel(config_file)
        df = df.dropna(subset=[self.name])
        for col in [self.spend_cap]:
            df[col] = df[col] * 100
        self.config = df.set_index(self.name).to_dict(orient='index')

    def check_config(self, campaign):
        self.check_param(campaign, self.objective, Campaign.Objective)
        self.check_param(campaign, self.status, Campaign.EffectiveStatus)

    def check_param(self, campaign, param, param_class):
        input_param = self.config[campaign][param]
        valid_params = [v for k, v in vars(param_class).items()
                        if not k[-2:] == '__']
        if input_param not in valid_params:
            logging.warning(str(param) + ' not valid.  Use one ' +
                            'of the following names: ' + str(valid_params))

    def set_campaign(self, campaign):
        self.cam_objective = self.config[campaign][self.objective]
        self.cam_spend_cap = self.config[campaign][self.spend_cap]
        self.cam_status = self.config[campaign][self.status]

    def upload_all_campaigns(self, api):
        total_campaigns = str(len(self.config))
        for idx, campaign in enumerate(self.config):
            logging.info('Uploading campaign ' + str(idx + 1) + ' of ' +
                         total_campaigns + '.  Campaign Name: ' + campaign)
            self.upload_campaign(api, campaign)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    def upload_campaign(self, api, campaign):
        self.check_config(campaign)
        self.set_campaign(campaign)
        api.create_campaign(campaign, self.cam_objective, self.cam_status,
                            self.cam_spend_cap)


class AdSetUpload(object):
    key = 'key'
    name = 'adset_name'
    cam_name = 'campaign_name'
    target = 'adset_target'
    country = 'adset_country'
    age_min = 'age_min'
    age_max = 'age_max'
    genders = 'genders'
    device = 'device_platforms'
    pubs = 'publisher_platforms'
    pos = 'facebook_positions'
    budget_type = 'adset_budget_type'
    budget_value = 'adset_budget_value'
    goal = 'adset_optimization_goal'
    bid = 'adset_bid_amount'
    start_time = 'adset_start_time'
    end_time = 'adset_end_time'
    status = 'adset_status'
    bill_evt = 'adset_billing_event'
    prom_page = 'adset_page_id'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.as_name = None
        self.as_cam_name = None
        self.as_target = None
        self.as_country = None
        self.as_age_min = None
        self.as_age_max = None
        self.as_genders = None
        self.as_device = None
        self.as_pubs = None
        self.as_pos = None
        self.as_budget_type = None
        self.as_budget_value = None
        self.as_goal = None
        self.as_bid = None
        self.as_start_time = None
        self.as_end_time = None
        self.as_status = None
        self.as_bill_evt = None
        self.as_prom_page = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='adset_upload.xlsx'):
        config_file = os.path.join(config_path, config_file)
        df = pd.read_excel(config_file)
        df = df.dropna(subset=[self.name])
        df[self.prom_page] = df[self.prom_page].astype('U').str.strip('_')
        df[self.genders] = df[self.genders].map({'M': [1], 'F': [2]})
        df = self.age_check(df)
        df = df.fillna('')
        for col in [self.budget_value, self.bid]:
            df[col] = df[col] * 100
        df[self.key] = df[self.cam_name] + df[self.name]
        self.config = df.set_index(self.key).to_dict(orient='index')
        for k in self.config:
            for item in [self.cam_name, self.target, self.country, self.device,
                         self.pubs, self.pos]:
                self.config[k][item] = self.config[k][item].split('|')
            for item in [self.target]:
                for idx, target in enumerate(self.config[k][item]):
                    self.config[k][item][idx] = target.split('::')
                    try:
                        self.config[k][item][idx][1] = (self.config[k][item]
                                                        [idx][1].split(','))
                    except IndexError:
                        logging.warning('Adset target: ' + str(k) +
                                        ' was incorrectly formatted for ' +
                                        ' target: ' +
                                        str(self.config[k][item]))
    
    def age_check(self, df):
        for col in [self.age_min, self.age_max]:
            df.loc[df[col] < 13, col] = 13
            df.loc[df[col] > 65, col] = 65
        df[self.age_min] = np.where(df[self.age_min] > df[self.age_max],
                                    df[self.age_max], df[self.age_min])
        df[self.age_max] = np.where(df[self.age_max] < df[self.age_min],
                                    df[self.age_min], df[self.age_max])
        return df

    def set_adset(self, adset):
        self.as_name = self.config[adset][self.name]
        self.as_cam_name = self.config[adset][self.cam_name]
        self.as_target = self.config[adset][self.target]
        self.as_country = self.config[adset][self.country]
        self.as_age_min = self.config[adset][self.age_min]
        self.as_age_max = self.config[adset][self.age_max]
        self.as_genders = self.config[adset][self.genders]
        self.as_device = self.config[adset][self.device]
        self.as_pubs = self.config[adset][self.pubs]
        self.as_pos = self.config[adset][self.pos]
        self.as_budget_type = self.config[adset][self.budget_type]
        self.as_budget_value = self.config[adset][self.budget_value]
        self.as_goal = self.config[adset][self.goal]
        self.as_bid = self.config[adset][self.bid]
        self.as_start_time = self.config[adset][self.start_time]
        self.as_end_time = self.config[adset][self.end_time]
        self.as_status = self.config[adset][self.status]
        self.as_bill_evt = self.config[adset][self.bill_evt]
        self.as_prom_page = self.config[adset][self.prom_page]

    def upload_all_adsets(self, api):
        total_adsets = str(len(self.config))
        for idx, adset in enumerate(self.config):
            logging.info('Uploading adset ' + str(idx + 1) + ' of ' +
                         total_adsets + '.  Adset Name: ' + adset)
            self.upload_adset(api, adset)
        logging.info('Pausing for 30s while adsets finish uploading.')
        time.sleep(30)

    def upload_adset(self, api, adset):
        self.set_adset(adset)
        self.format_adset(api)

    def format_adset(self, api):
        cids = api.campaign_to_id(self.as_cam_name)
        if not cids:
            logging.warning(str(self.as_cam_name) + ' does not exist in the ' +
                            'account.  ' + str(self.as_name) + ' was not ' +
                            'uploaded.')
            return None
        api.create_adset(self.as_name, cids, self.as_goal, self.as_budget_type,
                         self.as_budget_value, self.as_bill_evt, self.as_bid,
                         self.as_status, self.as_start_time, self.as_end_time,
                         self.as_prom_page, self.as_country, self.as_target,
                         self.as_age_min, self.as_age_max, self.as_genders,
                         self.as_device, self.as_pubs, self.as_pos)


class AdUpload(object):
    key = 'key'
    name = 'ad_name'
    cam_name = 'campaign_name'
    adset_name = 'adset_name'
    filename = 'creative_filename'
    prom_page = 'ad_page_id'
    ig_id = 'instagram_page_id'
    link = 'link_url'
    d_link = 'display_url'
    title = 'title'
    body = 'body'
    desc = 'description'
    cta = 'call_to_action'
    view_tag = 'view_tag'
    status = 'ad_status'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.ad_key = None
        self.ad_name = None
        self.ad_cam_name = None
        self.ad_adset_name = None
        self.ad_filename = None
        self.ad_prom_page = None
        self.ad_ig_id = None
        self.ad_link = None
        self.ad_d_link = None
        self.ad_title = None
        self.ad_body = None
        self.ad_desc = None
        self.ad_cta = None
        self.ad_view_tag = None
        self.ad_status = None
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='ad_upload.xlsx'):
        config_file = os.path.join(config_path, config_file)
        df = pd.read_excel(config_file)
        df = df.dropna(subset=[self.name])
        for col in [self.prom_page, self.ig_id]:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.strip('_')
        for col in [self.title, self.body, self.desc, self.filename]:
            df[col] = df[col].replace(np.nan, '', regex=True)
        df[self.key] = df[self.cam_name] + df[self.adset_name] + df[self.name]
        self.config = df.set_index(self.key).to_dict(orient='index')
        for k in self.config:
            self.split_config_by_strings(k)

    def split_config_by_strings(self, k):
        for item in [self.cam_name, self.adset_name, self.filename,
                     self.link, self.title, self.desc]:
            self.config[k][item] = self.config[k][item].split('|')
            if item == self.filename:
                self.config[k][self.filename] = [x.split('::') for x in
                                                 self.config[k][self.filename]]

    def set_ad(self, ad):
        self.ad_name = self.config[ad][self.name]
        self.ad_cam_name = self.config[ad][self.cam_name]
        self.ad_adset_name = self.config[ad][self.adset_name]
        self.ad_filename = self.config[ad][self.filename]
        self.ad_prom_page = self.config[ad][self.prom_page]
        self.ad_ig_id = self.config[ad][self.ig_id]
        self.ad_link = self.config[ad][self.link]
        self.ad_d_link = self.config[ad][self.d_link]
        self.ad_title = self.config[ad][self.title]
        self.ad_body = self.config[ad][self.body]
        self.ad_desc = self.config[ad][self.desc]
        self.ad_cta = self.config[ad][self.cta]
        self.ad_view_tag = self.config[ad][self.view_tag]
        self.ad_status = self.config[ad][self.status]

    def upload_all_creatives(self, api, creative_class):
        creatives = list(set(y for k in self.config for x in
                             self.config[k][self.filename] for y in x))
        images = [x for x in creatives
                  if x.split('.')[-1].lower() in utl.static_types]
        videos = [x for x in creatives if x not in images]
        creative_class.upload_all_creatives(api, images, videos)
        self.creative_filename_to_hash(table=creative_class.table)
        self.add_thumbnail_images(api, videos, table=creative_class.table)

    def add_thumbnail_images(self, api, videos, table=None):
        thumb_vids = []
        for k in self.config:
            for cre in self.config[k][self.filename]:
                if (len(cre) == 1) and (cre[0].isdigit()):
                    thumb_vids.append(cre[0])
        thumb_dict = {}
        for tid in set(thumb_vids):
            file_name = [k for (k, v) in table.items() if v == tid]
            if file_name and file_name[0].split('.')[-1] in utl.static_types:
                continue
            img_url = api.get_video_thumbnail(tid)
            thumb_dict[tid] = img_url
        for k in self.config:
            for idx, cre in enumerate(self.config[k][self.filename]):
                if len(cre) == 1 and cre[0].isdigit():
                    self.config[k][self.filename][idx].append(
                        thumb_dict[cre[0]])

    def creative_filename_to_hash(self, table):
        for k in self.config:
            for idx_1, cre in enumerate(self.config[k][self.filename]):
                for idx_2, ind_cre in enumerate(cre):
                    self.config[k][self.filename][idx_1][idx_2] = (
                        table['creative/' + ind_cre])
        return table

    def upload_all_ads(self, api, creative_class):
        self.upload_all_creatives(api, creative_class)
        if not api.ad_dict:
            api.set_id_name_dict(Ad)
        total_ads = str(len(self.config))
        for idx, ad in enumerate(self.config):
            logging.info('Uploading ad ' + str(idx + 1) + ' of ' + total_ads +
                         '.  Ad Name: ' + ad)
            self.upload_ad(ad, api)

    def upload_ad(self, ad, api):
        self.set_ad(ad)
        self.format_ad(api)

    def format_ad(self, api):
        cids = api.campaign_to_id(self.ad_cam_name)
        asids = api.adset_to_id(self.ad_adset_name, cids)
        if not cids:
            logging.warning(str(self.ad_cam_name) + ' does not exist in the ' +
                            'account.  ' + str(self.ad_name) + ' was not ' +
                            'uploaded.')
            return None
        if not asids:
            logging.warning(str(self.ad_adset_name) + ' does not exist in ' +
                            'the account.  ' + str(self.ad_name) + ' was ' +
                            'not uploaded.')
            return None
        if len(self.ad_filename) == 1 and len(self.ad_filename[0]) == 1:
            api.create_ad(self.ad_name, asids, self.ad_title[0],
                          self.ad_body,  self.ad_desc[0], self.ad_cta,
                          self.ad_d_link,  self.ad_link[0], self.ad_prom_page,
                          self.ad_ig_id, self.ad_view_tag, self.ad_status,
                          self.ad_filename[0][0])
        elif len(self.ad_filename) == 1 and len(self.ad_filename[0]) == 2:
            api.create_ad(self.ad_name, asids, self.ad_title[0], self.ad_body,
                          self.ad_desc[0], self.ad_cta, self.ad_d_link,
                          self.ad_link[0], self.ad_prom_page, self.ad_ig_id,
                          self.ad_view_tag, self.ad_status,
                          self.ad_filename[0][1],
                          vid_id=self.ad_filename[0][0])
        elif len(self.ad_filename) > 1:
            api.create_ad(self.ad_name, asids, self.ad_title, self.ad_body,
                          self.ad_desc, self.ad_cta, self.ad_d_link,
                          self.ad_link, self.ad_prom_page, self.ad_ig_id,
                          self.ad_view_tag, self.ad_status,
                          self.ad_filename)


class Creative(object):
    def __init__(self, creative_file=None, creative_path='creative/'):
        self.creative_path = creative_path
        self.creative_file = creative_file
        self.creative_path_file = None
        self.fn_col = 'filename'
        self.hash_col = 'hash'
        self.table = None
        if self.creative_file:
            self.load_config(self.creative_file, self.creative_path)

    def set_config_file(self, creative_file, creative_path):
        self.creative_file = creative_file
        self.creative_path = creative_path
        if not self.creative_file or not self.creative_path:
            self.creative_path_file = None
        else:
            self.creative_path_file = self.creative_path + self.creative_file

    def load_config(self, creative_file='creative_hashes.csv',
                    creative_path='creative/'):
        self.set_config_file(creative_file, creative_path)
        if not os.path.isfile(self.creative_path_file):
            df = pd.DataFrame(columns=[self.fn_col, self.hash_col], index=None)
            dir_name = os.path.dirname(os.path.abspath(self.creative_path_file))
            utl.dir_check(dir_name)
            df.to_csv(self.creative_path_file, index=False)
        df = pd.read_csv(self.creative_path_file)
        df[self.hash_col] = df[self.hash_col].str.strip('_')
        self.table = pd.Series(df[self.hash_col].values,
                               index=df[self.fn_col]).to_dict()

    def get_new_creative(self, creatives, creative_path):
        creatives = [(creative_path + x) for x in creatives if str(x) != 'nan']
        new_cre = [x for x in creatives if x not in list(self.table.keys())]
        return new_cre

    def upload_all_creatives(self, api, images, videos,
                             creative_path='creative/'):
        new_vid = self.get_new_creative(videos, creative_path)
        new_img = self.get_new_creative(images, creative_path)
        total_cre = str(len(new_vid + new_img))
        for idx, creative in enumerate(new_img + new_vid):
            logging.info('Uploading creative ' + str(idx + 1) + ' of ' +
                         total_cre + '.  Creative Name: ' + creative)
            if os.path.isfile(creative):
                if creative in new_img:
                    self.upload_creative(api, creative, AdImage)
                elif creative in new_vid:
                    self.upload_creative(api, creative, AdVideo)
            else:
                logging.warning(creative + 'not found.  It was not uploaded')
        self.write_df_to_csv()

    def upload_creative(self, api, creative_filename, creative_class):
        creative_hash = api.upload_creative(creative_class, creative_filename)
        self.table[creative_filename] = creative_hash

    @staticmethod
    def dict_to_df(dictionary, first_col, second_col):
        df = pd.Series(dictionary, name=second_col)
        df.index.name = first_col
        df = df.reset_index()
        return df

    def write_df_to_csv(self):
        df = self.dict_to_df(self.table, self.fn_col, self.hash_col)
        df[self.hash_col] = '_' + df[self.hash_col]
        try:
            df.to_csv(self.creative_path_file, index=False)
        except IOError:
            logging.warning(self.creative_file + ' could not be opened.  ' +
                            'This dictionary was not saved.')
