import os
import sys
import time
import json
import pytz
import logging
import itertools
import numpy as np
import pandas as pd
import datetime as dt
import decimal
import upload.utils as utl
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.targeting import Targeting
from facebook_business.adobjects.user import User
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
    saved_audience = 'savedaudience'
    custom_audience = 'customaudience'

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
        self.tz = self.timezone_check()

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

    @staticmethod
    def timezone_check():
        now = dt.datetime.now(pytz.timezone('America/Los_Angeles'))
        time_zone = now.tzname()
        return time_zone

    def has_account(self):
        """Return True if a usable ad-account id is configured."""
        act_id = str(self.act_id or '').replace('act_', '').strip()
        return bool(self.account and act_id)

    def probe_account(self):
        """(ok, message) — cheapest authenticated account read, for
        the live pre-flight checks."""
        try:
            if not self.has_account():
                return False, 'No ad-account id configured.'
            self.account.api_get(fields=['id'])
            return True, ''
        except FacebookRequestError as e:
            return False, str(e.api_error_message())
        except Exception as e:
            return False, str(e)

    fb_objects_by_level = {'Campaign': Campaign, 'Adset': AdSet, 'Ad': Ad}

    def update_statuses(self, object_level, platform_ids, activate=True):
        """Set ACTIVE/PAUSED on existing objects by platform id.
        Returns one dict per id: {'platform_id', 'status'
        ('updated'|'failed'), 'error_code', 'error_message'}."""
        fb_object = self.fb_objects_by_level.get(object_level)
        status = 'ACTIVE' if activate else 'PAUSED'
        results = []
        for pid in platform_ids:
            result = {'platform_id': pid, 'status': 'updated',
                      'error_code': None, 'error_message': None}
            if not fb_object:
                result['status'] = 'failed'
                result['error_message'] = (
                    'Unknown Facebook level: {}'.format(object_level))
                results.append(result)
                continue
            try:
                fb_object(str(pid)).api_update(params={'status': status})
            except FacebookRequestError as e:
                result['status'] = 'failed'
                result['error_code'] = str(e.api_error_code())
                result['error_message'] = str(e.api_error_message())
            except Exception as e:
                result['status'] = 'failed'
                result['error_message'] = str(e)
            results.append(result)
        return results

    def update_object(self, object_level, platform_id, changes,
                      context=None):
        """Push whitelisted field edits to one existing object.

        ``changes`` is ``{spreadsheet_column: new_value}`` in row
        (spreadsheet) space — the same vocabulary the upload files and
        ``pushed_values`` snapshots carry; this method applies the
        create path's platform transforms (cent scaling, tz-suffixed
        datetimes, budget_type-routed budget field). All of an
        object's edits ride ONE ``api_update`` call, so a partial
        push can't happen. ``context`` is the object's current file
        row, consulted for routing values that aren't themselves
        updatable (``adset_budget_type``).

        :param object_level: 'Campaign' | 'Adset' | 'Ad'.
        :param platform_id: The platform id to update.
        :param changes: ``{spreadsheet_column: new_value}``.
        :param context: Optional current file row dict.
        :returns: ``{'platform_id', 'status' ('updated'|'failed'),
            'error_code', 'error_message'}``.
        """
        result = {'platform_id': platform_id, 'status': 'updated',
                  'error_code': None, 'error_message': None}
        try:
            fb_object, params = self.update_params(
                object_level, changes, context)
            fb_object(str(platform_id)).api_update(params=params)
        except FacebookRequestError as e:
            result['status'] = 'failed'
            result['error_code'] = str(e.api_error_code())
            result['error_message'] = str(e.api_error_message())
        except Exception as e:
            result['status'] = 'failed'
            result['error_message'] = str(e)
        return result

    def update_params(self, object_level, changes, context=None):
        """The whole ``api_update`` payload for one object, built
        before any call is made so a rejected edit can't leave a
        partial push behind.

        :param object_level: 'Campaign' | 'Adset' | 'Ad'.
        :param changes: ``{spreadsheet_column: new_value}``.
        :param context: Optional current file row dict.
        :returns: ``(fb_object_class, params)``.
        :raises ValueError: On an unsupported level, a column outside
            the update whitelist, an untransformable value, or no
            changes at all.
        """
        fb_object = self.fb_objects_by_level.get(object_level)
        fields = UPDATE_COLUMN_FIELDS.get(object_level)
        if not fb_object or not fields:
            raise ValueError(
                'No update support for Facebook level: {}'.format(
                    object_level))
        params = {}
        for col, value in (changes or {}).items():
            if col not in fields:
                raise ValueError(
                    'Column not updatable: {}'.format(col))
            params.update(self.update_param(
                object_level, col, value, context))
        if not params:
            raise ValueError('No changes supplied.')
        return fb_object, params

    def update_param(self, object_level, col, value, context=None):
        """``{platform_field: platform_value}`` for one
        spreadsheet-space edit, mirroring the create path exactly:
        money x100 to integer cents, datetimes day-bounded then
        tz-suffixed, the adset budget routed to daily/lifetime via the
        row's ``adset_budget_type``.

        :param col: Spreadsheet-space column name.
        :param value: The new value in spreadsheet space.
        :param context: Optional current file row dict.
        :returns: One-entry params dict for ``api_update``.
        :raises ValueError: When the value can't be transformed.
        """
        field = UPDATE_COLUMN_FIELDS[object_level][col]
        if col in MONEY_UPDATE_COLS:
            cents = money_cents(col, value)
            if col == AdSetUpload.budget_value:
                bud_type = str((context or {}).get(
                    AdSetUpload.budget_type) or '').strip()
                if bud_type == 'daily':
                    return {AdSet.Field.daily_budget: cents}
                if bud_type == 'lifetime':
                    return {AdSet.Field.lifetime_budget: cents}
                raise ValueError(
                    'Unknown adset_budget_type {!r} — cannot route '
                    'the budget update.'.format(bud_type))
            return {field: cents}
        if col in DATE_UPDATE_COLS:
            v = str(value)
            if ':' not in v:
                v = '{} {}'.format(
                    v, '23:59:59' if col == AdSetUpload.end_time
                    else '00:00:00')
            return {field: '{} {}'.format(v, self.tz)}
        return {field: str(value)}

    def set_id_name_dict(self, fb_object, parent_ids=None):
        if not self.has_account():
            logging.warning('No Facebook ad-account id configured.  '
                            'Skipping object lookup.')
            dict_attr = {Campaign: 'cam_dict', AdSet: 'adset_dict',
                         Ad: 'ad_dict'}.get(fb_object)
            if dict_attr:
                setattr(self, dict_attr, [])
            return
        if fb_object == Campaign:
            fields = ['id', 'name']
            self.cam_dict = list(self.account.get_campaigns(fields=fields))
        elif fb_object == AdSet:
            params = None
            if parent_ids:
                params = {
                    "filtering": [{
                        "field": "campaign.id",
                        "operator": "IN",
                        "value": parent_ids,
                    }]
                }
            fields = ['id', 'name', 'campaign_id']
            self.adset_dict = list(self.account.get_ad_sets(
                fields=fields, params=params))
        elif fb_object == Ad:
            params = None
            if parent_ids:
                params = {
                    "filtering": [{
                        "field": "adset.id",
                        "operator": "IN",
                        "value": parent_ids,
                    }]
                }
            fields = ['id', 'name', 'campaign_id', 'adset_id']
            self.ad_dict = list(self.account.get_ads(
                fields=fields, params=params))

    def campaign_to_id(self, campaigns):
        if not self.cam_dict:
            self.set_id_name_dict(Campaign)
        cids = [x['id'] for x in self.cam_dict if x['name'] in campaigns]
        return cids

    def adset_to_id(self, adsets, cids):
        as_and_cam = list(itertools.product(adsets, cids))
        if not self.adset_dict:
            self.set_id_name_dict(AdSet, parent_ids=cids)
        asids = [tuple([x['id'], x['campaign_id']]) for x in self.adset_dict
                 if tuple([x['name'], x['campaign_id']]) in as_and_cam]
        return asids

    def create_campaign(self, campaign_name, objective, status, spend_cap):
        if not self.cam_dict:
            self.set_id_name_dict(Campaign)
        existing = [x for x in self.cam_dict if x['name'] == campaign_name]
        if existing:
            logging.warning(campaign_name + ' already in account.  This ' +
                            'campaign was not uploaded.')
            return {'status': 'skipped_exists',
                    'platform_id': existing[0].get('id'),
                    'error_code': None, 'error_message': None}
        self.campaign = Campaign(parent_id=self.account.get_id_assured())
        self.campaign.update({
            Campaign.Field.name: campaign_name,
            Campaign.Field.objective: objective,
            Campaign.Field.status: status,
            Campaign.Field.spend_cap: int(spend_cap),
            Campaign.Field.special_ad_categories: 'NONE',
            Campaign.Field.is_adset_budget_sharing_enabled: False,
        })
        try:
            self.campaign.remote_create()
        except FacebookRequestError as e:
            return {'status': 'failed', 'platform_id': None,
                    'error_code': str(e.api_error_code() or '') or None,
                    'error_message': e.api_error_message()}
        return {'status': 'created',
                'platform_id': self.campaign.get_id(),
                'error_code': None, 'error_message': None}

    @staticmethod
    def geo_target_search(geos, location_types=Targeting.Field.country):
        all_geos = []
        for geo in geos:
            params = {
                'q': geo,
                'type': 'adgeolocation',
                'location_types': [location_types],
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
        if isinstance(audiences, (str, bytes)):
            audiences = [audiences]
        aud_list = []
        for audience in audiences:
            if not audience:
                continue
            audience = CustomAudience(audience)
            val_aud = audience.remote_read(fields=['targeting'])
            aud_list.append(val_aud)
            aud_list = aud_list[0]['targeting']
        return aud_list

    def get_account_custom_audiences(self):
        """Every custom audience on the account as ``[{'id','name'}]``.
        The list-all the matching helper already relies on, exposed so
        the app layer can offer an audience picker instead of free-text
        IDs."""
        act_auds = self.account.get_custom_audiences(
            fields=[CustomAudience.Field.name, CustomAudience.Field.id])
        return [{'id': x['id'], 'name': x['name']} for x in act_auds]

    def get_account_pixels(self):
        """Every ads pixel on the account as ``[{'id','name'}]``
        (name falls back to the id when the pixel is unnamed)."""
        pixels = self.account.get_ads_pixels(fields=['id', 'name'])
        return [{'id': x['id'], 'name': x.get('name') or x['id']}
                for x in pixels]

    def get_account_pages(self):
        """Pages the account can promote as ``[{'id','name'}]`` so the
        app layer can offer a page picker for adset/ad page ids."""
        pages = self.account.get_promote_pages(fields=['id', 'name'])
        return [{'id': x['id'], 'name': x.get('name') or x['id']}
                for x in pages]

    def get_user_pages(self):
        """Every Page the token can access (``me/accounts``) as
        ``[{'id','name'}]`` — page ids aren't ad-account-scoped, so the
        picker offers the full set, not just the account's promote-pages.
        """
        pages = User(fbid='me').get_accounts(fields=['id', 'name'])
        return [{'id': x['id'], 'name': x.get('name') or x['id']}
                for x in pages]

    def get_matching_custom_audiences(self, audiences):
        return [a for a in self.get_account_custom_audiences()
                if a['id'] in audiences]

    def get_matching_audience(self, target, targeting):
        """
        Checks if the audience type is custom or saved and returns in targeting

        :param target: List with first item the audience type second audience id
        :param targeting: The dictionary containing all targeting info to update
        :return: The updated targeting dictionary
        """
        audience_id = target[1]
        if isinstance(audience_id, (list, tuple)):
            has_value = any(x for x in audience_id)
        else:
            has_value = bool(audience_id)
        if not has_value:
            logging.warning(
                'Target type {!r} has no audience id (raw value: '
                '{!r}); skipping audience lookup. Fix the '
                'adset_target column if an audience was '
                'intended.'.format(target[0], audience_id))
            return targeting
        search_order = (self.custom_audience, self.saved_audience)
        if target[0] == self.saved_audience:
            search_order = search_order[::-1]
        for audience_type in search_order:
            if audience_type == self.saved_audience:
                aud_target = self.get_matching_saved_audiences(audience_id)
                if aud_target:
                    targeting.update(aud_target)
                    break
            elif audience_type == self.custom_audience:
                aud_target = self.get_matching_custom_audiences(audience_id)
                if aud_target:
                    targeting[Targeting.Field.custom_audiences] = aud_target
                    break
        return targeting

    @staticmethod
    def check_additional_positions(targeting, facebook_positions, platform,
                                   split_on_delim=True):
        """
        Additional positions based on platform (messenger or threads) are
        updated in the original positions list and targeting dict

        :param targeting: The full targeting dict to update
        :param facebook_positions: The list of positions to use
        :param platform: messenger or threads
        :param split_on_delim: Split on the platform string
        :return: The updated targeting and facebook_positions
        """
        platform_str = platform.split('_')[0]
        has_messenger = [x for x in facebook_positions if platform_str in x]
        if has_messenger:
            facebook_positions = [x for x in facebook_positions
                                  if x not in has_messenger]
            mess_pos = has_messenger
            if split_on_delim:
                platform_delim = '{}_'.format(platform_str)
                mess_pos = [x.split(platform_delim)[1] for x in has_messenger]
            targeting[platform] = mess_pos
        return targeting, facebook_positions

    def set_positions(self, targeting, facebook_positions, publisher_platform):
        """
        Updates the positions to target and returns the targeting dictionary

        :param targeting: The full targeting dict to update
        :param facebook_positions: The list of positions to use
        :param publisher_platform: The publisher platforms specified
        :return:
        """
        key = Targeting.Field.facebook_positions
        if publisher_platform and 'instagram' in publisher_platform:
            key = Targeting.Field.instagram_positions
        targeting, facebook_positions = self.check_additional_positions(
            targeting, facebook_positions,
            platform=Targeting.Field.messenger_positions)
        targeting, facebook_positions = self.check_additional_positions(
            targeting, facebook_positions,
            platform='threads_positions', split_on_delim=False)
        targeting[key] = facebook_positions
        return targeting

    def parse_geo_locations(self, geos, targeting):
        """
        Parses list of geos and returns targeting dict with the locations
        in a way that the fb api can interpret

        :param geos: List of geo strings by default will be include country
        :param targeting: A dictionary that will be added to
        :return: The targeting dictionary
        """
        exclude_dict = {}
        include_dict = {}
        for geo in geos:
            cur_dict = include_dict
            key = Targeting.Field.countries
            if 'exclude' in geo:
                geo = geo.replace('exclude', '')
                cur_dict = exclude_dict
            if 'region' in geo:
                geo = geo.replace('region', '')
                key = Targeting.Field.regions
                geo = self.geo_target_search([geo], location_types='region')
                geo = {'key': geo[0]['key']}
            if key in cur_dict:
                cur_dict[key].append(geo)
            else:
                cur_dict[key] = [geo]
        if include_dict:
            targeting[Targeting.Field.geo_locations] = include_dict
        if exclude_dict:
            targeting[Targeting.Field.excluded_geo_locations] = exclude_dict
        return targeting

    def set_target(self, geos, targets, age_min, age_max, gender, device,
                   publisher_platform, facebook_positions):
        targeting = {"targeting_automation": {"advantage_audience": 0}}
        if geos and geos != ['']:
            targeting = self.parse_geo_locations(geos, targeting)
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
            targeting = self.set_positions(
                targeting, facebook_positions, publisher_platform)
        for target in targets:
            if target[0] == 'interest' or target[0] == 'interest-broad':
                int_targets = self.target_search(target)
                targeting[Targeting.Field.interests] = int_targets
            if 'audience' in target[0]:
                targeting = self.get_matching_audience(target, targeting)
        return targeting

    def create_adset(self, adset_name, cids, opt_goal, bud_type, bud_val,
                     bill_evt, bid_amt, status, start_time, end_time, prom_obj,
                     country, target, age_min, age_max, genders, device, pubs,
                     pos):
        if not self.adset_dict:
            self.set_id_name_dict(AdSet, parent_ids=cids)
        outcomes = []
        for cid in cids:
            existing = [x for x in self.adset_dict
                        if x['name'] == adset_name
                        and x['campaign_id'] == cid]
            if existing:
                msg = '{} already in campaign.  Adset was not uploaded.'.format(
                    adset_name)
                logging.warning(msg)
                outcomes.append({
                    'status': 'skipped_exists',
                    'platform_id': existing[0].get('id'),
                    'parent_platform_id': cid,
                    'error_code': None, 'error_message': None})
                continue
            targeting = self.set_target(country, target, age_min, age_max,
                                        genders, device, pubs, pos)
            if ':' not in start_time:
                start_time = '{} 00:00:00'.format(start_time)
            sd = '{} {}'.format(start_time, self.tz)
            if ':' not in end_time:
                end_time = '{} 23:59:59'.format(end_time)
            ed = '{} {}'.format(end_time, self.tz)
            params = {
                AdSet.Field.name: adset_name,
                AdSet.Field.campaign_id: cid,
                AdSet.Field.billing_event: bill_evt,
                AdSet.Field.status: status,
                AdSet.Field.targeting: targeting,
                AdSet.Field.start_time: sd,
                AdSet.Field.end_time: ed,
            }
            if bid_amt == '':
                params['bid_strategy'] = 'LOWEST_COST_WITHOUT_CAP'
            else:
                params[AdSet.Field.bid_amount] = int(bid_amt)
            if 'REACH' in opt_goal and '|' in opt_goal:
                opt_goal = opt_goal.split('|')
                interval_days = opt_goal[1]
                max_frequency = opt_goal[2]
                params[AdSet.Field.frequency_control_specs] = [{
                    'event': 'IMPRESSIONS',
                    'interval_days': interval_days,
                    'max_frequency': max_frequency,
                }]
                opt_goal = opt_goal[0]
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
                if prom_obj:
                    params[AdSet.Field.promoted_object] = {
                        'page_id': prom_obj}
                else:
                    logging.warning(
                        'Adset {!r} has no page_id '
                        '(adset_page_id is blank); skipping '
                        'promoted_object. Set adset_page_id to '
                        'the Facebook page id if needed.'.format(
                            adset_name))
            if not bud_val:
                msg = 'Budget value missing, did not upload'.format(params)
                logging.warning(msg)
                outcomes.append({
                    'status': 'failed', 'platform_id': None,
                    'parent_platform_id': cid,
                    'error_code': 'missing_budget',
                    'error_message': 'Budget value missing'})
                continue
            if bud_type == 'daily':
                params[AdSet.Field.daily_budget] = int(bud_val)
            elif bud_type == 'lifetime':
                params[AdSet.Field.lifetime_budget] = int(bud_val)
            try:
                created = self.account.create_ad_set(params=params)
            except FacebookRequestError as e:
                outcomes.append({
                    'status': 'failed', 'platform_id': None,
                    'parent_platform_id': cid,
                    'error_code': str(e.api_error_code() or '') or None,
                    'error_message': e.api_error_message()})
                continue
            outcomes.append({
                'status': 'created',
                'platform_id': created.get('id') if created else None,
                'parent_platform_id': cid,
                'error_code': None, 'error_message': None})
        return outcomes

    def upload_creative(self, creative_class, image_path):
        cre = creative_class(parent_id=self.account.get_id_assured())
        if creative_class == AdImage:
            creative_key = AdImage.Field.filename
            hash_function = cre.get_hash
        elif creative_class == AdVideo:
            creative_key = AdVideo.Field.filepath
            hash_function = cre.get_id
        else:
            return None
        cre[creative_key] = image_path
        for _ in range(3):
            try:
                cre.remote_create()
                break
            except FacebookRequestError as e:
                logging.warning('Request Error retrying: {}'.format(e))
                time.sleep(5)
        creative_hash = hash_function()
        return creative_hash

    def get_all_thumbnails(self, vid):
        video = AdVideo(vid)
        thumbnails = video.get_thumbnails()
        if not thumbnails:
            logging.warning('Could not retrieve thumbnail for vid: ' +
                            str(vid) + '.  Retrying in 120s.')
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
        return continue_running

    def create_ad(self, ad_name, asids, title, body, desc, cta, durl, url,
                  prom_obj, ig_id, view_tag, ad_status, creative_hash=None,
                  vid_id=None):
        outcomes = []
        for asid in asids:
            existing = [x for x in self.ad_dict
                        if x['name'] == ad_name
                        and x['campaign_id'] == asid[1]
                        and x['adset_id'] == asid[0]]
            if existing:
                logging.warning(ad_name + ' already in campaign/adset. ' +
                                'This ad was not uploaded.')
                outcomes.append({
                    'status': 'skipped_exists',
                    'platform_id': existing[0].get('id'),
                    'parent_platform_id': asid[0],
                    'error_code': None, 'error_message': None})
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
            params['contextual_multi_ads'] = {'enroll_status': 'OPT_OUT'}
            created = None
            last_err = None
            for attempt_number in range(100):
                try:
                    created = self.account.create_ad(params=params)
                    break
                except FacebookRequestError as e:
                    last_err = e
                    continue_running = self.request_error(e)
                    if not continue_running:
                        break
            if created is not None:
                outcomes.append({
                    'status': 'created',
                    'platform_id': created.get('id') if created else None,
                    'parent_platform_id': asid[0],
                    'error_code': None, 'error_message': None})
            else:
                outcomes.append({
                    'status': 'failed', 'platform_id': None,
                    'parent_platform_id': asid[0],
                    'error_code': (
                                      str(last_err.api_error_code() or '')
                                      if last_err else None) or None,
                    'error_message': (
                        last_err.api_error_message()
                        if last_err else 'Unknown error from Facebook')})
        return outcomes

    @staticmethod
    def check_add_instagram_threads_ids(story, ig_id):
        """
        Checks the provided ig_id and sorts into instagram_user_id and
        threads_id (if | in ig_id)

        :param story: Dictionary to update
        :param ig_id: The values to check ids for
        :return: The update story dictionary
        """
        ig_id = str(ig_id)
        if ig_id and ig_id != 'nan':
            if '|' in ig_id:
                ig_id = ig_id.split('|')
                threads_id = ig_id[1].replace('_', '')
                ig_id = ig_id[0].replace('_', '')
                story['threads_user_id'] = threads_id
            story[AdCreativeObjectStorySpec.Field.instagram_user_id] = ig_id
        return story

    def get_video_ad_params(self, ad_name, asid, title, body, desc, cta, url,
                            prom_obj, ig_id, creative_hash, vid_id, view_tag,
                            ad_status):
        data = self.get_video_ad_data(vid_id, body, title, desc, cta, url,
                                      creative_hash)
        story = {
            AdCreativeObjectStorySpec.Field.page_id: str(prom_obj),
            AdCreativeObjectStorySpec.Field.video_data: data
        }
        story = self.check_add_instagram_threads_ids(story, ig_id)
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
        """
        Creates a dictionary to be used for ad upload

        https://developers.facebook.com/docs/marketing-api/ad-creative/asset-feed-spec
        :param ad_name: Name of the ad to upload
        :param asid: ID of the adset for the ad
        :param title: Copy title
        :param body: Copy body
        :param desc: Copy description
        :param cta: Call to action button string
        :param durl: Display URL
        :param url: Link URL
        :param prom_obj: object to promote
        :param ig_id: Instagram Page ID
        :param creative_hash: Hash value of the creative already uploaded
        :param view_tag: Tag that tracks views
        :param ad_status: Paused or active
        :return: params Dictionary representation of the ad
        """
        data = self.get_link_ad_data(body, creative_hash, durl, desc, url,
                                     title, cta)
        story = {
            AdCreativeObjectStorySpec.Field.page_id: str(prom_obj),
            AdCreativeObjectStorySpec.Field.link_data: data
        }
        story = self.check_add_instagram_threads_ids(story, ig_id)
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
    def check_dynamic_copy(body, creative_hash, durl, desc, url, title,
                           cta):
        is_asset_feed = False
        params = [body, creative_hash, desc, url, title, cta]
        for param in [body, creative_hash, desc, url, title, cta]:
            if '&&&' in param:
                is_asset_feed = True

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
        story = self.check_add_instagram_threads_ids(story, ig_id)
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
    snapshot_cols = [objective, spend_cap, status]

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.raw_rows = {}
        self.cam_objective = None
        self.cam_status = None
        self.cam_spend_cap = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='campaign_upload.xlsx'):
        config_file = os.path.join(config_path, config_file)
        df = pd.read_excel(config_file)
        df = df.dropna(subset=[self.name])
        self.raw_rows = {
            k: utl.snapshot_values(v, self.snapshot_cols)
            for k, v in df.set_index(self.name).to_dict(
                orient='index').items()}
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
        results = []
        for idx, campaign in enumerate(self.config):
            logging.info('Uploading campaign ' + str(idx + 1) + ' of ' +
                         total_campaigns + '.  Campaign Name: ' + campaign)
            results.append(self.upload_campaign(api, campaign))
        return results

    def upload_campaign(self, api, campaign):
        self.check_config(campaign)
        self.set_campaign(campaign)
        outcome = api.create_campaign(
            campaign, self.cam_objective, self.cam_status,
            self.cam_spend_cap) or {}
        return {
            'source_name': campaign,
            'object_level': 'Campaign',
            'uploader_type': 'Facebook',
            'platform_id': outcome.get('platform_id'),
            'parent_platform_id': None,
            'status': outcome.get('status') or 'failed',
            'error_code': outcome.get('error_code'),
            'error_message': outcome.get('error_message'),
            'pushed_values': self.raw_rows.get(campaign),
        }


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
    snapshot_cols = [budget_type, budget_value, goal, bid, start_time,
                     end_time, status, bill_evt]

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.raw_rows = {}
        self.as_key = None
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
        raw = df.copy()
        raw[self.key] = raw[self.cam_name] + raw[self.name]
        self.raw_rows = {
            k: utl.snapshot_values(v, self.snapshot_cols)
            for k, v in raw.set_index(self.key).to_dict(
                orient='index').items()}
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
        self.as_key = adset
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
        results = []
        for idx, adset in enumerate(self.config):
            logging.info('Uploading adset ' + str(idx + 1) + ' of ' +
                         total_adsets + '.  Adset Name: ' + adset)
            results.extend(self.upload_adset(api, adset))
        return results

    def upload_adset(self, api, adset):
        self.set_adset(adset)
        return self.format_adset(api)

    def format_adset(self, api):
        cids = api.campaign_to_id(self.as_cam_name)
        if not cids:
            msg = 'Campaign {} does not exist.  {} was not uploaded'.format(
                self.as_cam_name, self.as_name)
            logging.warning(msg)
            return [{
                'source_name': self.as_name,
                'object_level': 'Adset',
                'uploader_type': 'Facebook',
                'platform_id': None,
                'parent_platform_id': None,
                'status': 'skipped_dep_missing',
                'error_code': None,
                'error_message': msg,
                'pushed_values': self.raw_rows.get(self.as_key),
            }]
        outcomes = api.create_adset(
            self.as_name, cids, self.as_goal, self.as_budget_type,
            self.as_budget_value, self.as_bill_evt, self.as_bid,
            self.as_status, self.as_start_time, self.as_end_time,
            self.as_prom_page, self.as_country, self.as_target,
            self.as_age_min, self.as_age_max, self.as_genders,
            self.as_device, self.as_pubs, self.as_pos) or []
        return [{
            'source_name': self.as_name,
            'object_level': 'Adset',
            'uploader_type': 'Facebook',
            'platform_id': o.get('platform_id'),
            'parent_platform_id': o.get('parent_platform_id'),
            'status': o.get('status') or 'failed',
            'error_code': o.get('error_code'),
            'error_message': o.get('error_message'),
            'pushed_values': self.raw_rows.get(self.as_key),
        } for o in outcomes]


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
    snapshot_cols = [status, title, body, desc, cta, link, d_link]

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.raw_rows = {}
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
        raw = df.copy()
        raw[self.key] = (raw[self.cam_name] + raw[self.adset_name] +
                         raw[self.name])
        self.raw_rows = {
            k: utl.snapshot_values(v, self.snapshot_cols)
            for k, v in raw.set_index(self.key).to_dict(
                orient='index').items()}
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
            if str(self.config[k][item]) == 'nan':
                self.config[k][item] = ''
            self.config[k][item] = self.config[k][item].split('|')
            if item == self.filename:
                self.config[k][self.filename] = [x.split('::') for x in
                                                 self.config[k][self.filename]]

    def set_ad(self, ad):
        self.ad_key = ad
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
        self.ad_status = self.config[ad][self.status]
        if self.view_tag in self.config[ad]:
            self.ad_view_tag = self.config[ad][self.view_tag]
        else:
            self.ad_view_tag = ''
        self.ad_status = self.config[ad][self.status]

    def upload_all_creatives(self, api, creative_class):
        creatives = list(set(y for k in self.config for x in
                             self.config[k][self.filename] for y in x))
        images = [x for x in creatives
                  if x.split('.')[-1].lower() in utl.static_types]
        videos = [x for x in creatives if x not in images]
        creative_class.upload_all_creatives(api, images, videos)
        self.creative_filename_to_hash(table=creative_class.table)
        # self.add_thumbnail_images(api, videos, table=creative_class.table)

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
            if not api.cam_dict:
                api.set_id_name_dict(Campaign)
            if not api.adset_dict:
                campaign_names = [v['campaign_name'][0] for k, v in
                                  self.config.items()]
                campaign_ids = [x['id'] for x in api.cam_dict if
                                x['name'] in campaign_names]
                api.set_id_name_dict(AdSet, parent_ids=campaign_ids)
            adset_names = [v['adset_name'][0] for k, v in self.config.items()]
            adset_ids = [x['id'] for x in api.adset_dict
                         if x['name'] in adset_names]
            api.set_id_name_dict(Ad, parent_ids=adset_ids)
        total_ads = str(len(self.config))
        results = []
        for idx, ad in enumerate(self.config):
            logging.info('Uploading ad ' + str(idx + 1) + ' of ' + total_ads +
                         '.  Ad Name: ' + ad)
            results.extend(self.upload_ad(ad, api))
        return results

    def upload_ad(self, ad, api):
        self.set_ad(ad)
        return self.format_ad(api)

    def _ad_skip_result(self, message):
        return [{
            'source_name': self.ad_name,
            'object_level': 'Ad',
            'uploader_type': 'Facebook',
            'platform_id': None,
            'parent_platform_id': None,
            'status': 'skipped_dep_missing',
            'error_code': None,
            'error_message': message,
            'pushed_values': self.raw_rows.get(self.ad_key),
        }]

    def format_ad(self, api):
        cids = api.campaign_to_id(self.ad_cam_name)
        asids = api.adset_to_id(self.ad_adset_name, cids)
        if not cids:
            msg = '{} does not exist in the account. {} was not uploaded.' \
                .format(self.ad_cam_name, self.ad_name)
            logging.warning(msg)
            return self._ad_skip_result(msg)
        if not asids:
            msg = '{} does not exist in the account. {} was not uploaded.' \
                .format(self.ad_adset_name, self.ad_name)
            logging.warning(msg)
            return self._ad_skip_result(msg)
        outcomes = []
        if len(self.ad_filename) == 1 and len(self.ad_filename[0]) == 1:
            outcomes = api.create_ad(
                self.ad_name, asids, self.ad_title[0],
                self.ad_body, self.ad_desc[0], self.ad_cta,
                self.ad_d_link, self.ad_link[0], self.ad_prom_page,
                self.ad_ig_id, self.ad_view_tag, self.ad_status,
                self.ad_filename[0][0])
        elif len(self.ad_filename) == 1 and len(self.ad_filename[0]) == 2:
            outcomes = api.create_ad(
                self.ad_name, asids, self.ad_title[0], self.ad_body,
                self.ad_desc[0], self.ad_cta, self.ad_d_link,
                self.ad_link[0], self.ad_prom_page, self.ad_ig_id,
                self.ad_view_tag, self.ad_status,
                self.ad_filename[0][1],
                vid_id=self.ad_filename[0][0])
        elif len(self.ad_filename) > 1:
            outcomes = api.create_ad(
                self.ad_name, asids, self.ad_title, self.ad_body,
                self.ad_desc, self.ad_cta, self.ad_d_link,
                self.ad_link, self.ad_prom_page, self.ad_ig_id,
                self.ad_view_tag, self.ad_status,
                self.ad_filename)
        outcomes = outcomes or []
        return [{
            'source_name': self.ad_name,
            'object_level': 'Ad',
            'uploader_type': 'Facebook',
            'platform_id': o.get('platform_id'),
            'parent_platform_id': o.get('parent_platform_id'),
            'status': o.get('status') or 'failed',
            'error_code': o.get('error_code'),
            'error_message': o.get('error_message'),
            'pushed_values': self.raw_rows.get(self.ad_key),
        } for o in outcomes]


# Spreadsheet-space column -> ``api_update`` field per level, for
# ``FbApi.update_object``. Defined after the Upload classes so the
# column vocabulary is spelled once (their constants). The adset
# budget maps to no fixed field — ``update_param`` routes it to
# daily/lifetime via the row's ``adset_budget_type``.
UPDATE_COLUMN_FIELDS = {
    'Campaign': {
        CampaignUpload.name: Campaign.Field.name,
        CampaignUpload.spend_cap: Campaign.Field.spend_cap,
        CampaignUpload.status: Campaign.Field.status,
    },
    'Adset': {
        AdSetUpload.name: AdSet.Field.name,
        AdSetUpload.budget_value: None,
        AdSetUpload.bid: AdSet.Field.bid_amount,
        AdSetUpload.start_time: AdSet.Field.start_time,
        AdSetUpload.end_time: AdSet.Field.end_time,
        AdSetUpload.status: AdSet.Field.status,
    },
    'Ad': {
        AdUpload.name: Ad.Field.name,
        AdUpload.status: Ad.Field.status,
    },
}

# Columns holding dollars in spreadsheet space — pushed as x100 cents.
MONEY_UPDATE_COLS = (CampaignUpload.spend_cap, AdSetUpload.budget_value,
                     AdSetUpload.bid)


def money_cents(col, value):
    """Integer cents for a dollars-in-spreadsheet-space amount.

    Decimal, not float: a budget arrives as text or an Excel float,
    and ``round(19.99 * 100)`` is decided by a binary representation
    that cannot hold 19.99 exactly. Money going to a live platform
    rounds half-up on the decimal digits instead, so what the user
    typed is what gets billed.

    :param col: Spreadsheet-space column name, for the error message.
    :param value: The amount, as text or a number.
    :returns: The amount in whole cents.
    :raises ValueError: When the value is not a number.
    """
    try:
        dollars = decimal.Decimal(str(value).strip().replace(',', ''))
    except (AttributeError, TypeError, decimal.InvalidOperation):
        raise ValueError('{} is not a number: {!r}'.format(col, value))
    return int((dollars * 100).quantize(
        decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP))

# Columns holding flight datetimes — day-bounded + tz-suffixed like
# the create path.
DATE_UPDATE_COLS = (AdSetUpload.start_time, AdSetUpload.end_time)


class Creative(object):
    """Facebook creative store (filename -> asset hash). This is the
    production reference the shared ``utils.BaseCreativeStore`` was
    extracted from; FB keeps its own ``{path: hash}`` CSV format so
    existing ``creative_hashes.csv`` files stay valid, while AW / DCM /
    Reddit use the shared base.
    """

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
