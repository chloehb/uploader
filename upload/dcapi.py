import os
import re
import sys
import json
import time
import logging
import requests
import pandas as pd
import upload.utils as utl
from requests_oauthlib import OAuth2Session

dcm_path = 'dcm'
config_path = os.path.join(utl.config_file_path, dcm_path)

base_url = 'https://www.googleapis.com/dfareporting'

# Tracking ads need no creative — used for 1x1 placements whose tags
# are trafficked into other platforms (FB/AW/Reddit).
TRACKING_AD_TYPE = 'AD_SERVING_TRACKING'

# Extension -> creativeAssets assetIdentifier type. Images cover the
# standard display path; anything else best-efforts to HTML.
_ASSET_TYPE_BY_EXT = {
    '.png': 'HTML_IMAGE', '.jpg': 'HTML_IMAGE', '.jpeg': 'HTML_IMAGE',
    '.gif': 'HTML_IMAGE', '.html': 'HTML', '.zip': 'HTML',
}

_SIZE_TOKEN = re.compile(r'(\d{2,4})\s*[xX]\s*(\d{2,4})')

CAMPAIGN_ID_DICTS = {'placement': 'place_dict', 'ad': 'ad_dict',
                     'creative': 'creative_dict'}

_LIVE_ENTITIES = {
    'Campaign': ('campaigns', None, 'advertiserId'),
    'Adset': ('placements', 'campaignIds', 'campaignId'),
    'Ad': ('ads', 'placementIds', 'placementAssignments.0.placementId'),
}


def campaign_lookup(api, dcm_object, campaign_id):
    """``api``'s ``dcm_object`` name lookup for ``campaign_id``, re-listed
    when another campaign of the run filled it; one set by hand is kept."""
    attr = CAMPAIGN_ID_DICTS[dcm_object]
    scope = getattr(api, 'id_dict_scope', {}).get(attr, str(campaign_id))
    if scope != str(campaign_id) or not getattr(api, attr):
        api.set_id_dict(dcm_object=dcm_object, filter_id=campaign_id)
    return getattr(api, attr)


def _asset_type_for(file_name):
    ext = os.path.splitext(str(file_name))[1].lower()
    return _ASSET_TYPE_BY_EXT.get(ext, 'HTML_IMAGE')


def _size_from_name(file_name):
    """``{'width', 'height'}`` from the ad-ops size token in a creative
    filename (e.g. ``banner_300x250.jpg``), or ``None`` — DCM can also
    detect size from the image asset itself."""
    match = _SIZE_TOKEN.search(str(file_name))
    if not match:
        return None
    return {'width': int(match.group(1)), 'height': int(match.group(2))}


def _multipart_related(metadata, media, boundary='lqapp_creative'):
    """``(body, content_type)`` for a Google ``uploadType=multipart``
    request: a JSON metadata part plus the media bytes. Built by hand —
    requests only generates multipart/form-data."""
    parts = [
        '--{}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n'
        '{}\r\n'.format(boundary, json.dumps(metadata)).encode(),
        '--{}\r\nContent-Type: application/octet-stream\r\n\r\n'.format(
            boundary).encode() + media + b'\r\n',
        '--{}--\r\n'.format(boundary).encode(),
    ]
    return b''.join(parts), 'multipart/related; boundary={}'.format(boundary)


def _fail_from_dcm(result, body, http_status=None):
    """Apply DCM's error envelope to a result."""
    err = body.get('error') or {}
    return utl.fail_result(
        result, err.get('message') or 'Unknown error from DCM',
        err.get('code') or http_status)


def _fail_if_refused(result, response):
    """Mark a result failed when DCM refuses the request.

    An HTTP refusal often carries no error envelope, so a body-only
    check would read a rejected write back as a success.
    """
    body = utl.response_body(response)
    status = getattr(response, 'status_code', None)
    if response is not None and not body.get('error') and (status or 0) < 400:
        return result
    return _fail_from_dcm(result, body, status)


def _populate_dcm_result(result, response):
    """Fill ``result`` with platform_id / status / error from a DCM
    create response. Mirrors ``awapi._populate_aw_result``.
    """
    body = utl.response_body(response)
    if 'id' in body:
        result['platform_id'] = body['id']
        result['status'] = 'created'
        return
    _fail_from_dcm(
        result, body, getattr(response, 'status_code', None))


def _to_dcm_date(col, value):
    """Normalize a spreadsheet value to CM360's date format."""
    parsed = pd.to_datetime(str(value), errors='coerce')
    if pd.isnull(parsed):
        raise ValueError(f'Unparseable date for {col}: {value!r}')
    return parsed.strftime('%Y-%m-%d')


class DcApi(object):
    version = '5'
    token_ttl = 3000

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
        self.client_time = 0
        self.lp_dict = {}
        self.site_dict = {}
        self.cam_dict = {}
        self.place_dict = {}
        self.tag_dict = {}
        self.ad_dict = {}
        self.creative_dict = {}
        self.directory_site_dict = {}
        self.id_dict_scope = {}
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
        """Refresh the OAuth token and build the session, which
        :meth:`make_request` reuses for ``token_ttl`` seconds -- Google's
        token lives an hour, and refreshing per request doubled latency.
        """
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
        self.client_time = time.time()

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
        # size/tagFormats let the ad level detect tracking placements.
        fields = {'id': 'id', 'name': 'name', 'size': 'size',
                  'tagFormats': 'tagFormats'}
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

    def get_creative_id_dict(self, advertiser_id=None, campaign_id=None):
        parent = {'advertiserId': 'advertiserId'}
        fields = {'id': 'id', 'name': 'name'}
        request_filter = {}
        if campaign_id:
            request_filter['campaignId'] = campaign_id
        elif advertiser_id:
            request_filter['advertiserId'] = advertiser_id
        return self.get_id_dict(
            entity='creatives', parent=parent, fields=fields,
            resp_entity='creatives',
            request_filter=request_filter or None)

    def get_ad_id_dict(self, campaign_id=None):
        parent = {'campaignId': 'campaignId'}
        fields = {'id': 'id', 'name': 'name'}
        request_filter = {'campaignIds': campaign_id} if campaign_id else None
        return self.get_id_dict(
            entity='ads', parent=parent, fields=fields,
            resp_entity='ads', request_filter=request_filter)

    def set_id_dict(self, dcm_object=None, filter_id=None):
        if dcm_object in CAMPAIGN_ID_DICTS:
            self.id_dict_scope[CAMPAIGN_ID_DICTS[dcm_object]] = str(filter_id)
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
        if dcm_object == 'creative':
            self.creative_dict = self.get_creative_id_dict(
                campaign_id=filter_id)
        if dcm_object == 'ad':
            self.ad_dict = self.get_ad_id_dict(campaign_id=filter_id)

    def list_recent(self, level, limit=25, parent_ids=None):
        """The profile's ``level`` objects for the copy-from-account
        picker: one page by id descending (CM360 sorts by ID or NAME
        only), then by change time where the rows carry one."""
        if level not in _LIVE_ENTITIES:
            return []
        entity, parent_filter, parent_key = _LIVE_ENTITIES[level]
        params = {'sortField': 'ID', 'sortOrder': 'DESCENDING',
                  'maxResults': limit}
        if parent_ids and parent_filter:
            params[parent_filter] = [str(x) for x in parent_ids]
        response = self.make_request(self.create_url(entity), method='get',
                                     params=params)
        return utl.recent_rows(
            utl.response_body(response).get(entity) or [], limit,
            parent_key=parent_key, created_key='createInfo.time',
            updated_key='lastModifiedInfo.time')

    def create_entity(self, entity, entity_name=''):
        url = self.create_url(entity_name)
        r = self.make_request(url, method='post', body=entity.upload_dict)
        if 'error' in r.json():
            msg = '{} not uploaded. \n Response: {} \n Body: {}'.format(
                entity_name, r.json(), entity.upload_dict)
            logging.warning(msg)
        return r

    def make_request(self, url, method, params=None, body=None,
                     rebuilt=False):
        """Send one request, building the session when it is missing or
        its token is nearly spent. A connection the server closed while
        idle rebuilds the session and retries once; SSLError keeps its own
        branch above, being a handshake to wait out, not a dead socket.

        :param rebuilt: the retry has happened; raise instead of looping
        :return: the response
        """
        if not self.client or time.time() - self.client_time > self.token_ttl:
            self.get_client()
        try:
            self.r = self.raw_request(url, method, params, body)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url, method, params, body)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            if rebuilt:
                raise
            logging.warning(
                'Connection dropped, rebuilding session: {}'.format(e))
            self.get_client()
            self.r = self.make_request(url, method, params, body,
                                       rebuilt=True)
        return self.r

    def get_account_id(self):
        """The CM360 account/network id for the configured user profile,
        resolved from the userprofiles resource (``None`` on failure).
        Used to build modern trafficking deep-links, which key on the
        network account rather than the advertiser."""
        try:
            r = self.make_request(self.create_url(), method='get')
            body = r.json() if r is not None else {}
            return (body or {}).get('accountId') if isinstance(
                body, dict) else None
        except Exception:
            return None

    def probe_account(self):
        """(ok, message) — verify the user profile is reachable, for
        the live pre-flight checks."""
        try:
            r = self.make_request(
                self.create_url('accountPermissions'), method='get')
            body = r.json() if r is not None else {}
            err = (body or {}).get('error')
            if err:
                return False, str(err.get('message') or err)
            return True, ''
        except Exception as e:
            return False, str(e)

    def update_statuses(self, object_level, platform_ids, activate=True):
        """Toggle DCM ads' ``active`` flag. Campaigns and placements
        have no paused state — Ad is the only level with one. Returns
        one dict per id: {'platform_id', 'status'
        ('updated'|'failed'), 'error_code', 'error_message'}."""
        if object_level != 'Ad':
            msg = (f'DCM {object_level} objects have no paused state — '
                   'only ads can be toggled.')
            return [utl.fail_result(utl.new_update_result(pid), msg)
                    for pid in platform_ids]
        url = self.create_url('ads')
        results = []
        for pid in platform_ids:
            result = utl.new_update_result(pid)
            try:
                _fail_if_refused(result, self.make_request(
                    url, method='patch', params={'id': pid},
                    body={'active': bool(activate)}))
            except Exception as e:
                utl.fail_result(result, e)
            results.append(result)
        return results

    update_segments_by_level = {'Campaign': 'campaigns',
                                'Adset': 'placements'}

    def update_object(self, object_level, platform_id, changes,
                      context=None):
        """Push one object's whitelisted edits in a single PATCH."""
        result = utl.new_update_result(platform_id)
        try:
            segment, body = self.update_request(
                object_level, platform_id, changes)
            _fail_if_refused(result, self.make_request(
                self.create_url(segment), method='patch',
                params={'id': platform_id}, body=body))
        except Exception as e:
            utl.fail_result(result, e)
        return result

    def update_request(self, object_level, platform_id, changes):
        """Build a complete ``(collection, body)`` update request."""
        segment = self.update_segments_by_level.get(object_level)
        fields = UPDATE_COLUMN_FIELDS.get(object_level)
        if not segment or not fields:
            raise ValueError(
                f'No update support for DCM level: {object_level}')
        body = {}
        dates = {}
        for col, value in (changes or {}).items():
            if col not in fields:
                raise ValueError(f'Column not updatable: {col}')
            if col in DATE_UPDATE_COLS:
                dates[fields[col]] = _to_dcm_date(col, value)
            else:
                body[fields[col]] = str(value)
        if not body and not dates:
            raise ValueError('No changes supplied.')
        # Preserve every pricingSchedule field when changing placement dates.
        if dates and object_level == 'Adset':
            body['pricingSchedule'] = self._live_pricing_schedule(
                platform_id, dates)
        elif dates:
            body.update(dates)
        return segment, body

    def _live_pricing_schedule(self, platform_id, dates):
        """Overlay dates on a placement's complete pricing schedule."""
        placement = self.get_placement(platform_id)
        schedule = placement.get('pricingSchedule')
        if not isinstance(schedule, dict) or not schedule:
            err = placement.get('error') or {}
            detail = (err.get('message')
                      or 'no pricingSchedule in the response')
            raise ValueError(
                'Could not read the live placement to update its '
                f'dates ({detail}) — refusing to guess the '
                'pricingSchedule.')
        schedule = dict(schedule)
        schedule.update(dates)
        return schedule

    def get_placement(self, platform_id):
        """One placement resource by id (``placements/{id}``), parsed
        to a dict (empty on an unparseable response)."""
        url = self.create_url(f'placements/{platform_id}')
        return utl.response_body(self.make_request(url, method='get'))

    def raw_request(self, url, method, params=None, body=None):
        if not params:
            params = {}
        if body:
            if method == 'get':
                self.r = self.client.get(url, params=params, json=body)
            elif method == 'post':
                self.r = self.client.post(url, params=params, json=body)
            elif method == 'patch':
                self.r = self.client.patch(url, params=params, json=body)
        else:
            if method == 'get':
                self.r = self.client.get(url, params=params)
            elif method == 'post':
                self.r = self.client.post(url, params=params)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)

    def upload_creative(self, file_path, advertiser_id=None):
        """Insert a creative asset (multipart metadata + media) and
        wrap it in a DISPLAY creative named after the file, so ads can
        reference it. Returns ``{'id': creative_id, 'asset_name': ..}``.
        Wire format documented best-effort — validate on a real account
        before relying on it in live trafficking.
        """
        file_name = os.path.basename(file_path)
        metadata = Asset(file_name, _asset_type_for(file_name)).upload_dict
        upload_base = base_url.replace(
            'www.googleapis.com/dfareporting',
            'www.googleapis.com/upload/dfareporting')
        url = ('{}/v{}/userprofiles/{}/creativeAssets/{}/creativeAssets'
               '?uploadType=multipart'.format(
                   upload_base, self.version, self.usr_id,
                   advertiser_id or ''))
        self.get_client()
        with open(file_path, 'rb') as f:
            body, content_type = _multipart_related(metadata, f.read())
        r = self.client.post(
            url, data=body, headers={'Content-Type': content_type})
        try:
            resp = r.json() if r is not None else {}
        except (ValueError, AttributeError):
            resp = {}
        if not isinstance(resp, dict):
            resp = {}
        asset_ident = (resp.get('assetIdentifier')
                       or metadata['assetIdentifier'])
        creative_id = self.create_creative(
            file_name, asset_ident, advertiser_id,
            size=_size_from_name(file_name))
        return {'id': creative_id, 'asset_name': asset_ident.get('name')}

    def create_creative(self, name, asset_identifier, advertiser_id,
                        size=None):
        """Insert a DISPLAY creative wrapping an uploaded asset and
        return its id (``None`` on failure)."""
        body = {
            'name': str(name),
            'type': 'DISPLAY',
            'active': True,
            'creativeAssets': [{
                'assetIdentifier': asset_identifier,
                'role': 'PRIMARY',
            }],
        }
        if advertiser_id:
            body['advertiserId'] = int(advertiser_id)
        if size:
            body['size'] = size
        url = self.create_url('creatives')
        r = self.make_request(url, method='post', body=body)
        try:
            resp = r.json() if r is not None else {}
        except (ValueError, AttributeError):
            resp = {}
        if not isinstance(resp, dict) or 'id' not in resp:
            logging.warning(
                'Creative {} not created. Response: {}'.format(
                    name, resp))
            return None
        return resp['id']


class CampaignUpload(object):
    name = 'name'
    advertiserId = 'advertiserId'
    defaultLandingPage = 'defaultLandingPage'
    sd = 'startDate'
    ed = 'endDate'
    snapshot_cols = [advertiserId, defaultLandingPage, sd, ed]

    @staticmethod
    def settings_from_live(fields):
        """The live campaign's advertiser as a level-file cell; the
        landing page and dates are the plan's."""
        return utl.live_settings(
            {CampaignUpload.advertiserId: fields.get('advertiserId')})

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        if self.name not in df.columns:
            logging.warning(
                '{} column missing from {}.  Skipping load.'.format(
                    self.name, config_file))
            self.config = {}
            return
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
        results = []
        for idx, c_id in enumerate(self.config):
            logging.info('Uploading campaign {} of {}.  '
                         'Campaign Name: {}'.format(idx + 1, total_camp, c_id))
            result = self.upload_campaign(api, c_id)
            result['pushed_values'] = utl.snapshot_values(
                self.config[c_id], self.snapshot_cols)
            results.append(result)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        return results

    def upload_campaign(self, api, campaign_id):
        campaign = self.set_campaign(campaign_id, api)
        result = {
            'source_name': campaign.name,
            'object_level': 'Campaign',
            'uploader_type': 'DCM',
            'platform_id': None,
            'parent_platform_id': None,
            'status': None,
            'error_code': None,
            'error_message': None,
        }
        if not campaign.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                'Missing advertiserId or defaultLandingPage')
            return result
        if campaign.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = campaign.id
            return result
        _populate_dcm_result(
            result, api.create_entity(campaign, entity_name='campaigns'))
        if result['status'] == 'created':
            campaign.id = result['platform_id']
        return result


class Campaign(object):
    __slots__ = ['name', 'advertiserId', 'archived', 'defaultLandingPageId',
                 'startDate', 'endDate', 'upload_dict', 'api', 'id', 'upload',
                 'defaultLandingPage']

    def __init__(self, cam_dict, api=None, upload=True):
        self.defaultLandingPageId = None
        self.defaultLandingPage = None
        self.advertiserId = None
        self.id = None
        self.upload = upload
        for k in cam_dict:
            try:
                setattr(self, k, cam_dict[k])
            except AttributeError as e:
                logging.warning('AttributeError: {}'.format(e))
                continue
        self.api = api
        if self.api:
            self.get_landing_page_id(self.api)
        if self.upload:
            self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        if not self.advertiserId:
            logging.warning('{} needs advertiserId'.format(self.name))
            return {}
        if not self.defaultLandingPageId:
            logging.warning(
                '{} has no defaultLandingPageId; skipping upload. '
                'Set defaultLandingPage on the campaign row.'.format(
                    self.name))
            return {}
        cam_dict = {
            'name': '{}'.format(self.name),
            'archived': '{}'.format('false'),
            'startDate': '{}'.format(self.startDate),
            'endDate': '{}'.format(self.endDate),
            'advertiserId': int(self.advertiserId),
            'defaultLandingPageId': int(self.defaultLandingPageId),
            'euPoliticalAdsDeclaration': 'DOES_NOT_CONTAIN_EU_POLITICAL_ADS'
        }
        return cam_dict

    def get_landing_page_id(self, api):
        if not self.defaultLandingPage:
            logging.warning(
                '{} has no defaultLandingPage; cannot resolve a '
                'landing page id. Campaign will be skipped.'.format(
                    self.name))
            return
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
        ad_exists = False
        self.set_id(api)
        if self.id:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            ad_exists = True
        return ad_exists


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
        if not self.name or not self.url:
            logging.warning(
                'Landing page is missing name or url '
                '(name={!r}, url={!r}); cannot look up or create.'.format(
                    self.name, self.url))
            return
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
        resp = r.json()
        if 'id' in resp:
            self.id = resp['id']
        else:
            logging.warning(
                'Landing page upload did not return an id for '
                '{!r}. DCM response: {}'.format(self.name, resp))


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
    snapshot_cols = [site, startDate, endDate, pricingType, compatibility]

    @staticmethod
    def settings_from_live(fields):
        """The live placement's compatibility, payment source and first
        tag format as level-file cells; site, size and pricing are each
        placement's own."""
        pu = PlacementUpload
        return utl.live_settings({
            pu.compatibility: fields.get('compatibility'),
            pu.paymentSource: fields.get('paymentSource'),
            pu.tagFormats: utl.dig(fields, 'tagFormats.0')})

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
            msg = 'Could not set config, {} does not exist'.format(file_name)
            logging.warning(msg)
            return False
        df = utl.read_excel(file_name)
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        df = self.format_size(df)
        df = utl.data_to_type(df, date_col=[self.startDate, self.endDate])
        for col in [self.startDate, self.endDate]:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        self.config = df.to_dict(orient='index')
        return True

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
        results = []
        for idx, p_id in enumerate(self.config):
            placement = self.set_placement(p_id, api)
            logging.info('Uploading placement {} of {}.  '
                         'Placement Name: {}'.format(idx + 1, total_placements,
                                                     placement.name))
            result = self.upload_placement(api, placement)
            result['pushed_values'] = utl.snapshot_values(
                self.config[p_id], self.snapshot_cols)
            results.append(result)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        self.attach_placement_tags(api, results)
        return results

    @staticmethod
    def attach_placement_tags(api, results):
        """Enrich each placement result with its DCM click tag.

        Tags are the artifact trafficking teams copy into other
        systems, so surface them on the run. Tags are fetched per
        campaign (the API filters by campaignId); a fetch failure is
        logged and skipped — tags are an enrichment, never block the
        run results.
        """
        campaign_ids = {r.get('parent_platform_id') for r in results
                        if r.get('parent_platform_id')}
        tags_by_placement = {}
        for campaign_id in campaign_ids:
            try:
                tag_dict = PlacementUpload.generate_dcm_tags(api, campaign_id)
            except Exception as e:
                logging.warning(
                    'Could not fetch DCM tags for campaign {}: {}'.format(
                        campaign_id, e))
                continue
            for placement_id, data in tag_dict.items():
                tags_by_placement[str(placement_id)] = data.get('clickTag')
        for r in results:
            tag = tags_by_placement.get(str(r.get('platform_id')))
            if tag:
                r['tag'] = tag
        return results

    @staticmethod
    def upload_placement(api, placement):
        result = {
            'source_name': placement.name,
            'object_level': 'Adset',
            'uploader_type': 'DCM',
            'platform_id': None,
            'parent_platform_id': (
                str(placement.campaignId)
                if placement.campaignId else None),
            'status': None,
            'error_code': None,
            'error_message': None,
        }
        if not placement.campaignId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                'Campaign {!r} not found in account'.format(
                    placement.campaign))
            return result
        if not placement.siteId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                'Site {!r} not found in DCM (or the site column is '
                'missing from the upload file)'.format(placement.site))
            return result
        if placement.check_exists(api):
            existing = api.get_id(api.place_dict, placement.name)
            if existing:
                result['platform_id'] = existing[0]
            result['status'] = 'skipped_exists'
            return result
        _populate_dcm_result(
            result,
            api.create_entity(placement, entity_name='placements'))
        return result

    @staticmethod
    def generate_dcm_tags(api, campaign_id):
        """
        Grabs tags for placements with the specified campaign_id.  Returns as
        a dictionary.

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
        # Default every slot — a column missing from the upload file
        # must produce a per-row failure, not an AttributeError that
        # kills the whole run.
        for attr in self.__slots__:
            setattr(self, attr, None)
        self.upload = upload
        for k in cam_dict:
            try:
                setattr(self, k, cam_dict[k])
            except AttributeError as e:
                logging.warning('AttributeError: {}'.format(e))
                continue
        self.api = api
        if self.api:
            self.get_site_id(self.api)
            self.get_campaign_id(self.api)
        if self.upload:
            self.upload_dict = self.create_p_dict()

    def create_p_dict(self):
        if self.campaignId:
            cid = int(self.campaignId)
        else:
            logging.warning('{} needs campaignId'.format(self.name))
            cid = ''
        if not self.tagFormats:
            self.tagFormats = 'PLACEMENT_TAG_TRACKING'
        if not self.compatibility:
            self.compatibility = 'DISPLAY'
        p_dict = {
            'name': '{}'.format(self.name),
            'campaignId': cid,
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
        if not self.site:
            logging.warning('{} has no site; cannot resolve a site '
                            'id.'.format(self.name))
            return
        site = Site({'name': '{}'.format(self.site)}, api=api)
        self.siteId = site.id

    def check_exists(self, api):
        if not self.campaignId:
            logging.warning(
                'Placement {!r} has no campaignId (campaign {!r} '
                'was not found in account); skipping existence '
                'check and upload.'.format(self.name, self.campaign))
            return True
        campaign_lookup(api, 'placement', self.campaignId)
        pid = api.get_id(api.place_dict, self.name)
        if pid:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True
        return False

    def get_campaign_id(self, api):
        campaign = Campaign({'name': self.campaign}, upload=False)
        campaign.set_id(api)
        if campaign.id is None:
            logging.warning(
                'Campaign {!r} not found for placement {!r}; '
                'placement will be skipped.'.format(
                    self.campaign, self.name))
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
    file_name = 'ad_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    placement = 'placement'
    creative = 'creative'
    active = 'active'
    type = 'type'
    startTime = 'startTime'
    endTime = 'endTime'
    clickThroughUrl = 'clickThroughUrl'
    # Relation-picker keys (DCM API field names).
    campaignId = 'campaignId'
    creativeRotation = 'creativeRotation'
    deliverySchedule = 'deliverySchedule'
    placementAssignments = 'placementAssignments'
    snapshot_cols = [active, type, startTime, endTime, creative]

    @staticmethod
    def settings_from_live(fields):
        """The live ad's type as a level-file cell; creative,
        placements, click-through and status are each ad's own."""
        return utl.live_settings({AdUpload.type: fields.get('type')})

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
            logging.warning(f'Ad config missing: {file_name}')
            return False
        df = utl.read_excel(file_name)
        df = df.dropna(subset=[self.name]).fillna('')
        df = utl.data_to_type(
            df, date_col=[self.startTime, self.endTime])
        for col in [self.startTime, self.endTime]:
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S-07:00')
        self.config = df.to_dict(orient='index')
        return True

    def set_ad(self, ad_id, api=None):
        return Ad(self.config[ad_id], api=api)

    def _advertiser_id_for_ads(self, api):
        """advertiserId from the first resolvable campaign among this
        run's ads — ``cam_dict`` rows carry it as the parent field."""
        if not api.cam_dict:
            api.set_id_dict(dcm_object='campaign')
        for a_id in self.config:
            cam_name = self.config[a_id].get(self.campaign)
            found = api.get_id(api.cam_dict, cam_name)
            if found:
                return (api.cam_dict.get(found[0]) or {}).get(
                    'advertiserId')
        return None

    def upload_all_creatives(self, api):
        """Upload local creative files referenced by the ads' creative
        column (asset -> DISPLAY creative) before ad creation. Creative
        names without a matching local file resolve on-platform by name
        as before. Mirrors ``awapi.AdUpload.upload_all_creatives``."""
        cu = CreativeUpload()
        if not self.config:
            return cu
        names = {str(row[self.creative]) for row in self.config.values()
                 if row.get(self.creative)}
        local = [n for n in names
                 if os.path.isfile(os.path.join(cu.creative_path, n))]
        if not local:
            return cu
        cu.advertiser_id = self._advertiser_id_for_ads(api)
        cu.upload_all(api, local)
        return cu

    def upload_all_ads(self, api):
        if not self.config:
            return []
        cu = self.upload_all_creatives(api)
        total = len(self.config)
        results = []
        for idx, a_id in enumerate(self.config):
            ad = self.set_ad(a_id, api)
            if not ad.creativeId and ad.creative:
                # Fresh creatives miss the campaign-scoped listing.
                ad.creativeId = cu.get_id(str(ad.creative))
                if ad.creativeId:
                    ad.upload_dict = ad.create_ad_dict()
            if (not ad.creativeId and not ad.is_tracking()
                    and ad.campaignId and ad.placementIds
                    and ad.placements_all_tracking(api)):
                logging.info(
                    'Creative %r not found and all placements are '
                    '1x1/tracking - creating %r as tracking ad.',
                    ad.creative, ad.name)
                ad.convert_to_tracking()
            logging.info(
                f'Uploading ad {idx + 1} of {total}. Ad Name: {ad.name}')
            result = self.upload_ad(api, ad)
            result['pushed_values'] = utl.snapshot_values(
                self.config[a_id], self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_ad(api, ad):
        result = {
            'source_name': ad.name,
            'object_level': 'Ad',
            'uploader_type': 'DCM',
            'platform_id': None,
            'parent_platform_id': (
                str(ad.campaignId) if ad.campaignId else None),
            'status': None,
            'error_code': None,
            'error_message': None,
        }
        if not ad.campaignId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Campaign {ad.campaign!r} not found in account')
            return result
        if not ad.placementIds:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'No placements resolved for {ad.name!r}')
            return result
        if not ad.creativeId and not ad.is_tracking():
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Creative {ad.creative!r} not found in advertiser '
                f'and placements are not 1x1/tracking, so no tracking '
                f'ad was auto-created')
            return result
        if ad.check_exists(api):
            existing = api.get_id(api.ad_dict, ad.name)
            if existing:
                result['platform_id'] = existing[0]
            result['status'] = 'skipped_exists'
            return result
        _populate_dcm_result(
            result, api.create_entity(ad, entity_name='ads'))
        if (result['status'] == 'created' and ad.is_tracking()
                and not ad.creativeId):
            result['error_message'] = (
                'Created as tracking ad (no creative needed)')
        return result


class Ad(object):
    __slots__ = ['name', 'campaign', 'campaignId', 'placement',
                 'placementId', 'placementIds', 'creative', 'creativeId',
                 'active', 'type', 'startTime', 'endTime', 'upload_dict',
                 'api', 'upload', 'clickThroughUrl']

    def __init__(self, row_dict, api=None, upload=True):
        self.name = None
        self.campaign = None
        self.campaignId = None
        self.placement = None
        self.placementId = None
        self.placementIds = []
        self.creative = None
        self.creativeId = None
        self.active = True
        self.type = 'AD_SERVING_STANDARD_AD'
        self.startTime = None
        self.endTime = None
        self.clickThroughUrl = None
        self.upload = upload
        for k in row_dict:
            try:
                setattr(self, k, row_dict[k])
            except AttributeError as e:
                logging.warning('AttributeError: {}'.format(e))
                continue
        self.api = api
        if self.api:
            self.resolve_ids(self.api)
        if self.upload:
            self.upload_dict = self.create_ad_dict()

    def resolve_ids(self, api):
        if self.campaign:
            cam = Campaign({'name': self.campaign}, upload=False)
            cam.set_id(api)
            self.campaignId = cam.id
        if not self.campaignId:
            return
        campaign_lookup(api, 'placement', self.campaignId)
        # DCM convention: the ad is named after its placement.
        placement_names = [
            p.strip() for p in str(self.placement or self.name or '')
            .split('|') if p.strip()]
        self.placementIds = []
        for pname in placement_names:
            pid = api.get_id(api.place_dict, pname)
            if pid:
                self.placementIds.append(pid[0])
        if not self.placementIds and self.placementId:
            for p in str(self.placementId).split('|'):
                p = p.strip()
                if not p:
                    continue
                try:
                    self.placementIds.append(int(float(p)))
                except ValueError:
                    logging.warning(
                        f'{self.name}: bad placementId {p!r}')
        campaign_lookup(api, 'creative', self.campaignId)
        if self.creative:
            cre = api.get_id(api.creative_dict, self.creative)
            if cre:
                self.creativeId = cre[0]

    def is_tracking(self):
        return str(self.type) == TRACKING_AD_TYPE

    @staticmethod
    def _placement_is_tracking(place):
        """A placement record is tracking-eligible when it is 1x1 or
        every tagFormat is a tracking format. Missing both signals
        (stale place_dict from an old fetch) -> not eligible."""
        size = place.get('size') or {}
        try:
            if (int(size.get('width', 0)) == 1
                    and int(size.get('height', 0)) == 1):
                return True
        except (TypeError, ValueError):
            pass
        tag_formats = place.get('tagFormats') or []
        return bool(tag_formats) and all(
            str(f).startswith('PLACEMENT_TAG_TRACKING')
            for f in tag_formats)

    def placements_all_tracking(self, api):
        if not self.placementIds:
            return False
        return all(self._placement_is_tracking(
            api.place_dict.get(pid) or {}) for pid in self.placementIds)

    def convert_to_tracking(self):
        self.type = TRACKING_AD_TYPE
        self.upload_dict = self.create_ad_dict()

    def click_through_url(self):
        """The ad's ClickThroughUrl object. A custom URL requires
        defaultLandingPage false with landingPageId left unset;
        blank inherits the campaign's default landing page, which is
        the shape every ad used before this column existed.

        An ad with no creative gets no per-ad URL: the API carries a
        top-level clickThroughUrl only for AD_SERVING_CLICK_TRACKER,
        and creativeRotation is the sole carrier for the ad types
        this uploader creates."""
        url = str(self.clickThroughUrl or '').strip()
        if not url:
            return {'defaultLandingPage': True}
        return {'defaultLandingPage': False,
                'customClickThroughUrl': url}

    def create_ad_dict(self):
        if not (self.campaignId and self.placementIds):
            return {}
        if not self.creativeId and not self.is_tracking():
            return {}
        ad_dict = {
            'name': str(self.name),
            'campaignId': int(self.campaignId),
            'active': bool(self.active),
            'type': str(self.type),
            'placementAssignments': [
                {'placementId': int(pid), 'active': True}
                for pid in self.placementIds],
        }
        if self.creativeId:
            ad_dict['creativeRotation'] = {
                'creativeAssignments': [{
                    'creativeId': int(self.creativeId),
                    'active': True,
                    'clickThroughUrl': self.click_through_url(),
                }],
            }
        if self.startTime:
            ad_dict['startTime'] = str(self.startTime)
        if self.endTime:
            ad_dict['endTime'] = str(self.endTime)
        return ad_dict

    def check_exists(self, api):
        if not self.campaignId:
            return True
        campaign_lookup(api, 'ad', self.campaignId)
        aid = api.get_id(api.ad_dict, self.name)
        if aid:
            logging.warning(
                f'{self.name} already in account. Not uploaded.')
            return True
        return False


class Creative(object):
    """Resolve a DCM creative by name. Uploading new creatives
    via the multipart asset chain is a follow-up."""
    __slots__ = ['name', 'campaignId', 'advertiserId', 'id', 'api']

    def __init__(self, cre_dict, api=None):
        self.name = None
        self.campaignId = None
        self.advertiserId = None
        self.id = None
        for k in cre_dict:
            try:
                setattr(self, k, cre_dict[k])
            except AttributeError as e:
                logging.warning('AttributeError: {}'.format(e))
                continue
        self.api = api
        if self.api:
            self.set_id(self.api)

    def set_id(self, api):
        campaign_lookup(api, 'creative', self.campaignId)
        if not self.name:
            return
        cid = api.get_id(api.creative_dict, self.name)
        if cid:
            self.id = cid[0]


class Asset(object):
    """Creative-asset identifier wrapper — the metadata part of the
    multipart upload in ``DcApi.upload_creative``."""
    name = 'name'
    type = 'asset_type'

    def __init__(self, name, asset_type):
        self.name = name
        self.asset_type = asset_type
        self.upload_dict = self.create_upload_dict()

    def create_upload_dict(self):
        return {'assetIdentifier': {'name': self.name,
                                    'type': self.asset_type}}

    def upload(self, api):
        logging.info(f'Uploading asset with {self.upload_dict}')
        r = api.create_entity(self, entity_name='creativeAssets')
        body = r.json() if r is not None else {}
        if isinstance(body, dict) and 'id' in body:
            self.id = body['id']


class CreativeUpload(utl.BaseCreativeStore):
    """DCM creative store: filename -> inserted CREATIVE id (plus the
    asset name) in ``dcm_creative_ids.csv``, resolved for ad creation.
    Per-file upload lives on ``DcApi.upload_creative`` (asset ->
    DISPLAY creative); this class is the shared find-new / persist
    bookkeeping only.
    """
    id_cols = ('id', 'asset_name')

    def __init__(self, id_file_name='dcm_creative_ids.csv',
                 creative_path='creative/', advertiser_id=None):
        self.advertiser_id = advertiser_id
        super().__init__(id_file_name, creative_path)

    def _upload_one(self, api, file_path):
        return api.upload_creative(
            file_path, advertiser_id=self.advertiser_id)


# Spreadsheet columns mapped to CM360 PATCH fields.
UPDATE_COLUMN_FIELDS = {
    'Campaign': {
        CampaignUpload.name: 'name',
        CampaignUpload.sd: 'startDate',
        CampaignUpload.ed: 'endDate',
    },
    'Adset': {
        PlacementUpload.name: 'name',
        PlacementUpload.startDate: 'startDate',
        PlacementUpload.endDate: 'endDate',
    },
}

LIVE_UPLOADS = {'Campaign': CampaignUpload, 'Adset': PlacementUpload,
                'Ad': AdUpload}

DATE_UPDATE_COLS = frozenset((
    CampaignUpload.sd, CampaignUpload.ed,
    PlacementUpload.startDate, PlacementUpload.endDate))
