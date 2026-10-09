"""TikTok Business API uploader. Mirrors the redditapi class shape so
the relation system, name-create flow, and run telemetry pick it up
without special-casing. Campaign, AdGroup (Adset) and Ad levels.

Auth is a static Access-Token header (no OAuth refresh dance) shared
with the processor's read client — the config keys match the
processor's tikapi.json so token sharing is a straight copy. Every
response arrives HTTP 200 wrapped in ``{code, message, data}``; only
``code == 0`` is success, so the envelope is checked on every call.

Field names and the enum defaults below come from TikTok's v1.3 spec
(the generated ``tiktok-business-api-sdk`` model docs), confirmed
against a live ad account — see the ``OBSERVED`` comments. Values the
plan supplies pass through verbatim rather than being mapped onto a
guessed vocabulary; only the defaults are spelled here, each one a
value the live account actually uses."""
import hashlib
import itertools
import json
import logging
import os
import sys
import time

import pandas as pd
import requests

import uploader.upload.utils as utl


tik_path = 'tik'
config_path = os.path.join(utl.config_file_path, tik_path)
base_url = 'https://business-api.tiktok.com/open_api'
api_version = 'v1.3'
CHANNEL = 'TikTok'

REQUEST_TIMEOUT = (10, 30)
UPLOAD_TIMEOUT = (10, 300)
MAX_LIST_PAGES = 100
LIST_PAGE_SIZE = 100

OBJECTIVE_DEFAULT = 'TRAFFIC'
# Paused-by-default guardrail: TikTok defaults new objects to ENABLE.
STATUS_DEFAULT = 'DISABLE'
BUDGET_MODE_DEFAULT = 'BUDGET_MODE_TOTAL'

# /adgroup/create/ makes all of these required, so a blank config cell
# still has to resolve to something valid. OBSERVED live: CLICK+CPC
# always co-occur, and pair with the TRAFFIC campaign default.
BILLING_EVENT_DEFAULT = 'CPC'
OPTIMIZATION_GOAL_DEFAULT = 'CLICK'
PACING_DEFAULT = 'PACING_MODE_SMOOTH'
ADGROUP_BUDGET_MODE_DEFAULT = 'BUDGET_MODE_DAY'
PLACEMENT_TYPE_DEFAULT = 'PLACEMENT_TYPE_NORMAL'
PLACEMENTS_DEFAULT = ['PLACEMENT_TIKTOK']
SCHEDULE_START_END = 'SCHEDULE_START_END'
SCHEDULE_FROM_NOW = 'SCHEDULE_FROM_NOW'

# OBSERVED live: schedule times are plain local strings in the
# advertiser's timezone — NOT ISO-8601, and no offset/'Z' suffix.
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# The /ad/create/ enum also has CAROUSEL_ADS, CATALOG_CAROUSEL and
# LIVE_CONTENT; v1 creates only the two single-asset formats.
AD_FORMAT_VIDEO = 'SINGLE_VIDEO'
AD_FORMAT_IMAGE = 'SINGLE_IMAGE'
UPLOAD_TYPE_FILE = 'UPLOAD_BY_FILE'

# "no id dict built yet" — distinct from the '' scope an unfiltered
# (campaign-level) read legitimately records.
_UNSCOPED = object()


def _api_url(endpoint):
    """Full v1.3 url for an endpoint path like ``/campaign/create/``."""
    return '{}/{}{}'.format(base_url, api_version, endpoint)


def _to_budget(value):
    """TikTok budgets are plain currency floats — no micro/cent
    scaling (unlike Reddit/FB). None for blank/non-numeric values so
    the field is omitted rather than sent malformed."""
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _to_datetime(value, end_of_day=False):
    """Plan-derived flight dates (MM/DD/YYYY strings or excel
    datetimes) -> TikTok's ``'%Y-%m-%d %H:%M:%S'``. A bare date is day
    bounded the way fbapi's update path bounds it, so an end date means
    end *of* that day rather than midnight at its start.

    :param value: raw spreadsheet cell
    :param end_of_day: bound a midnight timestamp to 23:59:59
    :returns: formatted local timestamp, or None when unparseable
    """
    if value is None or value == '':
        return None
    try:
        stamp = pd.to_datetime(value)
    except (ValueError, TypeError):
        return None
    if pd.isna(stamp):
        return None
    if end_of_day and (stamp.hour, stamp.minute, stamp.second) == (0, 0, 0):
        stamp = stamp.replace(hour=23, minute=59, second=59)
    return stamp.strftime(TIME_FORMAT)


def _platform_id(data, id_key):
    """Platform id out of a create response's ``data`` block.

    The single-object creates answer with a scalar (``campaign_id``,
    ``adgroup_id``), but ``/ad/create/`` takes a ``creatives`` list and
    so answers with the plural ``ad_ids``. Accept either spelling —
    one ad per config row then reads exactly like the levels above it.

    :param data: the response's ``data`` block
    :param id_key: singular id field, e.g. ``'adgroup_id'``
    :returns: the id as a string, or None
    """
    if not isinstance(data, dict):
        return None
    value = data.get(id_key)
    if not value:
        value = data.get(f'{id_key}s')
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
    return str(value) if value else None


def _to_bool(value):
    """Truthiness of a spreadsheet cell for TikTok's boolean fields —
    excel hands these over as 'TRUE'/'true'/1/True inconsistently, and
    a blank must read as unset rather than False."""
    if value is None or value == '':
        return False
    if isinstance(value, str):
        return value.strip().lower() in ('true', 'yes', '1', 'y')
    return bool(value)


def _live_flag(value):
    """A live boolean as the ``TRUE``/``FALSE`` cell :func:`_to_bool`
    reads, or '' when absent so it is not copied as off."""
    if value is None or value == '':
        return ''
    return 'TRUE' if _to_bool(value) else 'FALSE'


def _extract_error(body):
    """TikTok envelopes every response as ``{code, message, data}``
    and answers HTTP 200 even on failure — the envelope code is the
    only real signal. {} on success (``code == 0``), else
    ``{'code', 'message'}``; a body without a ``code`` key is treated
    as an error so an unparseable response can't read as success."""
    if not isinstance(body, dict) or 'code' not in body:
        return {'code': None, 'message': 'Unparseable TikTok response'}
    if body.get('code') == 0:
        return {}
    return {'code': body.get('code'),
            'message': str(body.get('message') or '').strip()}


def _populate_tik_result(result, response, id_key='campaign_id'):
    """Fill ``result`` from a TikTok create response. Success:
    ``code == 0`` with ``data.campaign_id``. Failure: non-zero code +
    message; empty messages get the raw body appended — the actionable
    reason is otherwise lost."""
    body = utl.response_body(response)
    err = _extract_error(body)
    if not err:
        platform_id = _platform_id(body.get('data') or {}, id_key)
        if platform_id:
            result['platform_id'] = platform_id
            result['status'] = 'created'
            return
        err = {'code': None,
               'message': f'TikTok response missing {id_key}'}
    message = err.get('message') or ''
    http_status = getattr(response, 'status_code', '') or ''
    if not message:
        try:
            raw = json.dumps(body)[:500]
        except (TypeError, ValueError):
            raw = ''
        if raw and raw != '{}':
            message = f'TikTok Ads error (HTTP {http_status}): {raw}'
    utl.fail_result(result, message or 'Unknown error from TikTok Ads',
                    err.get('code'))
    logging.warning('TikTok create failed (HTTP %s): %s',
                    http_status, result['error_message'])


class TikApi(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.access_token = None
        self.advertiser_id = None
        self.interest_categories = None
        self.config_list = None
        self.headers = None
        self.cam_dict = {}
        self.adgroup_dict = {}
        self.ad_dict = {}
        self.id_dict_scope = {}
        self.r = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning(
                'TikTok config file not in vendor matrix. Aborting.')
            sys.exit(0)
        logging.info(f'Loading TikTok config file: {config}')
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
        self.access_token = self.config.get('access_token', '')
        self.advertiser_id = str(self.config.get('advertiser_id', '') or '')
        self.config_list = [self.access_token, self.advertiser_id]

    def check_config(self):
        """Both keys are required for writes — unlike the processor's
        read client, where advertiser_id is optional."""
        for key in ('access_token', 'advertiser_id'):
            if not str(self.config.get(key, '') or '').strip():
                logging.warning(f'{key} not in TikTok config file. Aborting.')
                sys.exit(0)

    def set_headers(self):
        self.headers = {'Access-Token': self.access_token,
                        'Content-Type': 'application/json'}
        return self.headers

    def _post(self, url, body=None):
        self.set_headers()
        try:
            self.r = requests.post(url, headers=self.headers,
                                   json=body or {}, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.SSLError as e:
            logging.warning(f'TikTok SSLError: {e}')
            time.sleep(30)
            self.r = self._post(url, body=body)
        return self.r

    def _get(self, url, params=None):
        self.set_headers()
        return requests.get(url, headers=self.headers,
                            params=params or {}, timeout=REQUEST_TIMEOUT)

    def _post_file(self, url, data=None, files=None):
        """Multipart POST for the asset-library uploads. The JSON
        Content-Type is deliberately dropped — requests has to set the
        multipart boundary itself, and sending application/json here
        makes TikTok reject the body."""
        self.set_headers()
        headers = {'Access-Token': self.access_token}
        return requests.post(url, headers=headers, data=data or {},
                             files=files or {}, timeout=UPLOAD_TIMEOUT)

    @staticmethod
    def get_id(dict_o, match, match_name='name'):
        return [k for k, v in dict_o.items() if v.get(match_name) == match]

    def get_interest_categories(self, language='en'):
        """The interest-category taxonomy as ``[{'id', 'name',
        'level'}]``, read once per client; ids are strings."""
        if self.interest_categories is not None:
            return self.interest_categories
        body = utl.response_body(self._get(
            _api_url('/tool/interest_category/'),
            params={'advertiser_id': self.advertiser_id,
                    'language': language}))
        err = _extract_error(body)
        if err:
            logging.warning('TikTok interest categories failed: %s',
                            err.get('message') or err.get('code'))
            return []
        rows = (body.get('data') or {}).get('interest_categories') or []
        self.interest_categories = [
            {'id': str(x.get('interest_category_id') or ''),
             'name': str(x.get('interest_category_name') or ''),
             'level': x.get('level')} for x in rows
            if x.get('interest_category_id')]
        return self.interest_categories

    def ad_preview(self, ad_id):
        """A shareable preview link for a launched ad, or ``''`` when
        TikTok refuses or the body is unreadable."""
        body = utl.response_body(self._post(
            _api_url('/creative/ads_preview/create/'),
            body={'advertiser_id': self.advertiser_id,
                  'preview_type': 'AD', 'ad_id': str(ad_id)}))
        if _extract_error(body):
            return ''
        return str((body.get('data') or {}).get('preview_link') or '')

    def resolve_interest_ids(self, names):
        """``{name: id}`` for interest names, ``''`` for a miss."""
        catalogue = self.get_interest_categories()
        return {name: (utl.match_name(
            catalogue, name, 'TikTok interest categories') or {}).get(
            'id', '') for name in names}

    def _list_pages(self, endpoint, params=None):
        """Yield every ``data.list`` row across a v1.3 list endpoint.
        Page-number pagination driven by ``data.page_info.total_page``,
        bounded by ``MAX_LIST_PAGES`` so a contract change can never
        spin the worker forever."""
        page_params = dict(params or {})
        page_params.setdefault('advertiser_id', self.advertiser_id)
        page_params.setdefault('page_size', LIST_PAGE_SIZE)
        for page in range(1, MAX_LIST_PAGES + 1):
            page_params['page'] = page
            body = utl.response_body(
                self._get(_api_url(endpoint), params=page_params))
            err = _extract_error(body)
            if err:
                logging.warning('TikTok list %s failed: %s', endpoint,
                                err.get('message') or err.get('code'))
                return
            data = body.get('data') or {}
            yield from (data.get('list') or [])
            total = (data.get('page_info') or {}).get('total_page') or 1
            if page >= total:
                return
        logging.warning(
            'TikTok list hit the %s-page cap; results may be truncated.',
            MAX_LIST_PAGES)

    list_specs = {
        'campaign': ('/campaign/get/', 'campaign_id', 'campaign_name',
                     None, None),
        'adgroup': ('/adgroup/get/', 'adgroup_id', 'adgroup_name',
                    'campaign_id', 'campaign_ids'),
        'ad': ('/ad/get/', 'ad_id', 'ad_name', 'adgroup_id', 'adgroup_ids'),
    }
    id_dict_attrs = {'campaign': 'cam_dict', 'adgroup': 'adgroup_dict',
                     'ad': 'ad_dict'}

    def set_id_dict(self, kind=None, filter_id=None):
        """Objects at one level keyed by id and normalized to
        ``{'id', 'name', 'parent_id'}`` so the shared ``get_id`` name
        match works unchanged across channels.

        ``filter_id`` scopes the read to one parent via TikTok's
        ``filtering`` param, and is re-applied locally: OBSERVED live,
        a filter key TikTok does not recognise comes back ``code == 0``
        with the whole account rather than an error, silently widening
        the name-match that decides ``skipped_exists``.

        :param kind: one of ``campaign`` / ``adgroup`` / ``ad``
        :param filter_id: parent platform id to scope to
        """
        spec = self.list_specs.get(kind)
        if not spec:
            return
        endpoint, id_field, name_field, parent_field, filter_key = spec
        params = {}
        if filter_id and filter_key:
            params['filtering'] = json.dumps({filter_key: [str(filter_id)]})
        found = {}
        for row in self._list_pages(endpoint, params=params):
            oid = row.get(id_field)
            if not oid:
                continue
            parent = str(row.get(parent_field) or '') if parent_field else ''
            if filter_id and parent_field and parent != str(filter_id):
                continue
            found[str(oid)] = {'id': str(oid),
                               'name': row.get(name_field, ''),
                               'parent_id': parent}
        setattr(self, self.id_dict_attrs[kind], found)
        self.id_dict_scope[kind] = str(filter_id or '')

    def ensure_id_dict(self, kind, filter_id=None):
        """The ``kind`` dict, re-listed when it is missing or was built
        for a different parent.

        Cached on the recorded scope, not on emptiness like Reddit's
        template: OBSERVED live, TikTok ad names repeat verbatim across
        ad groups in one campaign, so a dict still scoped to the
        previous parent name-matches a sibling and reports
        ``skipped_exists`` against another ad group's ad. A parent with
        no children yet is a legitimate empty result.

        :param kind: one of ``campaign`` / ``adgroup`` / ``ad``
        :param filter_id: parent platform id to scope to
        :returns: the id dict for ``kind``
        """
        scope = str(filter_id or '')
        if self.id_dict_scope.get(kind, _UNSCOPED) != scope:
            self.set_id_dict(kind, filter_id=filter_id)
        return getattr(self, self.id_dict_attrs[kind])

    live_kinds = {'Campaign': 'campaign', 'Adset': 'adgroup', 'Ad': 'ad'}

    def list_recent(self, level, limit=25, parent_ids=None):
        """The advertiser's ``level`` objects for the copy-from-account
        picker, most recently changed first, from the first
        ``utl.RECENT_SCAN_CAP`` rows. The parent filter is re-applied
        locally for the reason :meth:`set_id_dict` gives."""
        kind = self.live_kinds.get(level)
        if not kind:
            return []
        endpoint, id_field, name_field, parent_field, filter_key = (
            self.list_specs[kind])
        params = {}
        wanted = {str(x) for x in parent_ids or []}
        if wanted and filter_key:
            params['filtering'] = json.dumps({filter_key: sorted(wanted)})
        found = itertools.islice(self._list_pages(endpoint, params=params),
                                 max(limit, utl.RECENT_SCAN_CAP))
        if wanted and parent_field:
            found = (row for row in found
                     if str(row.get(parent_field)) in wanted)
        return utl.recent_rows(found, limit, id_key=id_field,
                               name_key=name_field, parent_key=parent_field,
                               created_key='create_time',
                               updated_key='modify_time')

    def create_entity(self, entity, entity_name='campaign'):
        """POST the entity's ``upload_dict`` to the level's create
        endpoint; the advertiser id rides every write."""
        body = dict(entity.upload_dict)
        body['advertiser_id'] = self.advertiser_id
        return self._post(_api_url(f'/{entity_name}/create/'), body=body)

    def probe_account(self):
        """(ok, message) — verify the token reaches the configured
        advertiser, for the live pre-flight checks."""
        try:
            params = {'advertiser_ids': json.dumps([self.advertiser_id])}
            body = utl.response_body(
                self._get(_api_url('/advertiser/info/'), params=params))
            err = _extract_error(body)
            if err:
                return False, str(err.get('message') or err)
            rows = (body.get('data') or {}).get('list') or []
            if not rows:
                return False, (
                    'TikTok token reached no advertiser matching '
                    f'advertiser_id {self.advertiser_id!r} — confirm the id '
                    'in tikconfig.json.')
            return True, ''
        except Exception as e:
            return False, str(e)

    def upload_asset(self, file_path, is_video=True):
        """Push one local creative to the Asset Library.

        ``upload_type=UPLOAD_BY_FILE`` signs with the file's MD5, which
        TikTok verifies server-side — hence hashing the bytes here.

        :param file_path: local path to the image/video
        :param is_video: video endpoint when True, else image
        :returns: (asset_id, error_message) — one of the two is falsy
        """
        endpoint = ('/file/video/ad/upload/' if is_video
                    else '/file/image/ad/upload/')
        field = 'video_file' if is_video else 'image_file'
        signature_field = 'video_signature' if is_video else 'image_signature'
        id_key = 'video_id' if is_video else 'image_id'
        file_name = os.path.basename(file_path)
        try:
            with open(file_path, 'rb') as f:
                blob = f.read()
        except IOError as e:
            return '', f'Could not read {file_path}: {e}'
        data = {'advertiser_id': self.advertiser_id,
                'upload_type': UPLOAD_TYPE_FILE,
                'file_name': file_name,
                signature_field: hashlib.md5(blob).hexdigest()}
        try:
            response = self._post_file(
                _api_url(endpoint), data=data,
                files={field: (file_name, blob)})
        except requests.exceptions.RequestException as e:
            return '', str(e)
        body = utl.response_body(response)
        err = _extract_error(body)
        if err:
            return '', (err.get('message')
                        or f'TikTok upload failed (code {err.get("code")})')
        # A list of assets even for a single file.
        rows = body.get('data') or []
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows:
            if isinstance(row, dict) and row.get(id_key):
                return str(row[id_key]), ''
        return '', f'TikTok upload response missing {id_key}'

    entity_status_endpoints = {'Campaign': '/campaign/status/update/',
                               'Adset': '/adgroup/status/update/',
                               'Ad': '/ad/status/update/'}
    entity_status_id_fields = {'Campaign': 'campaign_ids',
                               'Adset': 'adgroup_ids', 'Ad': 'ad_ids'}

    def update_statuses(self, object_level, platform_ids, activate=True):
        """POST ``operation_status`` on existing objects by id — one
        call per id so each row carries its own verdict. Returns one
        dict per id: {'platform_id', 'status' ('updated'|'failed'),
        'error_code', 'error_message'}."""
        endpoint = self.entity_status_endpoints.get(object_level)
        id_field = self.entity_status_id_fields.get(object_level)
        status = 'ENABLE' if activate else 'DISABLE'
        results = []
        for pid in platform_ids:
            result = utl.new_update_result(pid)
            if not endpoint:
                results.append(utl.fail_result(
                    result, f'Unknown TikTok level: {object_level}'))
                continue
            try:
                body = {'advertiser_id': self.advertiser_id,
                        id_field: [str(pid)],
                        'operation_status': status}
                err = _extract_error(utl.response_body(
                    self._post(_api_url(endpoint), body=body)))
                if err:
                    utl.fail_result(
                        result,
                        err.get('message')
                        or 'Unknown error from TikTok Ads',
                        err.get('code'))
            except Exception as e:
                utl.fail_result(result, e)
            results.append(result)
        return results


class CampaignUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'TikTok campaign config'
    file_name = 'campaign_upload.xlsx'
    name = 'name'
    objective_type = 'objective_type'
    status = 'operation_status'
    budget_mode = 'budget_mode'
    budget = 'budget'
    budget_optimize_on = 'budget_optimize_on'
    snapshot_cols = [objective_type, status, budget_mode, budget,
                     budget_optimize_on]

    @staticmethod
    def settings_from_live(fields):
        """The live campaign's objective and budget settings as
        level-file cells; name, status and the amount are not copied."""
        cu = CampaignUpload
        return utl.live_settings({
            cu.objective_type: fields.get('objective_type'),
            cu.budget_mode: fields.get('budget_mode'),
            cu.budget_optimize_on: _live_flag(
                fields.get('budget_optimize_on'))})

    def upload_all_campaigns(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, c_id in enumerate(self.config):
            cam = Campaign(self.config[c_id], api=api)
            logging.info(
                f'Uploading TikTok campaign {idx + 1} of {total}: '
                f'{cam.name}')
            result = self.upload_campaign(api, cam)
            result['pushed_values'] = utl.snapshot_values(
                self.config[c_id], self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_campaign(api, campaign):
        result = utl.new_result('Campaign', campaign.name, CHANNEL)
        if not campaign.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = 'Missing required campaign fields'
            return result
        if campaign.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = campaign.id
            return result
        _populate_tik_result(result, api.create_entity(campaign))
        if result['status'] == 'created':
            campaign.id = result['platform_id']
        return result


class Campaign(object):
    __slots__ = ['name', 'objective_type', 'operation_status',
                 'budget_mode', 'budget', 'budget_optimize_on',
                 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.objective_type = OBJECTIVE_DEFAULT
        self.operation_status = STATUS_DEFAULT
        self.budget_mode = None
        self.budget = None
        self.budget_optimize_on = None
        utl.apply_row(self, row_dict)
        self.api = api
        self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        if not self.name:
            return {}
        d = {
            'campaign_name': str(self.name),
            'objective_type': str(self.objective_type or OBJECTIVE_DEFAULT),
            'operation_status': str(self.operation_status or STATUS_DEFAULT),
        }
        budget = _to_budget(self.budget)
        if budget:
            d['budget'] = budget
            d['budget_mode'] = str(self.budget_mode or BUDGET_MODE_DEFAULT)
        elif str(self.budget_mode or ''):
            # An explicit mode with no budget (BUDGET_MODE_INFINITE) is
            # valid; omitting both lets TikTok default to no cap.
            d['budget_mode'] = str(self.budget_mode)
        # Campaign Budget Optimization moves the budget up to this level
        # and hands bidding/optimization to the ad groups beneath it.
        if _to_bool(self.budget_optimize_on):
            d['budget_optimize_on'] = True
        return d

    def check_exists(self, api):
        found = api.get_id(api.ensure_id_dict('campaign'), self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdGroupUpload(utl.BaseUploadConfig):
    """TikTok's mid-tier object (Adset in LQ parlance).

    Column names stay in the spreadsheet vocabulary the other channels
    use (``name``, ``start_time``, ``end_time``) and are mapped onto
    TikTok's field spellings in ``create_adgroup_dict``, so the app's
    name-builder and apply-across machinery treat TikTok like any
    other channel.
    """
    config_dir = config_path
    config_label = 'TikTok adgroup config'
    file_name = 'adset_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    status = 'operation_status'
    budget = 'budget'
    budget_mode = 'budget_mode'
    budget_optimize_on = 'budget_optimize_on'
    dayparting = 'dayparting'
    billing_event = 'billing_event'
    optimization_goal = 'optimization_goal'
    optimization_event = 'optimization_event'
    pacing = 'pacing'
    bid_type = 'bid_type'
    bid_price = 'bid_price'
    schedule_type = 'schedule_type'
    start_time = 'start_time'
    end_time = 'end_time'
    promotion_type = 'promotion_type'
    placement_type = 'placement_type'
    placements = 'placements'
    pixel_id = 'pixel_id'
    # Targeting columns (pipe/comma-delimited cells).
    location_ids = 'location_ids'
    age_groups = 'age_groups'
    languages = 'languages'
    interest_category_ids = 'interest_category_ids'
    operating_systems = 'operating_systems'
    gender = 'gender'
    snapshot_cols = [status, budget, budget_mode, budget_optimize_on,
                     billing_event, optimization_goal, pacing, bid_type,
                     bid_price, schedule_type, start_time, end_time,
                     promotion_type, placement_type, dayparting, pixel_id]
    live_setting_cols = (budget_mode, budget_optimize_on, dayparting,
                         billing_event, optimization_goal,
                         optimization_event, pacing, bid_type,
                         promotion_type, placement_type, placements,
                         pixel_id, location_ids, age_groups, languages,
                         interest_category_ids, operating_systems, gender)

    @staticmethod
    def settings_from_live(fields):
        """The live ad group's settings as level-file cells; name,
        status, amounts and the schedule (the flight's) are not copied."""
        agu = AdGroupUpload
        lists = (agu.placements, agu.location_ids, agu.age_groups,
                 agu.languages, agu.interest_category_ids,
                 agu.operating_systems)
        values = {col: fields.get(col) for col in agu.live_setting_cols}
        values.update({col: utl.join_list(fields.get(col)) for col in lists})
        values[agu.budget_optimize_on] = _live_flag(
            fields.get(agu.budget_optimize_on))
        return utl.live_settings(values)

    def upload_all_adgroups(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, ag_id in enumerate(self.config):
            ag = AdGroup(self.config[ag_id], api=api)
            logging.info(
                f'Uploading TikTok adgroup {idx + 1} of {total}: {ag.name}')
            result = self.upload_adgroup(api, ag)
            result['pushed_values'] = utl.snapshot_values(
                self.config[ag_id], self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_adgroup(api, adgroup):
        result = utl.new_result('Adset', adgroup.name, CHANNEL,
                                adgroup.campaignId)
        if not adgroup.campaignId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Campaign {adgroup.campaign!r} not found')
            return result
        if not adgroup.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                'Missing required ad group fields: '
                f'{", ".join(adgroup.missing)}')
            return result
        if adgroup.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = adgroup.id
            return result
        _populate_tik_result(
            result, api.create_entity(adgroup, entity_name='adgroup'),
            id_key='adgroup_id')
        if result['status'] == 'created':
            adgroup.id = result['platform_id']
        return result


class AdGroup(object):
    __slots__ = ['name', 'campaign', 'campaignId', 'operation_status',
                 'budget', 'budget_mode', 'budget_optimize_on',
                 'billing_event', 'optimization_goal', 'optimization_event',
                 'pacing', 'bid_type', 'bid_price', 'schedule_type',
                 'start_time', 'end_time', 'dayparting', 'promotion_type',
                 'placement_type', 'placements', 'pixel_id', 'location_ids',
                 'age_groups', 'languages', 'interest_category_ids',
                 'operating_systems', 'gender', 'missing', 'upload_dict',
                 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.campaignId = None
        self.operation_status = STATUS_DEFAULT
        self.budget = None
        self.budget_mode = None
        self.budget_optimize_on = None
        self.dayparting = None
        self.billing_event = None
        self.optimization_goal = None
        self.optimization_event = None
        self.pacing = None
        self.bid_type = None
        self.bid_price = None
        self.schedule_type = None
        self.start_time = None
        self.end_time = None
        self.promotion_type = None
        self.placement_type = None
        self.placements = None
        self.pixel_id = None
        self.location_ids = None
        self.age_groups = None
        self.languages = None
        self.interest_category_ids = None
        self.operating_systems = None
        self.gender = None
        self.missing = []
        utl.apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.resolve_campaign(self.api)
        self.upload_dict = self.create_adgroup_dict()

    def resolve_campaign(self, api):
        cam = Campaign({'name': self.campaign}, api=api)
        cam.check_exists(api)
        self.campaignId = cam.id

    def create_adgroup_dict(self):
        """The ``/adgroup/create/`` body. Returns {} — recording what
        was absent in ``missing`` — when a field TikTok makes required
        can't be resolved, so the row fails locally with an actionable
        reason instead of spending a round trip on a rejected create.

        ``budget`` is required here only WITHOUT Campaign Budget
        Optimization — under CBO the budget lives on the campaign, so
        the local check would refuse a valid setup. The flag itself is
        a campaign field and is not forwarded.
        """
        cbo = _to_bool(self.budget_optimize_on)
        budget = _to_budget(self.budget)
        self.missing = [label for label, value in
                        (('name', self.name),
                         ('campaign', self.campaignId),
                         ('budget', budget or cbo)) if not value]
        if self.missing:
            return {}
        start = _to_datetime(self.start_time)
        end = _to_datetime(self.end_time, end_of_day=True)
        # SCHEDULE_START_END needs both ends; without an end date the
        # only valid schedule is the open-ended one.
        schedule_type = str(self.schedule_type or '') or (
            SCHEDULE_START_END if end else SCHEDULE_FROM_NOW)
        d = {
            'adgroup_name': str(self.name),
            'campaign_id': str(self.campaignId),
            'operation_status': str(self.operation_status or STATUS_DEFAULT),
            'billing_event': str(
                self.billing_event or BILLING_EVENT_DEFAULT),
            'optimization_goal': str(
                self.optimization_goal or OPTIMIZATION_GOAL_DEFAULT),
            'pacing': str(self.pacing or PACING_DEFAULT),
            'schedule_type': schedule_type,
            'placement_type': str(
                self.placement_type or PLACEMENT_TYPE_DEFAULT),
        }
        if not cbo:
            d['budget'] = budget
            d['budget_mode'] = str(
                self.budget_mode or ADGROUP_BUDGET_MODE_DEFAULT)
        if self.dayparting:
            d['dayparting'] = str(self.dayparting)
        if start:
            d['schedule_start_time'] = start
        if end and schedule_type == SCHEDULE_START_END:
            d['schedule_end_time'] = end
        # Placements are only the advertiser's to choose on the manual
        # placement type; TikTok picks them itself on the automatic one,
        # so sending a list there would conflict with the mode.
        if d['placement_type'] == PLACEMENT_TYPE_DEFAULT:
            d['placements'] = (utl.split_list(self.placements)
                               or list(PLACEMENTS_DEFAULT))
        for field, value in (('bid_type', self.bid_type),
                             ('promotion_type', self.promotion_type),
                             ('optimization_event', self.optimization_event),
                             ('pixel_id', self.pixel_id)):
            if value:
                d[field] = str(value)
        bid = _to_budget(self.bid_price)
        if bid:
            d['bid_price'] = bid
        d.update(self.create_targeting_dict())
        return d

    def create_targeting_dict(self):
        """Targeting fields for the create body. TikTok targets by
        opaque platform ids (locations, interests) and fixed enums that
        the plan supplies directly, so values are passed through
        verbatim — inventing a name->id or free-text->enum mapping here
        would silently mistarget. Empty when nothing is set.
        """
        targeting = {}
        for field, value in (
                ('location_ids', self.location_ids),
                ('age_groups', self.age_groups),
                ('languages', self.languages),
                ('interest_category_ids', self.interest_category_ids),
                ('operating_systems', self.operating_systems)):
            values = utl.split_list(value)
            if values:
                targeting[field] = values
        gender = str(self.gender or '').strip().upper()
        if gender:
            targeting['gender'] = gender
        return targeting

    def check_exists(self, api):
        found = api.get_id(
            api.ensure_id_dict('adgroup', filter_id=self.campaignId),
            self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class CreativeUpload(utl.BaseCreativeStore):
    """Local creative files -> Asset Library ids, cached in
    ``tik_creative_ids.csv`` so a re-run never re-uploads. Videos and
    images go to different endpoints, so both id columns are kept and
    the file's extension picks the one that gets filled.
    """
    id_cols = ('video_id', 'image_id')

    def __init__(self, id_file_name='tik_creative_ids.csv',
                 creative_path='creative/'):
        super().__init__(id_file_name, creative_path=creative_path)

    def _upload_one(self, api, file_path):
        ext = os.path.splitext(file_path)[1].lstrip('.').lower()
        if ext in utl.video_types:
            is_video = True
        elif ext in utl.static_types:
            is_video = False
        else:
            logging.warning(
                f'{file_path} is not a TikTok image/video type; skipped.')
            return {}
        asset_id, error = api.upload_asset(file_path, is_video=is_video)
        if not asset_id:
            logging.warning(f'TikTok creative upload failed for '
                            f'{file_path}: {error}')
            return {}
        return {'video_id' if is_video else 'image_id': asset_id}


class AdUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'TikTok ad config'
    file_name = 'ad_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    adgroup = 'adgroup'
    creative = 'creative'
    status = 'operation_status'
    ad_format = 'ad_format'
    ad_text = 'ad_text'
    call_to_action = 'call_to_action'
    landing_page_url = 'landing_page_url'
    display_name = 'display_name'
    identity_id = 'identity_id'
    identity_type = 'identity_type'
    video_id = 'video_id'
    image_ids = 'image_ids'
    snapshot_cols = [status, ad_format, ad_text, call_to_action,
                     landing_page_url, display_name, identity_id,
                     identity_type]
    live_setting_cols = (ad_format, call_to_action, display_name,
                         identity_id, identity_type)

    @staticmethod
    def settings_from_live(fields):
        """The live ad's format, identity and call to action as
        level-file cells; text, landing page and media are each ad's."""
        return utl.live_settings({col: fields.get(col)
                                  for col in AdUpload.live_setting_cols})

    def creative_filenames(self):
        """Creative filenames referenced by the config, de-duped."""
        seen = []
        for row in (self.config or {}).values():
            fn = str(row.get(self.creative, '') or '').strip()
            if fn and fn not in seen:
                seen.append(fn)
        return seen

    def upload_all_ads(self, api, creative_store=None):
        """Push every ad row, uploading any creative files first so the
        rows can resolve their ``video_id``.

        :param api: a connected ``TikApi``
        :param creative_store: ``CreativeUpload`` backing the
            filename->asset-id cache; None when the config supplies
            platform ids directly
        :returns: one result dict per config row
        """
        if not self.config:
            return []
        if creative_store is not None:
            creative_store.upload_all(api, self.creative_filenames())
        results = []
        total = len(self.config)
        for idx, a_id in enumerate(self.config):
            ad = Ad(self.config[a_id], api=api,
                    creative_store=creative_store)
            logging.info(
                f'Uploading TikTok ad {idx + 1} of {total}: {ad.name}')
            result = self.upload_ad(api, ad)
            result['pushed_values'] = utl.snapshot_values(
                self.config[a_id], self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_ad(api, ad):
        result = utl.new_result('Ad', ad.name, CHANNEL, ad.adGroupId)
        if not ad.adGroupId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = f'Ad group {ad.adgroup!r} not found'
            return result
        if not ad.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Missing required ad fields: {", ".join(ad.missing)}')
            return result
        if ad.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = ad.id
            return result
        _populate_tik_result(
            result, api.create_entity(ad, entity_name='ad'), id_key='ad_id')
        if result['status'] == 'created':
            ad.id = result['platform_id']
        return result


class Ad(object):
    __slots__ = ['name', 'campaign', 'adgroup', 'adGroupId', 'creative',
                 'operation_status', 'ad_format', 'ad_text',
                 'call_to_action', 'landing_page_url', 'display_name',
                 'identity_id', 'identity_type', 'video_id', 'image_ids',
                 'missing', 'upload_dict', 'api', 'creative_store', 'id']

    def __init__(self, row_dict, api=None, creative_store=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.adgroup = None
        self.adGroupId = None
        self.creative = None
        self.operation_status = STATUS_DEFAULT
        self.ad_format = None
        self.ad_text = None
        self.call_to_action = None
        self.landing_page_url = None
        self.display_name = None
        self.identity_id = None
        self.identity_type = None
        self.video_id = None
        self.image_ids = None
        self.missing = []
        utl.apply_row(self, row_dict)
        self.api = api
        self.creative_store = creative_store
        if self.api:
            self.resolve_ids(self.api)
        self.upload_dict = self.create_ad_dict()

    def resolve_ids(self, api):
        if self.adgroup:
            ag = AdGroup({'name': self.adgroup, 'campaign': self.campaign},
                         api=api)
            ag.check_exists(api)
            self.adGroupId = ag.id
        self.resolve_creative()

    def resolve_creative(self):
        """Fill ``video_id``/``image_ids`` from the creative store when
        the config names a local file rather than a platform id. An
        explicit id in the config always wins."""
        if not (self.creative_store and self.creative):
            return
        filename = str(self.creative).strip()
        if not self.video_id:
            self.video_id = self.creative_store.get_id(filename, 'video_id')
        if not self.image_ids:
            image_id = self.creative_store.get_id(filename, 'image_id')
            if image_id:
                self.image_ids = image_id

    def create_ad_dict(self):
        """The ``/ad/create/`` body. TikTok takes ads as a ``creatives``
        list under one adgroup; the engine is one ad per config row, so
        the list always holds exactly one entry — which keeps the
        result contract (one platform_id per row) intact.
        """
        images = utl.split_list(self.image_ids)
        self.missing = [label for label, value in
                        (('name', self.name),
                         ('adgroup', self.adGroupId),
                         ('creative', self.video_id or images))
                        if not value]
        if self.missing:
            return {}
        # Derive the format from what resolved rather than defaulting to
        # video: an image-only row sent as SINGLE_VIDEO has no video_id.
        derived = AD_FORMAT_VIDEO if self.video_id else AD_FORMAT_IMAGE
        creative = {
            'ad_name': str(self.name),
            'ad_format': str(self.ad_format or derived),
            'operation_status': str(self.operation_status or STATUS_DEFAULT),
        }
        if self.video_id:
            creative['video_id'] = str(self.video_id)
        if images:
            creative['image_ids'] = images
        for field, value in (('ad_text', self.ad_text),
                             ('call_to_action', self.call_to_action),
                             ('landing_page_url', self.landing_page_url),
                             ('display_name', self.display_name),
                             ('identity_id', self.identity_id),
                             ('identity_type', self.identity_type)):
            if value:
                creative[field] = str(value)
        return {'adgroup_id': str(self.adGroupId), 'creatives': [creative]}

    def check_exists(self, api):
        found = api.get_id(
            api.ensure_id_dict('ad', filter_id=self.adGroupId), self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


LIVE_UPLOADS = {'Campaign': CampaignUpload, 'Adset': AdGroupUpload,
                'Ad': AdUpload}
