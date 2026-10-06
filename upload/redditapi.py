"""Reddit Ads API uploader. Mirrors the awapi/dcapi class shape so
the relation system, name-create flow, and run telemetry pick it
up without special-casing."""
import json
import logging
import os
import re
import sys
import time

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

import uploader.upload.utils as utl


reddit_path = 'reddit'
config_path = os.path.join(utl.config_file_path, reddit_path)
base_url = 'https://ads-api.reddit.com/api/v3'
CHANNEL = 'Reddit'

REQUEST_TIMEOUT = (10, 30)
UPLOAD_TIMEOUT = (10, 120)
MAX_LIST_PAGES = 100
DEFAULT_USER_AGENT = 'web:liquid-advertising-uploader:v1.0'


def normalize_community(value):
    """A community cell as a bare name without ``r/``; '' for a value
    that is not a community name."""
    name = str(value or '').strip().lstrip('/')
    if name.lower().startswith('r/'):
        name = name[2:]
    if not name:
        return ''
    if re.search(r'[\s/]', name):
        logging.warning('%r is not a Reddit community name; skipped.',
                        value)
        return ''
    return name


def _to_iso(value):
    """Reddit wants ISO-8601 timestamps; plan-derived flight dates
    arrive as MM/DD/YYYY strings (or excel datetimes)."""
    if value is None or value == '':
        return None
    try:
        return pd.to_datetime(value).strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, TypeError):
        return None


def _to_micros(value):
    """Reddit money fields (spend_cap, budget_value, bids) are in
    microcurrency — 1/1,000,000 of the account currency unit. Plan
    costs arrive as whole-currency floats, so scale up; mirrors the
    processor's RedApi dividing reported spend by 1,000,000 on the way
    in. Returns None for blank/non-numeric values."""
    try:
        return int(round(float(value) * 1_000_000))
    except (TypeError, ValueError):
        return None


# Reddit's post CTA is a fixed Title-Case enum; map free text onto it.
_CTA_CANONICAL = [
    'Apply Now', 'Contact Us', 'Download', 'Get a Quote', 'Get Showtimes',
    'Install', 'Learn More', 'Order Now', 'Play Now', 'Pre-order Now',
    'See Menu', 'Shop Now', 'Sign Up', 'View More', 'Watch Now', 'Book Now',
    'Buy Tickets', 'Get Directions', 'Listen Now', 'Read More', 'Subscribe',
    'Visit Store', 'Donate Now', 'Remind Me']
CTA_DEFAULT = 'Learn More'
_CTA_BY_LOWER = {c.lower(): c for c in _CTA_CANONICAL}

# Plan/processor platform tokens -> Reddit v3 ad-group platform enum.
PLATFORM_MAP = {
    'ALL': 'ALL', 'DESKTOP': 'DESKTOP', 'MOBILE': 'MOBILE_NATIVE',
    'MOBILE_NATIVE': 'MOBILE_NATIVE', 'MOBILE_WEB': 'MOBILE_WEB',
    'APP': 'MOBILE_NATIVE', 'WEB': 'MOBILE_WEB'}

# Common country names -> ISO 3166-1 alpha-2 for the live geo resolver.
COUNTRY_TO_ISO = {
    'united states': 'US', 'usa': 'US', 'u.s.': 'US',
    'united kingdom': 'GB', 'great britain': 'GB',
    'canada': 'CA', 'australia': 'AU', 'germany': 'DE', 'france': 'FR',
    'spain': 'ES', 'italy': 'IT', 'netherlands': 'NL', 'sweden': 'SE',
    'norway': 'NO', 'denmark': 'DK', 'finland': 'FI', 'ireland': 'IE',
    'india': 'IN', 'japan': 'JP', 'brazil': 'BR', 'mexico': 'MX',
    'new zealand': 'NZ'}


def _to_cta(value):
    """Nearest valid Reddit CTA enum string for a free-text value,
    defaulting to 'Learn More'."""
    return _CTA_BY_LOWER.get(str(value or '').strip().lower(), CTA_DEFAULT)


def _extract_error(body):
    """Reddit Ads v3 returns failures as a single ``{"error": {"message",
    "code"}}`` object. Tolerate a legacy ``{"errors": [...]}`` list too so
    older surfaces don't collapse to 'Unknown error'."""
    if not isinstance(body, dict):
        return {}
    err = body.get('error')
    if isinstance(err, dict):
        return err
    errs = body.get('errors')
    if isinstance(errs, list) and errs and isinstance(errs[0], dict):
        return errs[0]
    return {}


_GENERIC_ERROR_MESSAGES = ('', 'bad request', 'unknown error',
                           'unknown error from reddit ads')


def _populate_reddit_result(result, response):
    """Fill ``result`` from a Reddit Ads create response. Success:
    ``{"data": {"id": ...}}``. Failure: ``{"error": {...}}``; generic
    messages ('Bad Request') get the raw body appended — the
    actionable reason often lives outside error.message."""
    body = utl.response_body(response)
    data = body.get('data') or {}
    if isinstance(data, dict) and data.get('id'):
        result['platform_id'] = data['id']
        result['status'] = 'created'
        return
    err = _extract_error(body)
    message = str(err.get('message') or err.get('detail') or '').strip()
    http_status = getattr(response, 'status_code', '') or ''
    if message.lower() in _GENERIC_ERROR_MESSAGES:
        try:
            raw = json.dumps(body)[:500]
        except (TypeError, ValueError):
            raw = ''
        if raw and raw != '{}':
            message = '{} (HTTP {}): {}'.format(
                message or 'Reddit Ads error', http_status, raw)
    utl.fail_result(result, message or 'Unknown error from Reddit Ads',
                    err.get('code'))
    logging.warning('Reddit create failed (HTTP %s): %s',
                    http_status, result['error_message'])


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
        self.username = None
        self.user_agent = None
        self.config_list = None
        self.client = None
        self.cam_dict = {}
        self.adgroup_dict = {}
        self.ad_dict = {}
        self.profile_dict = {}
        self.post_dict = {}
        self.asset_dict = {}
        self._geo_cache = {}
        self._account_ready = False
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
        self.username = self.config.get('username', '')
        self.user_agent = (self.config.get('user_agent')
                           or self.config.get('redirect_uri')
                           or DEFAULT_USER_AGENT)
        self.config_list = [
            self.client_id, self.client_secret,
            self.refresh_token, self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(
                    f'{item} not in Reddit config file. Aborting.')
                sys.exit(0)

    def get_client(self, force=False):
        """Exchange the refresh token for an access token and build an
        authed ``requests`` session. Reddit's token endpoint *requires*
        HTTP Basic Auth (client_id:client_secret) and a unique
        User-Agent — this mirrors the processor's
        ``RedApi.get_access_token``, the proven flow. The previous
        OAuth2Session call put the client creds in the POST body with
        no User-Agent, which Reddit rejects with a 401."""
        if self.client and not force:
            return
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'User-Agent': self.user_agent}
        data = {'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token}
        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        try:
            r = requests.post(self.refresh_url, headers=headers, data=data,
                              auth=auth, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            self.client = None
            raise utl.UploaderAuthError(
                'Reddit OAuth token request failed: {}'.format(e))
        try:
            token = r.json()
        except ValueError:
            token = {}
        access_token = (token or {}).get('access_token')
        if r.status_code != 200 or not access_token:
            self.client = None
            raise utl.UploaderAuthError(self._auth_error_message(r, token))
        self.access_token = access_token
        self.client = requests.Session()
        self.client.headers.update(
            {'Authorization': 'Bearer {}'.format(access_token),
             'User-Agent': self.user_agent,
             'Accept': 'application/json'})

    @staticmethod
    def _auth_error_message(response, token):
        """Secret-free, accurate diagnosis of a refused token request:
        ``invalid_grant`` means the refresh token is dead, while
        ``invalid_client`` / a bare 401 means client_id/client_secret
        weren't accepted as HTTP Basic Auth — different fixes."""
        status = getattr(response, 'status_code', '')
        error = (token or {}).get('error')
        if error == 'invalid_grant':
            hint = ('refresh token is invalid, expired or revoked — '
                    're-authorize the app and update redditconfig.json.')
        elif error == 'invalid_client' or status == 401:
            hint = ('client_id/client_secret were not accepted (HTTP '
                    'Basic Auth) — verify they match the authorized '
                    'Reddit app.')
        else:
            hint = 're-authorize the app and update redditconfig.json.'
        return ('Reddit OAuth refresh failed (HTTP {}, error={!r}): {}'
                .format(status, error, hint))

    def _entity_url(self, segment):
        return '{}/ad_accounts/{}/{}'.format(
            base_url, self.ensure_account_id(), segment)

    def ensure_account_id(self):
        """The ad-account id for the API path, resolved against the
        accounts the token can actually reach. Accepts the configured
        ``ad_account_id`` (with or without the ``a2_`` prefix) or the
        ``username`` (matched leniently to the account name), and always
        returns the canonical id Reddit expects. Fails early with the real
        account list instead of letting every call 404 as 'Account not
        found'. Resolved once per run."""
        if self._account_ready:
            return self.ad_account_id
        accounts = self.list_ad_accounts()
        resolved = (self._match_account(accounts, self.ad_account_id)
                    or self._match_account(accounts, self.username))
        if not resolved:
            raise utl.UploaderAuthError(self._account_error(accounts))
        self.ad_account_id = resolved
        self._account_ready = True
        logging.info('Using Reddit ad_account_id: %s', resolved)
        return resolved

    def _list_business_ids(self):
        """All business ids on the token, via ``me/businesses``."""
        url = '{}/me/businesses'.format(base_url)
        return [row['id'] for row in self._paginate(url) if row.get('id')]

    def list_ad_accounts(self):
        """``[{'id','name'}]`` for every ad account the token can reach
        across its businesses — the set valid for the API path."""
        accounts = []
        for business_id in self._list_business_ids():
            url = '{}/businesses/{}/ad_accounts'.format(base_url, business_id)
            try:
                rows = (self._get(url).json() or {}).get('data') or []
            except ValueError:
                continue
            accounts += [{'id': row['id'], 'name': row.get('name', '')}
                         for row in rows if row.get('id')]
        return accounts

    @staticmethod
    def _bare_id(value):
        """An ad-account id without its ``a2_`` prefix, lowercased — users
        often paste the bare id, so match it prefix-insensitively."""
        text = str(value or '').strip().lower()
        return text[3:] if text.startswith('a2_') else text

    @staticmethod
    def _norm_name(value):
        """Lowercased alphanumerics of an account name, so a ``username``
        like ``liquid_etl`` matches an account named ``Liquid ETL``."""
        return re.sub(r'[^a-z0-9]', '', str(value or '').lower())

    def _match_account(self, accounts, value):
        """Canonical id of the account ``value`` identifies — by id (with
        or without the ``a2_`` prefix) or by account name (case / space /
        underscore-insensitive). '' when nothing matches."""
        if not str(value or '').strip():
            return ''
        want_id, want_name = self._bare_id(value), self._norm_name(value)
        for account in accounts:
            if (self._bare_id(account['id']) == want_id
                    or self._norm_name(account['name']) == want_name):
                return account['id']
        return ''

    def _account_error(self, accounts):
        """Actionable message naming the ad accounts the token can reach,
        so a wrong ad_account_id/username is a one-line config fix."""
        if not accounts:
            return ('Reddit token reached no ad accounts — confirm the '
                    'refresh token is authorized for this business with the '
                    'adsedit scope, then re-check redditconfig.json.')
        listing = ', '.join("{} ('{}')".format(a['id'], a['name'])
                            for a in accounts)
        return ('Reddit ad_account_id {!r} / username {!r} matched none of '
                'the ad accounts this token can reach. Set "ad_account_id" '
                'in redditconfig.json to one of: {}'
                .format(self.ad_account_id, self.username, listing))

    def create_entity(self, entity, entity_name=''):
        url = self._entity_url(entity_name)
        return self._post(url, body={'data': entity.upload_dict})

    def _post(self, url, body=None):
        self.get_client()
        try:
            self.r = self.client.post(
                url, json=body or {}, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.SSLError as e:
            logging.warning(f'Reddit SSLError: {e}')
            time.sleep(30)
            self.r = self._post(url, body=body)
        return self.r

    def _get(self, url, params=None):
        self.get_client()
        return self.client.get(
            url, params=params or {}, timeout=REQUEST_TIMEOUT)

    def _patch(self, url, body=None):
        self.get_client()
        try:
            self.r = self.client.patch(
                url, json=body or {}, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.SSLError as e:
            logging.warning(f'Reddit SSLError: {e}')
            time.sleep(30)
            self.r = self._patch(url, body=body)
        return self.r

    @staticmethod
    def get_id(dict_o, match, match_name='name'):
        return [k for k, v in dict_o.items() if v.get(match_name) == match]

    def _paginate(self, url, params=None):
        """Yield every ``data`` row across a v3 list endpoint, following
        ``pagination.next_url`` directly (the cursor is baked into it — the
        docs warn against rebuilding it from query params). Bounded by
        ``MAX_LIST_PAGES`` so a contract change can never spin the worker
        forever."""
        page_params = dict(params or {})
        for _ in range(MAX_LIST_PAGES):
            try:
                body = self._get(url, params=page_params).json()
            except ValueError:
                return
            yield from (body.get('data') or [])
            url = (body.get('pagination') or {}).get('next_url')
            if not url:
                return
            page_params = None  # cursor is in next_url; don't re-append
        logging.warning(
            'Reddit list hit the %s-page cap; results may be truncated.',
            MAX_LIST_PAGES)

    def _list(self, entity_name, params=None):
        """Ad-account resource list keyed by id (campaigns, ad_groups,
        ads, funding_instruments, profiles, pixels)."""
        return {row['id']: row
                for row in self._paginate(self._entity_url(entity_name),
                                          params) if row.get('id')}

    def set_id_dict(self, kind=None, filter_id=None):
        if kind == 'campaign':
            self.cam_dict = self._list('campaigns')
        elif kind == 'adgroup':
            params = {'campaign_id': filter_id} if filter_id else None
            self.adgroup_dict = self._list('ad_groups', params=params)
        elif kind == 'ad':
            params = {'ad_group_id': filter_id} if filter_id else None
            self.ad_dict = self._list('ads', params=params)

    def _id_name_options(self, segment, *name_keys):
        """``[{'id','name'}]`` for an ad-account list, labelled by the
        first present ``name_keys`` value (else the id) — the shared shape
        every picker reader returns."""
        options = []
        for rid, row in self._list(segment).items():
            row = row or {}
            name = next((row[k] for k in name_keys if row.get(k)), rid)
            options.append({'id': rid, 'name': name})
        return options

    def get_funding_instruments(self):
        """Funding instruments on the ad account, labelled by name then
        currency. Feeds the campaign config's ``funding_instrument_id``
        picker — the id every Reddit campaign needs to be billable."""
        return self._id_name_options('funding_instruments', 'name', 'currency')

    def get_pixels(self):
        """Conversion pixels on the ad account, for the ad-group
        ``conversion_pixel_id`` picker — Reddit makes this field required
        for all ad groups starting 2026-07-13."""
        return self._id_name_options('pixels', 'name')

    def get_profiles(self):
        """Profiles on the ad account — the author a Post is created
        under. Feeds the ad-config profile picker."""
        return self._id_name_options('profiles', 'name', 'username')

    def resolve_profile_id(self, value):
        """Profile id for a name/username/id: pass through a value that is
        already a known profile id, else match by name/username
        (case-insensitive). '' when nothing matches."""
        value = str(value or '').strip()
        if not value:
            return ''
        if not self.profile_dict:
            self.profile_dict = self._list('profiles')
        if value in self.profile_dict:
            return value
        target = value.lower()
        for pid, row in self.profile_dict.items():
            names = {str((row or {}).get('name', '')).lower(),
                     str((row or {}).get('username', '')).lower()}
            if target in names:
                return pid
        return ''

    def get_creative_assets(self, profile_id):
        """Asset-library assets for a profile, as ``[{'id','name'}]`` for
        the picker. v3 has no media-upload endpoint — assets are created
        in the Reddit dashboard and referenced by their hosted url."""
        return [{'id': aid, 'name': (asset or {}).get('name', aid)}
                for aid, asset
                in self.list_creative_assets(profile_id).items()]

    def list_creative_assets(self, profile_id):
        """Asset-library assets under a profile, keyed by id. Cached per
        profile. Each asset carries ``media.permanent_url`` — the hosted
        url a Post references as its ``media_url``."""
        if not profile_id:
            return {}
        if profile_id in self.asset_dict:
            return self.asset_dict[profile_id]
        url = '{}/profiles/{}/creative_assets'.format(base_url, profile_id)
        assets = {}
        for row in self._paginate(url):
            # List wraps each asset as {'result': ...}; Get returns it bare.
            asset = (row.get('result')
                     if isinstance(row, dict) and 'result' in row else row)
            aid = (asset or {}).get('id')
            if aid:
                assets[aid] = asset
        self.asset_dict[profile_id] = assets
        return assets

    def resolve_asset_media(self, profile_id, name):
        """``(media_url, post_type)`` for an asset-library asset matched by
        name under ``profile_id``. A value that already looks like a url is
        passed straight through. ``('', '')`` when unresolved."""
        value = str(name or '').strip()
        if not value:
            return '', ''
        if value.lower().startswith(('http://', 'https://')):
            return value, ''
        target = value.lower()
        for asset in self.list_creative_assets(profile_id).values():
            if str((asset or {}).get('name', '')).lower() != target:
                continue
            media = (asset or {}).get('media') or {}
            url = media.get('permanent_url') or ''
            mime = str(media.get('mime_type') or '')
            return url, ('VIDEO' if mime.startswith('video') else 'IMAGE')
        return '', ''

    def resolve_country_geo(self, value):
        """Country-level geolocation id for a country name or ISO-2 code,
        '' when unknown. Cached per code. Reddit targets geolocations by
        id while the plan supplies country names — resolve them live."""
        text = str(value or '').strip()
        if not text:
            return ''
        code = COUNTRY_TO_ISO.get(text.lower(), text.upper())
        if len(code) != 2:
            return ''
        if code in self._geo_cache:
            return self._geo_cache[code]
        gid, rows = '', []
        try:
            url = '{}/targeting/geolocations'.format(base_url)
            rows = (self._get(url, params={'country': code}).json()
                    or {}).get('data') or []
        except (ValueError, requests.exceptions.RequestException) as e:
            logging.warning(
                'Reddit geolocation lookup failed for %s: %s', code, e)
        # Prefer the country-level row (no city/region); else the first.
        for row in rows:
            if not row.get('city') and not row.get('region'):
                gid = row.get('id') or ''
                break
        if not gid and rows:
            gid = rows[0].get('id') or ''
        self._geo_cache[code] = gid
        return gid

    def create_post(self, profile_id, post_dict):
        """Create a Post (the ad creative) under a profile; returns the
        raw create response for ``_populate_reddit_result``."""
        url = '{}/profiles/{}/posts'.format(base_url, profile_id)
        return self._post(url, body={'data': post_dict})

    def list_posts(self, profile_id):
        """Existing posts under a profile, keyed by id, for resolve-by-
        name (matched on the post headline)."""
        if not profile_id:
            return {}
        url = '{}/profiles/{}/posts'.format(base_url, profile_id)
        return {row['id']: row
                for row in self._paginate(url) if row.get('id')}

    def probe_account(self):
        """(ok, message) — verify the ad account is reachable, for the
        live pre-flight checks."""
        try:
            r = self._get(self._entity_url('funding_instruments'))
            body = r.json() if r is not None else {}
            if not isinstance(body, dict):
                body = {}
            err = _extract_error(body)
            if err:
                return False, str(err.get('message') or err)
            return True, ''
        except Exception as e:
            return False, str(e)

    entity_segments_by_level = {'Campaign': 'campaigns',
                                'Adset': 'ad_groups', 'Ad': 'ads'}

    def update_statuses(self, object_level, platform_ids, activate=True):
        """PATCH ``configured_status`` on existing objects by id.
        Returns one dict per id: {'platform_id', 'status'
        ('updated'|'failed'), 'error_code', 'error_message'}."""
        segment = self.entity_segments_by_level.get(object_level)
        status = 'ACTIVE' if activate else 'PAUSED'
        results = []
        for pid in platform_ids:
            result = utl.new_update_result(pid)
            if not segment:
                results.append(utl.fail_result(
                    result, f'Unknown Reddit level: {object_level}'))
                continue
            try:
                url = f'{self._entity_url(segment)}/{pid}'
                r = self._patch(
                    url, body={'data': {'configured_status': status}})
                self._fail_if_refused(result, r)
            except Exception as e:
                utl.fail_result(result, e)
            results.append(result)
        return results

    @staticmethod
    def _fail_if_refused(result, response):
        """Mark a PATCH result failed when Reddit refuses it."""
        err = _extract_error(utl.response_body(response))
        status = getattr(response, 'status_code', None)
        if response is not None and not err and (status or 0) < 400:
            return result
        return utl.fail_result(
            result, err.get('message') or err.get('detail')
            or 'Unknown error from Reddit Ads',
            err.get('code') or status)

    def update_object(self, object_level, platform_id, changes,
                      context=None):
        """Push one object's whitelisted edits in a single PATCH."""
        result = utl.new_update_result(platform_id)
        try:
            body = self.update_body(object_level, changes)
            segment = self.entity_segments_by_level[object_level]
            url = f'{self._entity_url(segment)}/{platform_id}'
            self._fail_if_refused(
                result, self._patch(url, body={'data': body}))
        except Exception as e:
            utl.fail_result(result, e)
        return result

    @staticmethod
    def update_body(object_level, changes):
        """Build one complete Reddit PATCH payload."""
        fields = UPDATE_COLUMN_FIELDS.get(object_level)
        if not fields:
            raise ValueError(
                f'No update support for Reddit level: {object_level}')
        body = {}
        for col, value in (changes or {}).items():
            if col not in fields:
                raise ValueError(f'Column not updatable: {col}')
            body[fields[col]] = RedditApi.update_value(col, value)
        if not body:
            raise ValueError('No changes supplied.')
        return body

    @staticmethod
    def update_value(col, value):
        """Transform spreadsheet money and dates for Reddit."""
        if col in MONEY_UPDATE_COLS:
            micros = _to_micros(value)
            if micros is None:
                raise ValueError(
                    f'Non-numeric money value for {col}: {value!r}')
            return micros
        if col in DATE_UPDATE_COLS:
            iso = _to_iso(value)
            if iso is None:
                raise ValueError(
                    f'Unparseable datetime for {col}: {value!r}')
            return iso
        return str(value)


class CampaignUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'Reddit campaign config'
    file_name = 'campaign_upload.xlsx'
    name = 'name'
    objective = 'objective'
    status = 'configured_status'
    funding_instrument_id = 'funding_instrument_id'
    spend_cap = 'spend_cap'
    snapshot_cols = [objective, status, funding_instrument_id, spend_cap]

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
        _populate_reddit_result(
            result, api.create_entity(campaign, entity_name='campaigns'))
        if result['status'] == 'created':
            campaign.id = result['platform_id']
        return result


class Campaign(object):
    __slots__ = ['name', 'objective', 'configured_status',
                 'funding_instrument_id', 'spend_cap',
                 'effective_status', 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.objective = 'CLICKS'
        self.configured_status = 'PAUSED'
        self.funding_instrument_id = None
        self.spend_cap = None
        self.effective_status = None
        utl.apply_row(self, row_dict)
        self.api = api
        self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        if not self.name:
            return {}
        d = {
            'name': str(self.name),
            'objective': str(self.objective or 'CLICKS'),
            'configured_status': str(self.configured_status or 'PAUSED'),
        }
        if self.funding_instrument_id:
            d['funding_instrument_id'] = str(self.funding_instrument_id)
        if self.spend_cap:
            micros = _to_micros(self.spend_cap)
            if micros is not None:
                d['spend_cap'] = micros
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


class AdGroupUpload(utl.BaseUploadConfig):
    """Reddit's mid-tier object (Adset in LQ parlance)."""
    config_dir = config_path
    config_label = 'Reddit adgroup config'
    file_name = 'adset_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    configured_status = 'configured_status'
    bid_strategy = 'bid_strategy'
    bid_type = 'bid_type'
    bid_value = 'bid_value'
    goal_type = 'goal_type'
    goal_value = 'goal_value'
    optimization_goal = 'optimization_goal'
    # Required for ALL ad groups starting 2026-07-13 (optional before).
    conversion_pixel_id = 'conversion_pixel_id'
    start_time = 'start_time'
    end_time = 'end_time'
    # Targeting columns (pipe/comma-delimited cells).
    communities = 'communities'
    geolocations = 'geolocations'
    interests = 'interests'
    devices = 'devices'
    platforms = 'platforms'
    gender = 'gender'
    snapshot_cols = [configured_status, bid_strategy, bid_type, bid_value,
                     goal_type, goal_value, optimization_goal,
                     conversion_pixel_id, start_time, end_time]

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
                 'bid_strategy', 'bid_type', 'bid_value', 'goal_type',
                 'goal_value', 'optimization_goal', 'conversion_pixel_id',
                 'start_time', 'end_time', 'communities', 'geolocations',
                 'interests', 'devices', 'platforms', 'gender',
                 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.campaignId = None
        self.configured_status = 'PAUSED'
        self.bid_strategy = 'MAXIMIZE_VOLUME'
        self.bid_type = None
        self.bid_value = None
        self.goal_type = 'DAILY_SPEND'
        self.goal_value = None
        self.optimization_goal = None
        self.conversion_pixel_id = None
        self.start_time = None
        self.end_time = None
        self.communities = None
        self.geolocations = None
        self.interests = None
        self.devices = None
        self.platforms = None
        self.gender = None
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
        if not (self.name and self.campaignId):
            return {}
        d = {
            'name': str(self.name),
            'campaign_id': str(self.campaignId),
            'configured_status': str(self.configured_status or 'PAUSED'),
            'bid_strategy': str(self.bid_strategy or 'MAXIMIZE_VOLUME'),
            'goal_type': str(self.goal_type or 'DAILY_SPEND'),
        }
        goal = _to_micros(self.goal_value)
        if goal is not None:
            d['goal_value'] = goal
        if self.bid_type:
            d['bid_type'] = str(self.bid_type)
        bid = _to_micros(self.bid_value)
        if bid is not None:
            d['bid_value'] = bid
        if self.optimization_goal:
            d['optimization_goal'] = str(self.optimization_goal)
        if self.conversion_pixel_id:
            d['conversion_pixel_id'] = str(self.conversion_pixel_id)
        for col, value in ((AdGroupUpload.start_time, self.start_time),
                           (AdGroupUpload.end_time, self.end_time)):
            iso = _to_iso(value)
            if iso:
                d[col] = iso
        targeting = self.create_targeting_dict()
        if targeting:
            d['targeting'] = targeting
        return d

    def create_targeting_dict(self):
        """Assemble the v3 ad-group ``targeting`` object from the pipe-
        delimited targeting columns. Communities/interests pass through as
        the ids/names the config supplies; geolocations resolve country
        names -> Reddit geo ids live; devices/platforms/gender map to the
        v3 enums. Empty when nothing is set (Reddit then runs broad)."""
        targeting = {}
        communities = [normalize_community(value)
                       for value in utl.split_list(self.communities)]
        if not all(communities):
            raise utl.UploaderTargetingError(
                'Verify every Reddit community before uploading.')
        if communities:
            targeting['communities'] = communities
        interests = utl.split_list(self.interests)
        if interests:
            targeting['interests'] = interests
        geos = []
        for value in utl.split_list(self.geolocations):
            gid = self.api.resolve_country_geo(value) if self.api else ''
            # Resolved country -> geo id; else assume it already is one.
            geos.append(gid or value)
        if geos:
            targeting['geolocations'] = geos
        devices = [{'type': d.upper()} for d in utl.split_list(self.devices)
                   if d.upper() in ('DESKTOP', 'MOBILE')]
        if devices:
            targeting['devices'] = devices
        platforms = []
        for p in utl.split_list(self.platforms):
            mapped = PLATFORM_MAP.get(p.upper())
            if mapped and mapped not in platforms:
                platforms.append(mapped)
        if platforms:
            targeting['platforms'] = platforms
        gender = str(self.gender or '').strip().upper()
        if gender in ('MALE', 'FEMALE'):
            targeting['gender'] = gender
        return targeting

    def check_exists(self, api):
        if not api.adgroup_dict:
            api.set_id_dict('adgroup', filter_id=self.campaignId)
        found = api.get_id(api.adgroup_dict, self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'Reddit ad config'
    file_name = 'ad_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    adgroup = 'ad_group'
    creative = 'creative'
    configured_status = 'configured_status'
    profile = 'profile'
    headline = 'headline'
    call_to_action = 'call_to_action'
    destination_url = 'destination_url'
    thumbnail = 'thumbnail'
    post_type = 'post_type'
    snapshot_cols = [configured_status, headline, call_to_action,
                     destination_url]

    def upload_all_ads(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, a_id in enumerate(self.config):
            ad = Ad(self.config[a_id], api=api)
            logging.info(
                f'Uploading Reddit ad {idx + 1} of {total}: {ad.name}')
            result = self.upload_ad(api, ad)
            result['pushed_values'] = {
                **utl.snapshot_values(self.config[a_id], self.snapshot_cols),
                'post_id': str(ad.postId or ''),
                'profile_id': str(ad.profileId or '')}
            results.append(result)
        return results

    @staticmethod
    def upload_ad(api, ad):
        result = utl.new_result('Ad', ad.name, CHANNEL, ad.adGroupId)
        if not ad.adGroupId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = f'Ad group {ad.ad_group!r} not found'
            return result
        if not ad.postId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Post for creative {ad.creative!r} could not be resolved '
                f'or created')
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
                 'creative', 'configured_status', 'profile', 'profileId',
                 'headline', 'call_to_action', 'destination_url',
                 'thumbnail', 'post_type', 'postId', 'upload_dict',
                 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.ad_group = None
        self.adGroupId = None
        self.creative = None
        self.configured_status = 'PAUSED'
        self.profile = None
        self.profileId = None
        self.headline = None
        self.call_to_action = None
        self.destination_url = None
        self.thumbnail = None
        self.post_type = None
        self.postId = None
        utl.apply_row(self, row_dict)
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
        self.profileId = api.resolve_profile_id(self.profile)
        if self.creative:
            post = Post({'name': self.creative,
                         'profile': self.profile,
                         'profileId': self.profileId,
                         'headline': self.headline,
                         'call_to_action': self.call_to_action,
                         'destination_url': self.destination_url,
                         'thumbnail': self.thumbnail,
                         'post_type': self.post_type}, api=api)
            self.postId = post.id

    def create_ad_dict(self):
        if not (self.name and self.adGroupId and self.postId):
            return {}
        d = {
            'name': str(self.name),
            'ad_group_id': str(self.adGroupId),
            'post_id': str(self.postId),
            'configured_status': str(self.configured_status or 'PAUSED'),
        }
        if self.destination_url:
            d['click_url'] = str(self.destination_url)
        if self.profileId:
            d['profile_id'] = str(self.profileId)
        return d

    def check_exists(self, api):
        if not api.ad_dict:
            api.set_id_dict('ad', filter_id=self.adGroupId)
        found = api.get_id(api.ad_dict, self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class Post(object):
    """Resolve or create the Reddit Post an Ad references. v3 has no
    creative/media endpoint: the creative is a Post created under a
    profile (``POST /profiles/{id}/posts``) whose ``content`` points at a
    hosted ``media_url``. That url comes from the asset library (asset
    name -> ``media.permanent_url``) or a direct url given in the creative
    column. An existing post is matched by headline so re-runs link it
    instead of duplicating it."""
    __slots__ = ['name', 'profile', 'profileId', 'headline',
                 'call_to_action', 'destination_url', 'thumbnail',
                 'post_type', 'id', 'api']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.profile = None
        self.profileId = None
        self.headline = None
        self.call_to_action = None
        self.destination_url = None
        self.thumbnail = None
        self.post_type = None
        utl.apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.set_id(self.api)

    def set_id(self, api):
        if not self.profileId:
            self.profileId = api.resolve_profile_id(self.profile)
        if not self.profileId:
            logging.warning(
                'Reddit creative %r has no resolvable profile; an ad needs '
                'a profile to author its post.', self.name)
            return
        if self.resolve_existing(api):
            return
        self.create(api)

    def resolve_existing(self, api):
        """Match an existing post under the profile by headline (falling
        back to the creative name). Sets ``id`` and returns True on hit so
        a re-run links the existing post instead of duplicating it."""
        wanted = str(self.headline or self.name or '').strip().lower()
        if not wanted:
            return False
        for pid, row in api.list_posts(self.profileId).items():
            if str((row or {}).get('headline', '')).strip().lower() == wanted:
                self.id = pid
                return True
        return False

    def create(self, api):
        media_url, derived_type = api.resolve_asset_media(
            self.profileId, self.name)
        if not media_url:
            logging.warning(
                'Reddit creative %r did not resolve to a media url '
                '(asset-library asset name or direct url); post not '
                'created.', self.name)
            return
        thumb_url = ''
        if self.thumbnail:
            thumb_url, _ = api.resolve_asset_media(
                self.profileId, self.thumbnail)
        ptype = str(self.post_type or derived_type or 'IMAGE').upper()
        # Pre-check type rules so a miss logs a reason, not a raw 400.
        if ptype in ('IMAGE', 'CAROUSEL') and not self.destination_url:
            logging.warning(
                'Reddit %s post %r needs a destination_url; post not '
                'created.', ptype, self.name)
            return
        if ptype == 'VIDEO' and not thumb_url:
            logging.warning(
                'Reddit VIDEO post %r needs a thumbnail; post not created.',
                self.name)
            return
        result = utl.new_result('Post', self.name, CHANNEL)
        _populate_reddit_result(result, api.create_post(
            self.profileId,
            self.create_post_dict(ptype, media_url, thumb_url)))
        if result['status'] == 'created':
            self.id = result['platform_id']
        else:
            logging.warning('Reddit post %r create failed: %s',
                            self.name, result['error_message'])

    def create_post_dict(self, post_type, media_url, thumb_url=''):
        content = {'media_url': str(media_url),
                   'call_to_action': _to_cta(self.call_to_action)}
        if self.destination_url:
            content['destination_url'] = str(self.destination_url)
        d = {'type': str(post_type),
             'headline': str(self.headline or self.name or ''),
             'content': [content]}
        if thumb_url:
            d['thumbnail_url'] = str(thumb_url)
        return d


# Spreadsheet columns mapped to Reddit PATCH fields.
UPDATE_COLUMN_FIELDS = {
    'Campaign': {
        CampaignUpload.name: 'name',
        CampaignUpload.spend_cap: 'spend_cap',
    },
    'Adset': {
        AdGroupUpload.name: 'name',
        AdGroupUpload.goal_value: 'goal_value',
        AdGroupUpload.bid_value: 'bid_value',
        AdGroupUpload.start_time: 'start_time',
        AdGroupUpload.end_time: 'end_time',
    },
}

MONEY_UPDATE_COLS = (CampaignUpload.spend_cap, AdGroupUpload.goal_value,
                     AdGroupUpload.bid_value)

DATE_UPDATE_COLS = (AdGroupUpload.start_time, AdGroupUpload.end_time)
