import os
import re
import time
import logging
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import datetime as dt

config_file_path = 'config/'
err_file_path = 'ERROR_REPORTS/'
read_attempts = 5
static_types = ['jpg', 'png', 'jpeg']
video_types = ['mp4', 'mpg', 'm4v', 'mkv', 'webm', 'mov', 'avi', 'wmv', 'flv']


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def _json_safe(val):
    """Coerce a single config value to a JSON-serializable scalar."""
    try:
        if val is None or pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(val, 'item') and not isinstance(val, str):
        try:
            val = val.item()
        except (AttributeError, ValueError):
            pass
    if isinstance(val, (str, bool, int, float)):
        return val
    return str(val)


def apply_row(instance, row):
    """Copy excel-row keys onto a slotted upload object, logging
    keys that aren't declared in ``__slots__``."""
    for k, v in row.items():
        try:
            setattr(instance, k, v)
        except AttributeError as e:
            logging.warning(f'AttributeError: {e}')


def split_list(value):
    """Pipe/comma-delimited cell -> de-duped list of non-empty trimmed
    strings. Targeting columns arrive as ``'gaming|technology'`` cells.

    :param value: raw spreadsheet cell
    :returns: list of trimmed, de-duped strings ([] when blank/NaN)
    """
    if value is None:
        return []
    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return []
    out = []
    for part in re.split(r'[|,]', text):
        part = part.strip()
        if part and part not in out:
            out.append(part)
    return out


def snapshot_values(row, columns):
    """JSON-safe dict of ``columns`` pulled from an upload config row.

    Attached to each result row as ``pushed_values`` so the app can
    persist what was actually sent per object and later diff config
    edits against it. Values are kept in row (spreadsheet) space —
    pre platform transforms like cent/micro scaling — so they compare
    directly against regenerated upload files.
    """
    snap = {}
    for col in columns:
        if col not in row:
            continue
        val = row[col]
        if isinstance(val, (list, tuple)):
            snap[col] = [_json_safe(v) for v in val]
        else:
            snap[col] = _json_safe(val)
    return snap


RECENT_SCAN_CAP = 200


def join_list(values):
    """``|``-join the non-blank values, the inverse of :func:`split_list`."""
    return '|'.join(str(v) for v in (values or [])
                    if v is not None and str(v) != '')


def dig(row, path):
    """``row['a']['b']`` for ``path`` ``'a.b'``, where a numeric step
    indexes a list; None when any step is missing."""
    value = row
    for key in str(path or '').split('.'):
        if isinstance(value, list) and key.isdigit():
            value = value[int(key)] if int(key) < len(value) else None
        elif isinstance(value, dict):
            value = value.get(key)
        else:
            return None
    return value


def live_stamp(value):
    """A platform timestamp (ISO with an offset, epoch milliseconds or
    ``YYYY-MM-DD hh:mm:ss``) as a sortable UTC ISO string, else None."""
    text = '' if value is None else str(value).strip()
    if not text:
        return None
    try:
        stamp = (pd.to_datetime(int(text), unit='ms', utc=True)
                 if text.isdigit() else pd.to_datetime(text, utc=True))
    except (ValueError, TypeError, OverflowError):
        return None
    return None if pd.isna(stamp) else stamp.isoformat()


def recent_rows(objects, limit, id_key='id', name_key='name',
                parent_key=None, created_key=None, updated_key=None):
    """``list_recent``'s answer: each object as ``{'id', 'name',
    'parent_id', 'created', 'updated', 'fields'}`` read along :func:`dig`
    paths, the ``limit`` most recently changed first."""
    rows = []
    for obj in objects:
        fields = dict(obj or {})
        parent = dig(fields, parent_key)
        rows.append({'id': str(dig(fields, id_key) or ''),
                     'name': str(dig(fields, name_key) or ''),
                     'parent_id': str(parent) if parent else None,
                     'created': live_stamp(dig(fields, created_key)),
                     'updated': live_stamp(dig(fields, updated_key)),
                     'fields': fields})
    rows.sort(key=lambda r: r['updated'] or r['created'] or '', reverse=True)
    return rows[:limit]


def live_settings(values):
    """``values`` as level-file cells without the blanks, so a copied
    setting never overwrites a cell the uploader already holds."""
    return {col: str(val) for col, val in values.items()
            if val is not None and str(val).strip() != ''}


def response_body(response):
    """Parse an API response into a dict; unwrap one-item lists."""
    try:
        body = response.json() if response is not None else {}
    except (ValueError, AttributeError):
        body = {}
    if isinstance(body, list):
        body = body[0] if body else {}
    return body if isinstance(body, dict) else {}


def match_name(catalogue, name, label):
    """The ``catalogue`` row named ``name`` (casefold), else the one row
    containing it; a non-blank miss is logged as not found in ``label``."""
    wanted = str(name or '').strip().casefold()
    if not wanted:
        return None
    exact = [x for x in catalogue if x['name'].casefold() == wanted]
    partial = [x for x in catalogue if wanted in x['name'].casefold()]
    hit = exact or (partial if len(partial) == 1 else [])
    if not hit:
        logging.warning('%s not found in %s.', name, label)
    return hit[0] if hit else None


def new_result(object_level, source_name, uploader_type, parent_id=None):
    """The per-object result row every channel's upload loop returns.

    :param object_level: Campaign / Adset / Ad / Post
    :param source_name: name in the upload config
    :param uploader_type: channel name, e.g. 'Reddit'
    :param parent_id: platform id of the level above, when resolved
    :returns: result dict the app persists as an UploaderUploadedItem
    """
    return {
        'source_name': source_name,
        'object_level': object_level,
        'uploader_type': uploader_type,
        'platform_id': None,
        'parent_platform_id': str(parent_id) if parent_id else None,
        'status': None,
        'error_code': None,
        'error_message': None,
    }


def new_update_result(platform_id):
    """Return the shared optimistic update result."""
    return {
        'platform_id': platform_id,
        'status': 'updated',
        'error_code': None,
        'error_message': None,
    }


def fail_result(result, message, code=None):
    """Mark a create or update result failed and return it."""
    result['status'] = 'failed'
    result['error_code'] = str(code) if code else None
    result['error_message'] = str(message)
    return result


def _blamed_fields(error):
    """Dotted field names out of a Facebook blame_field_specs list."""
    try:
        specs = error.api_blame_field_specs() or []
    except (AttributeError, KeyError, TypeError):
        return []
    named = []
    for spec in specs:
        if isinstance(spec, (list, tuple)):
            spec = '.'.join(str(x) for x in spec if x)
        spec = str(spec).strip()
        if spec and spec not in named:
            named.append(spec)
    return named


def fb_error_detail(error, context=''):
    """Everything Meta said about a rejected call, as one line.

    ``api_error_message()`` alone is usually the generic "Invalid
    parameter"; the reason a human can act on and the field actually
    rejected live in ``error_user_msg`` and ``blame_field_specs``.

    :param error: the raised ``FacebookRequestError``
    :param context: object name to log this against; pass it so the
        run log carries the detail as well as the result row
    :returns: the joined detail, safe to store on a run result
    """
    try:
        body = (error.body() or {}).get('error') or {}
    except (AttributeError, TypeError):
        body = {}
    message = str(error.api_error_message() or '').strip()
    parts = [message or 'Request rejected']
    for key in ('error_user_title', 'error_user_msg'):
        val = str(body.get(key) or '').strip()
        if val and val not in parts:
            parts.append(val)
    blamed = _blamed_fields(error)
    if blamed:
        parts.append('rejected field(s): {}'.format(', '.join(blamed)))
    subcode = body.get('error_subcode')
    if subcode:
        parts.append('subcode {}'.format(subcode))
    detail = ' | '.join(parts)
    if context:
        logging.warning('{} was rejected: {}'.format(context, detail))
    return detail


class UploaderAuthError(Exception):
    """Channel credential/refresh failure — fatal, message secret-free."""


class UploaderTargetingError(Exception):
    """An audience the object can't be built without — fatal to that
    object, not to the run, so the caller records it and carries on."""


class BaseUploadConfig(object):
    """Excel-backed upload config shared by every channel's level.

    Subclasses set ``config_dir`` (the channel's config folder),
    ``file_name`` (the default workbook), ``name`` (the column a row is
    worthless without) and ``config_label`` (what a missing file is
    called in the log), then own only their own ``upload_all_*`` loop.
    """
    config_dir = config_file_path
    file_name = ''
    name = 'name'
    config_label = 'config'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file=''):
        """Read the workbook into ``self.config`` as {row index: row}.

        :param config_file: workbook name, defaulting to ``file_name``
        :returns: True when loaded, False when the file is absent
        """
        file_name = os.path.join(
            self.config_dir, config_file or self.file_name)
        if not os.path.exists(file_name):
            logging.warning(
                f'{self.config_label} missing: {file_name}')
            return False
        df = pd.read_excel(file_name)
        df = df.dropna(subset=[self.name]).fillna('')
        self.config = df.to_dict(orient='index')
        return True


class BaseCreativeStore(object):
    """Shared filename->platform-id bookkeeping for creative upload.

    Each ad channel keeps a small CSV mapping a local creative
    filename to the id(s) the platform returned, so re-runs skip files
    already uploaded. Subclasses set ``id_cols`` and implement
    ``_upload_one(api, file_path)`` returning ``{id_col: value}``; this
    base owns the find-new / upload-loop / persist / resolve cycle so
    each channel writes one method, not a bespoke pipeline.

    The shape was extracted from ``fbapi.Creative`` (the production
    reference), which keeps its own ``{path: hash}`` CSV format and is
    intentionally left untouched so existing ``creative_hashes.csv``
    files stay valid.
    """
    fn_col = 'filename'
    id_cols = ('id',)

    def __init__(self, id_file_name, creative_path='creative/'):
        self.creative_path = creative_path
        self.id_file_name = os.path.join(creative_path, id_file_name)
        self.records = {}
        self.load_config()

    def load_config(self):
        cols = [self.fn_col, *self.id_cols]
        if not os.path.isfile(self.id_file_name):
            dir_check(os.path.dirname(
                os.path.abspath(self.id_file_name)))
            pd.DataFrame(columns=cols).to_csv(
                self.id_file_name, index=False)
        df = pd.read_csv(self.id_file_name)
        self.records = {}
        for _, row in df.iterrows():
            fn = row.get(self.fn_col)
            if pd.isna(fn):
                continue
            self.records[str(fn)] = {
                c: (None if pd.isna(row.get(c)) else row.get(c))
                for c in self.id_cols}
        return self.records

    def get_new(self, filenames):
        """Bare filenames not already in the store (and not NaN)."""
        return [fn for fn in filenames
                if fn and str(fn) != 'nan' and fn not in self.records]

    def upload_all(self, api, filenames):
        new = self.get_new(filenames)
        total = len(new)
        for idx, fn in enumerate(new):
            logging.info('Uploading creative {} of {}.  Creative '
                         'Name: {}'.format(idx + 1, total, fn))
            path = os.path.join(self.creative_path, fn)
            if not os.path.isfile(path):
                logging.warning('{} not found.  It was not '
                                'uploaded'.format(path))
                continue
            ids = self._upload_one(api, path) or {}
            if not any(ids.get(c) for c in self.id_cols):
                logging.warning('{} did not return a creative id.  It '
                                'will be retried on the next run.'.format(fn))
                continue
            self.records[fn] = {c: ids.get(c) for c in self.id_cols}
        self.write()
        return self

    def _upload_one(self, api, file_path):
        """Push one local file to the platform; return ``{id_col:
        value}``. Implemented per channel."""
        raise NotImplementedError

    def get_id(self, filename, id_col=None):
        rec = self.records.get(filename)
        if not rec:
            return None
        return rec.get(id_col or self.id_cols[0])

    def write(self):
        rows = []
        for fn, rec in self.records.items():
            row = {self.fn_col: fn}
            for col in self.id_cols:
                row[col] = rec.get(col)
            rows.append(row)
        df = pd.DataFrame(rows, columns=[self.fn_col, *self.id_cols])
        try:
            df.to_csv(self.id_file_name, index=False)
        except IOError:
            logging.warning('{} could not be opened. Creative ids '
                            'not saved.'.format(self.id_file_name))


def dir_remove(directory):
    if os.path.isdir(directory):
        if not os.listdir(directory):
            os.rmdir(directory)


def write_df(df, file_name, sheet_name='Sheet1'):
    dir_name = os.path.dirname(os.path.abspath(file_name))
    dir_check(dir_name)
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    writer.close()
    invalidate_excel_cache(file_name)


def remove_file(file_name):
    try:
        os.remove(file_name)
    except OSError:
        pass
    invalidate_excel_cache(file_name)


_EXCEL_CACHE = {}
_EXCEL_CACHE_MAX = 64


def _excel_cache_key(file_name, kwargs):
    """Cache key for a workbook read, or None for anything that is not
    a path on disk (a BytesIO from a browser upload has no mtime)."""
    if not isinstance(file_name, (str, os.PathLike)):
        return None
    try:
        stat = os.stat(file_name)
    except OSError:
        return None
    return (os.path.abspath(file_name), stat.st_mtime_ns, stat.st_size,
            repr(sorted(kwargs.items())))


def invalidate_excel_cache(file_name):
    """Forget every cached read of ``file_name`` -- called by the
    writers here so a rewrite within one mtime tick cannot serve the
    old frame."""
    if not isinstance(file_name, (str, os.PathLike)):
        return
    path = os.path.abspath(file_name)
    for key in [k for k in _EXCEL_CACHE if k[0] == path]:
        del _EXCEL_CACHE[key]


def read_excel_cached(file_name, **kwargs):
    """``pd.read_excel`` with one parsed frame kept per (path, mtime,
    size, arguments), handed out as a copy.

    A ``--create`` run opens the same handful of workbooks four to six
    times per level -- the creator config, then the target file in
    each relation, duplication and filter pass -- so a plan save over
    fifteen levels was mostly openpyxl re-parsing files nothing had
    changed. Raises exactly as pandas does, so callers keep their
    error handling.

    :param file_name: path of the workbook, or an open file object
    :param kwargs: passed through to pandas.read_excel
    :return: the sheet as a DataFrame
    """
    key = _excel_cache_key(file_name, kwargs)
    if key is not None and key in _EXCEL_CACHE:
        return _EXCEL_CACHE[key].copy()
    df = pd.read_excel(file_name, **kwargs)
    if key is not None:
        if len(_EXCEL_CACHE) >= _EXCEL_CACHE_MAX:
            _EXCEL_CACHE.clear()
        _EXCEL_CACHE[key] = df.copy()
    return df


def exceldate_to_datetime(excel_date):
    epoch = dt.datetime(1899, 12, 30)
    delta = dt.timedelta(hours=round(excel_date * 24))
    return epoch + delta


def string_to_date(my_string):
    month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']
    if ('/' in my_string and my_string[-4:][:2] != '20' and
            ':' not in my_string and len(my_string) in [6, 7, 8]):
        try:
            return dt.datetime.strptime(my_string, '%m/%d/%y')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ('/' in my_string and my_string[-4:][:2] == '20' and
          ':' not in my_string):
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (((len(my_string) == 5) and (my_string[0] == '4')) or
          ((len(my_string) == 7) and ('.' in my_string))):
        return exceldate_to_datetime(float(my_string))
    elif len(my_string) == 8 and my_string.isdigit() and my_string[0] == '2':
        try:
            return dt.datetime.strptime(my_string, '%Y%m%d')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif len(my_string) == 8 and '.' in my_string:
        return dt.datetime.strptime(my_string, '%m.%d.%y')
    elif my_string == '0' or my_string == '0.0':
        return pd.NaT
    elif ((len(my_string) == 22) and (':' in my_string) and
          ('+' in my_string)):
        my_string = my_string[:-6]
        return dt.datetime.strptime(my_string, '%Y-%m-%d %M:%S')
    elif ((':' in my_string) and ('/' in my_string) and my_string[1] == '/' and
          my_string[4] == '/'):
        my_string = my_string[:9]
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (('PST' in my_string) and (len(my_string) == 28) and
          (':' in my_string)):
        my_string = my_string.replace('PST ', '')
        return dt.datetime.strptime(my_string, '%a %b %d %M:%S:%H %Y')
    elif (('-' in my_string) and (my_string[:2] == '20') and
          len(my_string) == 10):
        try:
            return dt.datetime.strptime(my_string, '%Y-%m-%d')
        except ValueError:
            try:
                return dt.datetime.strptime(my_string, '%Y-%d-%m')
            except ValueError:
                logging.warning('Could not parse date: {}'.format(my_string))
                return pd.NaT
    elif ((len(my_string) == 19) and (my_string[:2] == '20') and
          ('-' in my_string) and (':' in my_string)):
        try:
            return dt.datetime.strptime(my_string, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ((len(my_string) == 7 or len(my_string) == 8) and
          my_string[-4:-2] == '20'):
        return dt.datetime.strptime(my_string, '%m%d%Y')
    elif ((len(my_string) == 6 or len(my_string) == 5) and
          my_string[-3:] in month_list):
        my_string = my_string + '-' + dt.datetime.today().strftime('%Y')
        return dt.datetime.strptime(my_string, '%d-%b-%Y')
    elif len(my_string) == 24 and my_string[-3:] == 'GMT':
        my_string = my_string[4:-11]
        return dt.datetime.strptime(my_string, '%d%b%Y')
    else:
        return my_string


def data_to_type(df, float_col=None, date_col=None, str_col=None, int_col=None,
                 fill_empty=True):
    if float_col is None:
        float_col = []
    if date_col is None:
        date_col = []
    if str_col is None:
        str_col = []
    if int_col is None:
        int_col = []
    for col in float_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: x.replace('$', ''))
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = df[col].replace(['nan', 'NA'], 0)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].astype(float)
    for col in date_col:
        if col not in df:
            continue
        df[col] = df[col].replace(['1/0/1900', '1/1/1970'], '0')
        if fill_empty:
            df[col] = df[col].fillna(dt.datetime.today())
        else:
            df[col] = df[col].fillna(pd.Timestamp('nat'))
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
    for col in str_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].str.strip()
        df[col] = df[col].apply(lambda x: ' '.join(x.split()))
    for col in int_col:
        if col not in df:
            continue
        df[col] = df[col].astype(int)
    return df


def read_excel(file_name, kwargs=None):
    """
    Read a sheet, waiting out a workbook that is mid-save. A ValueError is
    pandas refusing the request itself -- a missing sheet, a header past
    the last row -- and answers empty at once rather than in five seconds.

    :param file_name: path of the workbook, or an open file object
    :param kwargs: passed through to pandas.read_excel
    :return: the sheet as a DataFrame, empty when it could not be read
    """
    if not kwargs:
        kwargs = {}
    for attempt in range(read_attempts):
        try:
            return read_excel_cached(file_name, **kwargs)
        except (zipfile.BadZipFile, EOFError, ET.ParseError) as e:
            logging.warning(e)
            if attempt < read_attempts - 1:
                time.sleep(1)
        except (FileNotFoundError, ValueError) as e:
            logging.warning(e)
            break
    return pd.DataFrame()
