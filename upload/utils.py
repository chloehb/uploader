import os
import time
import logging
import zipfile
import pandas as pd
import datetime as dt

config_file_path = 'config/'
err_file_path = 'ERROR_REPORTS/'
static_types = ['jpg', 'png', 'jpeg']
video_types = ['mp4', 'mpg', 'm4v', 'mkv', 'webm', 'mov', 'avi', 'wmv', 'flv']


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


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


def remove_file(file_name):
    try:
        os.remove(file_name)
    except OSError:
        pass


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
    Read excel with a wrapper on zipfile to prevent error if file is saving

    :param file_name:
    :return:
    """
    if not kwargs:
        kwargs = {}
    df = pd.DataFrame()
    for _ in range(5):
        try:
            df = pd.read_excel(file_name, **kwargs)
            break
        except (zipfile.BadZipFile, ValueError) as e:
            logging.warning(e)
            time.sleep(1)
    return df
