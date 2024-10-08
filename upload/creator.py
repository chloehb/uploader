import os
import logging
import itertools
import numpy as np
import pandas as pd
import uploader.upload.utils as utl

file_path = utl.config_file_path
log = logging.getLogger()


class CreatorConfig(object):
    col_file_name = 'file_name'
    col_new_file = 'new_file'
    col_create_type = 'create_type'
    col_column_name = 'column_name'
    col_overwrite = 'overwrite'
    col_filter = 'file_filter'

    def __init__(self, file_name=None):
        self.file_name = file_name
        self.full_file_name = os.path.join(file_path, self.file_name)
        self.cur_file_name = None
        self.cur_new_file = None
        self.cur_create_type = None
        self.cur_column_name = None
        self.cur_overwrite = None
        self.job_dict = None
        self.error_dict = {}
        if self.file_name:
            self.config = self.read_config(self.full_file_name)

    @staticmethod
    def read_config(file_name):
        logging.info('Loading config file: {}'.format(file_name))
        df = pd.read_excel(file_name)
        df_dict = df.to_dict(orient='index')
        return df_dict

    def do_all(self):
        for key in self.config:
            job = self.set_job(key)
            error_dict = self.do_job(key)
            self.error_dict[job.new_file] = error_dict
        utl.dir_remove(utl.err_file_path)
        return self.error_dict

    def set_job(self, key):
        job = Job(self.config[key])
        return job

    def do_job(self, key):
        job = self.set_job(key)
        msg = 'Doing job from {} on {} of type {}.'.format(
            job.file_name, job.new_file, job.create_type)
        logging.info(msg)
        error_dict = job.do_job()
        return error_dict


class Job(object):
    match = 'match'
    create = 'create'
    duplicate = 'duplicate'
    relation = 'relation'
    mediaplan = 'mediaplan'

    def __init__(self, job_dict=None, file_name=None, new_file=None,
                 create_type=None, column_name=None, overwrite=None,
                 file_filter=None):
        self.file_name = file_name
        self.new_file = new_file
        self.create_type = create_type
        self.column_name = column_name
        self.overwrite = overwrite
        self.file_filter = file_filter
        self.df = None
        if job_dict:
            for k in job_dict:
                setattr(self, k, job_dict[k])

    def get_df(self):
        if self.create_type == self.mediaplan:
            mp = MediaPlan(self.file_name, first_row=0)
            df = mp.df
        else:
            utl.dir_check(file_path)
            for path_split in ['\\', '/']:
                if path_split in self.file_name:
                    new_path = self.file_name.split(path_split)[:-1]
                    new_path = path_split.join(new_path)
                    utl.dir_check(new_path)
            file_name = file_path + self.file_name
            logging.info('Reading from file: {}'.format(file_name))
            if not os.path.exists(file_name):
                logging.warning('File does not exist: {}'.format(file_name))
                cols = []
                if self.create_type == self.relation:
                    cols = [Creator.rel_col_name, Creator.rel_col_pos,
                            Creator.rel_col_val, Creator.rel_col_imp,
                            Creator.rel_col_imp_new_value]
                elif self.create_type == self.create:
                    cols = [self.create]
                if cols:
                    df = pd.DataFrame(columns=cols)
                    df.to_excel(file_name, index=False)
            df = pd.read_excel(file_name, dtype=object,
                               keep_default_na=False, na_values=[''])
        if str(self.file_filter) != 'nan':
            df = self.filter_df(df)
        return df

    def filter_df(self, df):
        self.file_filter = self.file_filter.split('::')
        filter_col = self.file_filter[0]
        filter_vals = self.file_filter[1].split('|')
        df = df[df[filter_col].isin(filter_vals)].copy()
        return df

    def do_job(self):
        df = self.get_df()
        cr = Creator(self.column_name, self.overwrite,
                     self.new_file, file_path, df=df)
        error_dict = {}
        if self.create_type == self.create:
            cr.create_upload_file()
        elif self.create_type == self.duplicate:
            cr.apply_duplication()
        elif self.create_type == self.relation:
            error_dict = cr.apply_relations()
        elif self.create_type == self.mediaplan:
            cr.get_plan_names()
        elif self.create_type == self.match:
            cr.generate_from_match_table()
        return error_dict


class Creator(object):
    rel_col_imp = 'impacted_column_name'
    rel_col_name = 'column_name'
    rel_col_val = 'column_value'
    rel_col_pos = 'position'
    rel_col_imp_new_value = 'impacted_column_new_value'

    def __init__(self, col_name, overwrite, new_file,
                 cc_file_path='config/create/', df=None, config_file=None):
        self.df = df
        self.col_name = col_name
        self.overwrite = overwrite
        self.new_file = new_file
        self.config_file = config_file
        self.error_dict = {}
        if cc_file_path and self.new_file:
            self.new_file = os.path.join(file_path, self.new_file)
        if cc_file_path and self.config_file:
            self.config_file = os.path.join(file_path, self.config_file)
        if self.config_file:
            self.df = pd.read_excel(file_path + self.config_file)

    def get_combined_list(self):
        z = list(itertools.product(*[self.df[x].dropna().values
                                     for x in self.df.columns]))
        combined_list = ['_'.join(map(str, x)) for x in z]
        return combined_list

    def create_df(self, new_values):
        df = pd.DataFrame()
        cols = [self.col_name]
        if os.path.exists(self.new_file):
            df = pd.read_excel(self.new_file)
            cols = df.columns
        ndf = pd.DataFrame(data={self.col_name: pd.Series(new_values)},
                           columns=cols)
        if not self.overwrite:
            df = pd.concat([df, ndf], ignore_index=True, sort=False)
            df = df.reset_index(drop=True)
        else:
            df = ndf
        return df

    def create_upload_file(self):
        combined_list = self.get_combined_list()
        df = self.create_df(combined_list)
        logging.info('Writing {} row(s) to {}'.format(len(df), self.new_file))
        utl.write_df(df, self.new_file)

    def apply_relations(self):
        cdf = pd.read_excel(self.new_file)
        for imp_col in self.df['impacted_column_name'].unique():
            df = self.df[self.df['impacted_column_name'] == imp_col]
            par_col = df['column_name'].values
            if len(par_col) == 0:
                continue
            par_col = str(par_col[0]).split('|')
            position = str(df['position'].values[0]).split('|')
            if position == ['Constant']:
                cdf[imp_col] = df['impacted_column_new_value'].values[0]
            else:
                rel_dict = self.create_relation_dictionary(df)
                cdf = self.set_values_to_imp_col(cdf, position, par_col,
                                                 imp_col)
                cdf = self.check_undefined_relation(cdf, rel_dict, imp_col)
                cdf[imp_col] = cdf[imp_col].replace(rel_dict)
        utl.write_df(cdf, self.new_file)
        return self.error_dict

    @staticmethod
    def create_relation_dictionary(df):
        df = df[['column_value', 'impacted_column_new_value']]
        rel_dict = pd.Series(df['impacted_column_new_value'].values,
                             index=df['column_value']).to_dict()
        return rel_dict

    @staticmethod
    def set_values_to_imp_col(df, position, par_col, imp_col):
        if position == ['nan']:
            df[imp_col] = df[par_col[0]]
        else:
            if len(position) != len(par_col):
                logging.warning('Length mismatch between {} and {}'
                                ''.format(par_col, position))
                par_col = par_col + [
                    par_col[0] for x in range(len(position) - len(par_col))]
            for idx, pos in enumerate(position):
                if str(pos) == '':
                    new_series = df[par_col[int(idx)]]
                else:
                    new_series = (
                        df[par_col[int(idx)]].astype('U').str.split('_')
                        .str[int(pos)])
                if idx == 0:
                    df[imp_col] = new_series
                else:
                    df[imp_col] = df[imp_col] + '|' + new_series
        return df

    def check_undefined_relation(self, df, rel_dict, imp_col):
        undefined = df.loc[~df[imp_col].isin(rel_dict), imp_col]
        imp_file = self.new_file.split('.')[0].replace(file_path, '')
        file_name = utl.err_file_path + imp_file + '_' + imp_col + '.xlsx'
        if not undefined.empty:
            msg = ('{} No match found for the following values, '
                   'they were left blank.  An error report was '
                   'generated {}'.format(imp_col, undefined.head().values))
            logging.warning(msg)
            df.loc[~df[imp_col].isin(rel_dict), imp_col] = ''
            self.error_dict[imp_col] = len(undefined.unique())
            err_file_path = os.path.join(
                *[x for x in file_name.split('/') if '.' not in x])
            utl.dir_check(err_file_path)
            utl.write_df(undefined.drop_duplicates(), file_name)
        else:
            self.error_dict[imp_col] = 0
            utl.remove_file(file_name)
        return df

    def apply_upload_filter(self, df):
        primary_col = self.col_name.split('::')[0]
        file_name = self.col_name.split('::')[2]
        file_name = file_path + file_name
        filter_df = pd.read_excel(file_name)
        filter_cols = [x.split('::') for x in filter_df.columns
                       if x not in primary_col]
        filter_dicts = filter_df.to_dict('records')
        ndf = pd.DataFrame()
        for filter_dict in filter_dicts:
            filtered_df = df.copy()
            for col in filter_cols:
                filter_col = '::'.join(col)
                col_name = col[0]
                col_position = int(col[1])
                filtered_df[filter_col] = filtered_df[
                    col_name].str.split('_').str[col_position]
                primary_val = filter_dict[primary_col]
                filter_vals = filter_dict[filter_col].split('|')
                filtered_df = filtered_df[
                    (filtered_df[primary_col] == primary_val) &
                    (filtered_df[filter_col].isin(filter_vals))]
            ndf = pd.concat([df, filtered_df], ignore_index=True, sort=False)
            ndf = ndf.reset_index(drop=True)
        for col in filter_cols:
            ndf = ndf.drop('::'.join(col), axis=1)
        return ndf

    def apply_duplication(self):
        cdf = pd.read_excel(self.new_file)
        original_cols = cdf.columns
        duplicated_col = self.col_name.split('::')[0]
        unique_list = cdf[duplicated_col].unique()
        cdf = pd.DataFrame(columns=original_cols)
        self.df = self.df[self.col_name.split('::')[1].split('|')][:]
        for item in unique_list:
            self.df[duplicated_col] = item
            cdf = pd.concat([cdf, self.df], ignore_index=True, sort=False)
        cdf = cdf.reset_index(drop=True)
        cdf = cdf[original_cols]
        if len(self.col_name.split('::')) > 2:
            cdf = self.apply_upload_filter(cdf)
        utl.write_df(cdf, self.new_file)

    def get_plan_names(self):
        self.col_name = self.col_name.split('|')
        df_dict = {}
        for col in self.col_name:
            if col in self.df.columns:
                df_dict[col] = pd.Series(self.df[col].unique())
            else:
                logging.warning('{} not in df.  Continuing.'.format(col))
        ndf = pd.DataFrame(df_dict)
        logging.info('Plan write {} columns {} to : {}'.format(
            len(self.col_name), self.col_name, self.new_file))
        utl.write_df(ndf, './' + self.new_file)

    def generate_from_match_table(self):
        new_file_list = self.new_file.split('|')
        mt = MatchTable(df=self.df,
                        creator_file=str(new_file_list[0]),
                        filter_file=str(new_file_list[1]),
                        relation_file=str(new_file_list[2]))
        mt.generate_files_from_match_table()


class MatchTable(object):
    ad_col = 'Ad Name'
    ad_group_col = 'Ad Group Name'
    tag_url_col = 'Website URL'
    creative_col = 'Creative File Name'
    headline_col = 'Link Headline'
    description_col = 'Link Description'
    text_col = 'Post Text'
    max_carousel = 10

    def __init__(self, df=None, file_name='/create/ad_match_table.xlsx',
                 creator_file='/create/ad_name_creator.xlsx',
                 filter_file='/create/ad_upload_filter.xlsx',
                 relation_file='/create/ad_relation.xlsx'):
        self.file_name = file_name
        self.df = df
        self.creator_file = creator_file
        self.filter_file = filter_file
        self.relation_file = relation_file
        self.clean_initial_df()

    def clean_initial_df(self):
        col = self.ad_group_col
        if col in self.df.columns:
            self.df[col] = self.df[col].fillna(method='ffill')

    @staticmethod
    def carousel_to_one_col(df, fixed_col_name, orig_col_name,
                            car_col_name, loops, loop_num_in_col=True):
        logging.info('{} doing {} loops'.format(fixed_col_name, loops))
        df[fixed_col_name] = df[orig_col_name]
        df[fixed_col_name] = df[fixed_col_name].fillna('')
        if not loop_num_in_col:
            loops = 2
        for col_num in range(1, loops):
            if loop_num_in_col:
                car_col = '{}{}'.format(car_col_name, col_num)
            else:
                car_col = car_col_name
            if car_col in df.columns:
                if col_num == 1:
                    df[fixed_col_name] = np.where(
                        ~df[car_col].isna(), df[car_col],
                        df[fixed_col_name])
                else:
                    df[fixed_col_name] = np.where(
                        ~df[car_col].isna(),
                        df[fixed_col_name] + '|' + df[car_col],
                        df[fixed_col_name])
        return df

    @staticmethod
    def get_fixed_col_name(col):
        fixed_col_name = '{} - Fixed'.format(col)
        return fixed_col_name

    def set_all_columns(self):
        for col in [self.creative_col, self.headline_col, self.description_col,
                    self.text_col]:
            car_col_name = '{} C'.format(col)
            fixed_col_name = self.get_fixed_col_name(col)
            loop_num_in_col = True
            if col == self.text_col:
                car_col_name = 'Carousel Text'
                loop_num_in_col = False
            self.df = self.carousel_to_one_col(
                df=self.df, fixed_col_name=fixed_col_name, orig_col_name=col,
                car_col_name=car_col_name, loops=self.max_carousel,
                loop_num_in_col=loop_num_in_col)

    def check_creative_for_file_type(self, col):
        fixed_col = self.get_fixed_col_name(col)
        file_types = utl.static_types + utl.video_types
        df = pd.DataFrame(self.df[fixed_col].unique())
        current_creative = os.listdir("./creative/")
        current_creative = [(os.path.splitext(x)[0], x)
                            for x in current_creative]
        df[fixed_col] = ''
        for val in range(len(df)):
            creative_list = df[0][val].split('|')
            new_creative_list = []
            for creative in creative_list:
                if creative.split('.')[-1] not in file_types:
                    new_val = [x[1]
                               for x in current_creative if x[0] == creative]
                    if new_val:
                        new_val = new_val[0]
                    else:
                        new_val = creative
                else:
                    new_val = creative
                new_creative_list.append(new_val)
            df[fixed_col][val] = '|'.join(new_creative_list)
        replace_dict = pd.Series(df[fixed_col].values, index=df[0]).to_dict()
        self.df[fixed_col] = self.df[fixed_col].replace(replace_dict)

    def append_and_write_relation_df(self, relation_df):
        df = pd.read_excel(utl.config_file_path + self.relation_file,
                           dtype=object, keep_default_na=False, na_values=[''])
        df = df[~df[Creator.rel_col_imp].isin(['creative_filename', 'body',
                                               'description', 'title'])]
        df = pd.concat([df, relation_df], ignore_index=True, sort=False)
        df = df.drop_duplicates()
        utl.write_df(df, utl.config_file_path + self.relation_file)

    def format_as_relation_df(self):
        if self.ad_group_col in self.df.columns:
            self.df[Creator.rel_col_val] = self.df[self.ad_group_col] + '|' + \
                                          self.df[self.ad_col]
        else:
            self.df[Creator.rel_col_val] = self.df[self.ad_col]
        relation_df = pd.DataFrame()
        relation_df = self.set_relation_from_df(relation_df)
        relation_df = self.add_constant_values_in_df(relation_df)
        self.append_and_write_relation_df(relation_df)
        return relation_df

    def set_relation_from_df(self, relation_df):
        col_lists = [
            [self.get_fixed_col_name(self.creative_col), 'creative_filename'],
            [self.get_fixed_col_name(self.text_col), 'body'],
            [self.get_fixed_col_name(self.description_col), 'description'],
            [self.get_fixed_col_name(self.headline_col), 'title']]
        for col_list in col_lists:
            new_df = self.df[[Creator.rel_col_val, col_list[0]]].copy()
            new_df[Creator.rel_col_imp] = col_list[1]
            new_df = new_df.rename(
                columns={col_list[0]: Creator.rel_col_imp_new_value})
            relation_df = pd.concat([relation_df, new_df], ignore_index=True,
                                    sort=False)
        return relation_df

    def add_constant_values_in_df(self, relation_df):
        if self.ad_group_col in self.df.columns:
            relation_df[Creator.rel_col_name] = 'adset_name|ad_name'
            relation_df[Creator.rel_col_pos] = '|'
            if self.tag_url_col in self.df.columns:
                tag_df = self.df.groupby(
                    [self.ad_group_col, self.tag_url_col]
                ).size().reset_index().rename(columns={0: 'count'})
                tag_df = tag_df.drop(columns='count')
                tag_df = tag_df.rename(
                    columns={self.ad_group_col: Creator.rel_col_val,
                             self.tag_url_col: Creator.rel_col_imp_new_value})
                tag_df[Creator.rel_col_name] = 'adset_name'
                tag_df[Creator.rel_col_pos] = ''
                tag_df[Creator.rel_col_imp] = 'link_url'
                relation_df = pd.concat(
                    [relation_df, tag_df], ignore_index=True, sort=False)
        else:
            relation_df[Creator.rel_col_name] = 'ad_name'
            relation_df[Creator.rel_col_pos] = ''
        return relation_df

    def write_name_creator_file(self):
        df = pd.DataFrame(self.df[self.ad_col].unique(), columns=[self.ad_col])
        utl.write_df(df, file_name=utl.config_file_path + self.creator_file)

    def write_filter_file(self, df):
        filter_file_split = self.filter_file.split('::')
        filter_file_name = filter_file_split[0]
        if len(filter_file_split) > 1:
            filter_file_cols = filter_file_split[1].split(',')
        else:
            filter_file_cols = []
        df = df[[self.ad_col, self.ad_group_col]]
        ndf = df[[self.ad_col]].drop_duplicates().reset_index(drop=True)
        for col in filter_file_cols:
            col = int(col)
            tdf = pd.DataFrame(df[self.ad_group_col].str.split('_').str[col])
            new_col_name = 'adset_name::{}'.format(col)
            tdf = tdf.rename(columns={self.ad_group_col: new_col_name})
            tdf = tdf.join(df[[self.ad_col]]).drop_duplicates().reset_index(
                drop=True)
            fdf = pd.DataFrame()
            for value in tdf[self.ad_col].unique():
                unique_vals = '|'.join(tdf[tdf[self.ad_col] == value][
                                           new_col_name].unique().tolist())
                value_dict = {new_col_name: [unique_vals],
                              self.ad_col: [value]}
                fdf = pd.concat([fdf, pd.DataFrame(value_dict)],
                                ignore_index=True, sort=False)
            ndf = ndf.merge(fdf, on=self.ad_col)
        ndf = ndf.rename(columns={self.ad_col: 'ad_name'})
        utl.write_df(ndf, file_name=utl.config_file_path + filter_file_name)

    def generate_files_from_match_table(self):
        self.set_all_columns()
        self.check_creative_for_file_type(col=self.creative_col)
        self.format_as_relation_df()
        self.write_name_creator_file()
        self.write_filter_file(self.df)


class MediaPlan(object):
    campaign_id = 'Campaign ID'
    campaign_name = 'Campaign Name'
    partner_name = 'Partner Name'
    ad_type_name = 'Ad Type'
    ad_serving_name = 'Ad Serving Type'
    old_placement_phase = 'Placement Phase\n(If Needed) '
    old_campaign_phase = 'Campaign Phase\n(If Needed) '
    placement_phase = 'Placement Phase (If Needed) '
    campaign_phase = 'Campaign Phase (If Needed) '
    country_name = 'Country'
    targeting = 'Targeting'
    creative = 'Creative (If Needed)'
    copy = 'Copy (If Needed)'
    data_source = 'Data Source (If Needed)'
    buy_model = 'Buy Model'
    buy_rate = 'CPM / Cost Per Unit'
    start_date = 'Start Date'
    serving = 'Ad Serving Type'
    ad_rate = 'Ad Serving Rate'
    report_rate = 'Reporting Fee Rate'
    kpi = 'KPI (If Needed)'
    placement_objective = 'Placement Objective (If Needed)'
    service_fee_rate = 'Service Fee Rate'
    verification_rate = 'Ad Verification Rate'
    reporting_source = 'Reporting Source'
    device = 'Device'
    ad_size = 'Ad Size (WxH)'
    ad_type = 'Ad Type'
    placement_description = 'Placement Description'
    package_description = 'Package Description'
    creative_description = 'Creative Description'
    placement_name = 'Placement Name'

    def __init__(self, file_name, sheet_name='Media Plan', first_row=2):
        self.file_name = file_name
        self.sheet_name = sheet_name
        self.first_row = first_row
        self.campaign_omit_list = ['_____']
        if self.file_name:
            self.df = self.load_df()

    def read_df(self):
        df = pd.DataFrame()
        cols = [self.partner_name, self.campaign_name, self.placement_name,
                self.old_placement_phase, self.old_campaign_phase,
                self.placement_phase, self.campaign_phase]
        cols = cols + [x.replace(' Name', '') for x in cols]
        for first_row in range(10):
            df = pd.read_excel(
                self.file_name,
                sheet_name=self.sheet_name,
                header=first_row,
                keep_default_na=False,
                na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN',
                           '-NaN', 'null', '-nan', '1.#IND', '1.#QNAN', 'N/A',
                           'NULL', 'NaN', 'n/a', 'nan'])
            if [x for x in cols if x in df.columns]:
                break
        return df

    def load_df(self):
        df = self.read_df()
        rename_dict = {
            self.old_placement_phase: self.placement_phase,
            self.old_campaign_phase: self.campaign_phase,
            self.partner_name.replace(' Name', ''): self.partner_name}
        df = df.rename(columns=rename_dict)
        for val in self.campaign_omit_list:
            if self.campaign_name in df.columns:
                df[self.campaign_name] = df[self.campaign_name].replace(val, '')
        # df = self.apply_match_dict(df)
        return df

    def apply_match_dict(self, df, file_name='mediaplan/mp_dcm_match.xlsx'):
        for col in [self.partner_name, self.ad_type_name, self.ad_serving_name]:
            match_dict = pd.read_excel(file_name, sheet_name=col)
            match_dict = match_dict.set_index('MP').to_dict()['DBM']
            df[col] = df[col].replace(match_dict)
        return df

    def set_campaign_name(self):
        cnames = self.df[self.campaign_name].unique()
        cnames = [x for x in cnames if x and x not in self.campaign_omit_list]
        self.df[self.campaign_name] = cnames[0]
