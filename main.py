import sys
import logging
import argparse
import upload.creator as cre
import upload.fbapi as fbapi
import upload.awapi as awapi
import upload.dcapi as dcapi
import upload.redditapi as redditapi
import upload.szkapi as szkapi


def set_log():
    formatter = logging.Formatter('%(asctime)s [%(module)14s]'
                                  '[%(levelname)8s] %(message)s')
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    log.addHandler(console)

    try:
        log_file = logging.FileHandler('logfile.log', mode='w')
        log_file.setFormatter(formatter)
        log.addHandler(log_file)
    except PermissionError as e:
        logging.warning('Could not open logfile with error: \n {}'.format(e))


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught exception: ",
                     exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception


def get_args(arguments=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--create', action='store_true')
    parser.add_argument('--api', choices=['all', 'fb', 'aw', 'szk', 'dcm'])
    parser.add_argument('--upload', choices=['all', 'c', 'as', 'ad'])
    if arguments:
        args = parser.parse_args(arguments.split())
    else:
        args = parser.parse_args()
    return args


def main(arguments=None):
    set_log()
    args = get_args(arguments)
    if args.create:
        crc = cre.CreatorConfig('create/creator_config.xlsx')
        return crc.do_all()
    results = []
    if args.api in ('all', 'fb'):
        api = fbapi.FbApi(config_file='fbconfig.json')
        if args.upload in ('all', 'c'):
            cu = fbapi.CampaignUpload(config_file='campaign_upload.xlsx')
            results.extend(cu.upload_all_campaigns(api=api) or [])
        if args.upload in ('all', 'as'):
            asu = fbapi.AdSetUpload(config_file='adset_upload.xlsx')
            results.extend(asu.upload_all_adsets(api=api) or [])
        if args.upload in ('all', 'ad'):
            ctv = fbapi.Creative(creative_file='creative_hashes.csv')
            adu = fbapi.AdUpload(config_file='ad_upload.xlsx')
            results.extend(adu.upload_all_ads(api, ctv) or [])
    if args.api in ('all', 'aw'):
        api = awapi.AwApi(config_file='awconfig.yaml')
        if args.upload in ('all', 'c'):
            cu = awapi.CampaignUpload(config_file='aw_campaign_upload.xlsx')
            results.extend(cu.upload_all_campaigns(api) or [])
        if args.upload in ('all', 'as'):
            agu = awapi.AdGroupUpload(config_file='aw_adgroup_upload.xlsx')
            results.extend(agu.upload_all_adgroups(api) or [])
        if args.upload in ('all', 'ad'):
            adu = awapi.AdUpload(config_file='aw_ad_upload.xlsx')
            results.extend(adu.upload_all_ads(api) or [])
    if args.api in ('all', 'szk'):
        api = szkapi.SzkApi(config_file='szkconfig.json')
        if args.upload in ('all', 'c'):
            cu = szkapi.CampaignUpload(config_file='szk_campaign_upload.xlsx')
            cu.upload_all_campaigns(api)
    if args.api in ('all', 'dcm'):
        api = dcapi.DcApi(config_file='dcapi.json')
        if args.upload in ('all', 'c'):
            cu = dcapi.CampaignUpload(config_file='campaign_upload.xlsx')
            results.extend(cu.upload_all_campaigns(api) or [])
        if args.upload in ('all', 'as'):
            pu = dcapi.PlacementUpload(config_file='adset_upload.xlsx')
            results.extend(pu.upload_all_placements(api) or [])
        if args.upload in ('all', 'ad'):
            adu = dcapi.AdUpload(config_file='ad_upload.xlsx')
            results.extend(adu.upload_all_ads(api) or [])
    if args.api in ('all', 'reddit'):
        api = redditapi.RedditApi(config_file='redditconfig.json')
        if args.upload in ('all', 'c'):
            cu = redditapi.CampaignUpload(config_file='campaign_upload.xlsx')
            results.extend(cu.upload_all_campaigns(api) or [])
        if args.upload in ('all', 'as'):
            agu = redditapi.AdGroupUpload(config_file='adset_upload.xlsx')
            results.extend(agu.upload_all_adgroups(api) or [])
        if args.upload in ('all', 'ad'):
            adu = redditapi.AdUpload(config_file='ad_upload.xlsx')
            results.extend(adu.upload_all_ads(api) or [])
    return {'results': results}


if __name__ == '__main__':
    main()
