import pandas as pd
import requests
import json
#from mfs import gcs_utils as gc
import gcs_utils as gc
import io
import os
from tqdm import tqdm
from openai import OpenAI
import io
import tsetools as tt
import time

def get_doccodes():
    doccodes = {350: 'tairyohoyu',
                360: 'teisei_tairyohoyu',
                380: 'henko',
                120: 'yuho',
                160: 'hanki',
                }
    return doccodes

def get_contenttypes():
    docdict = {'1':['application/zip', 'zip'],
                '2':['application/pdf', 'pdf'],
                '5':['application/zip', 'csv.zip']}
    return docdict


class MetaDataProcessor:
    """
    Purpose is to dump edinet data into the cloud.
    E.g. for docode 350, use the below
    filestructure
    edinet/tairyohoyu/YYYYMMDD/meta.csv
    edinet/tairyohoyu/YYYYMMDD/pdf/<docID>.pdf
    edinet/tairyohoyu/YYYYMMDD/zip/<docID>.zip
    edinet/tairyohoyu/YYYYMMDD/csv/<docID>.csv
    """
    def __init__(self, dt:pd.Timestamp, config=None):
        self.date = dt
        self.meta = None
        #example for config is cfg = {'doctypecode':350,
        #                             'bucketname' = 'ppfindata'}
        if config is None:
            config = {}
        self.config = config
        doctypename = get_doccodes()[self.config['doctypecode']]
        dtstr = self.date.strftime('%Y%m%d')
        self.basedir = 'edinet/{doctypename}/{dtstr}'.format(doctypename=doctypename,
                                                             dtstr=dtstr)
        if not 'docdict' in self.config.keys():
            docdict = get_contenttypes()
            self.config['docdict'] = docdict
        #full fn will look like this: 'edinet/{doctypename}/{dtstr}'/{fext}/{docID}.{fullext}'

    
    def read_all_meta_data(self):
        dt = self.date
        url = 'https://api.edinet-fsa.go.jp/api/v2/documents.json?date={dtstr}&type=2&Subscription-Key={skey}'
        rq = requests.get(url.format(dtstr=dt.strftime('%Y-%m-%d'), skey=os.environ['EDINETKEY']))
        jdict = json.loads(rq.content)
        self.meta = pd.json_normalize(jdict['results'])
        return self
    
    def _filter_metadata(self):
        #Historically hanki filings do not have their own docTypeCode but are all flagged only
        #as 120. As flagging them is fairly complex and non-trivial, we outsource all filtering
        #to filterfun, and leave docTypeCode in there only to organize our
        #filings conceptually.
        df = self.meta
        #filt = self.meta['docTypeCode'] == str(self.config['doctypecode'])
        #if 'formCodes' in self.config.keys():
        #    filt &= self.meta['formCode'].astype(str).isin(self.config['formCodes'])
        #self.meta = self.meta.loc[filt]
        if 'filterfun' in self.config.keys():
            self.meta = self.config['filterfun'](self.meta)
        return self

    def _download_all_types_for_docID(self, docID):
        dtstr = self.date.strftime('%Y%m%d')
        print(self.config['docdict'])
        for typenumb, ext in self.config['docdict'].items():
            try:
                url = 'https://api.edinet-fsa.go.jp/api/v2/documents/{docID}?type={typenumb}&Subscription-Key={skey}'
                rq = requests.get(url.format(docID=docID, skey=os.environ['EDINETKEY'], typenumb=typenumb))
                fext = ext[1].split('.')[0]
                fullext = ext[1]
                #full fn will look like this: 'edinet/{doctypename}/{dtstr}'/{fext}/{docID}.{fullext}'
                fname = self.basedir + f'/{fext}/{docID}.{fullext}'
                strm = io.BytesIO(rq.content)
                gc.save_stream_in_cloud(strm, os.environ['BUCKETNAME'], fname, contenttype=ext[0])
            except ConnectionResetError:
                url = 'https://api.edinet-fsa.go.jp/api/v2/documents/{docID}?type={typenumb}&Subscription-Key={skey}'
                rq = requests.get(url.format(docID=docID, skey=os.environ['EDINETKEY'], typenumb=typenumb))
                fext = ext[1].split('.')[0]
                fullext = ext[1]
                #full fn will look like this: 'edinet/{doctypename}/{dtstr}'/{fext}/{docID}.{fullext}'
                fname = self.basedir + f'/{fext}/{docID}.{fullext}'
                strm = io.BytesIO(rq.content)
                gc.save_stream_in_cloud(strm, os.environ['BUCKETNAME'], fname, contenttype=ext[0])
                
    def download_all_data(self):
        """
        """
        #csv file is contained in a directory, and a bit tricky to parse.
        #alas, it is not the responsibility of the downloader and left to the parser
        #fn = 'C:/tmp/jplvh010000-lvh-001_E35450-000_2024-11-12_01_2024-11-19.csv'
        #df = pd.read_csv(fn, encoding="utf-16",sep='\t')

        fn = self.basedir + '/meta.csv'
        try:
            cfn = 'gs://{bucketname}/{fn}'.format(bucketname=os.environ['BUCKETNAME'], fn=fn)
            curmeta = pd.read_csv(cfn+'.gz')
        except FileNotFoundError:
            curmeta = None
        if not curmeta is None:
            curmeta = curmeta.reindex(columns=['docID', 'downloaded'])
            curmeta.loc[:, 'downloaded'] = curmeta['downloaded'].fillna(False)
            imeta = self.meta.merge(curmeta, on='docID', how='left')
            imeta.loc[:, 'downloaded'] = imeta['downloaded'].fillna(False)
        else:
            imeta = self.meta.copy(deep=True)
            imeta.loc[:, 'downloaded'] = False
            gc.save_df_in_cloud(imeta, os.environ['BUCKETNAME'], fname=fn)
        for i, row in tqdm(imeta.iterrows(), total=imeta.shape[0], desc="Processing rows"):
            if row['downloaded'] == False:
                time.sleep(2)
                self._download_all_types_for_docID(row['docID'])
                imeta.loc[imeta['docID'] == row['docID'], 'downloaded'] = True
                gc.save_df_in_cloud(imeta, os.environ['BUCKETNAME'], fname=fn)


class MetaDataProcessorYuhos(MetaDataProcessor):
    """
    Purpose is to dump edinet data into the cloud.
    E.g. for docode 350, use the below
    filestructure
    edinet/tairyohoyu/YYYYMMDD/meta.csv
    edinet/tairyohoyu/YYYYMMDD/pdf/<docID>.pdf
    edinet/tairyohoyu/YYYYMMDD/zip/<docID>.zip
    edinet/tairyohoyu/YYYYMMDD/csv/<docID>.csv
    """
    def __init__(self, dt:pd.Timestamp, config=None):
        self.date = dt
        self.meta = None
        #example for config is cfg = {'doctypecode':350,
        #                             'bucketname' = 'ppfindata'}
        if config is None:
            config = {}
        self.config = config
        #doctypename = get_yuhodoccodes()[self.config['doctypecode']]
        doctypename = get_doccodes()[self.config['doctypecode']]
        dtstr = self.date.strftime('%Y%m%d')
        self.basedir = 'edinet/{doctypename}/{dtstr}'.format(doctypename=doctypename,
                                                             dtstr=dtstr)
        if not 'docdict' in self.config.keys():
            docdict = get_contenttypes()
            self.config['docdict'] = docdict
        #full fn will look like this: 'edinet/{doctypename}/{dtstr}'/{fext}/{docID}.{fullext}'

class MetaDataProcessorHanki(MetaDataProcessor):
    """
    Purpose is to dump edinet data into the cloud.
    E.g. for docode 350, use the below
    filestructure
    edinet/tairyohoyu/YYYYMMDD/meta.csv
    edinet/tairyohoyu/YYYYMMDD/pdf/<docID>.pdf
    edinet/tairyohoyu/YYYYMMDD/zip/<docID>.zip
    edinet/tairyohoyu/YYYYMMDD/csv/<docID>.csv
    """
    def __init__(self, dt:pd.Timestamp, config=None):
        self.date = dt
        self.meta = None
        #example for config is cfg = {'doctypecode':350,
        #                             'bucketname' = 'ppfindata'}
        if config is None:
            config = {}
        self.config = config
        #doctypename = get_hankidoccodes()[self.config['doctypecode']]
        doctypename = get_doccodes()[self.config['doctypecode']]
        dtstr = self.date.strftime('%Y%m%d')
        self.basedir = 'edinet/{doctypename}/{dtstr}'.format(doctypename=doctypename,
                                                             dtstr=dtstr)
        if not 'docdict' in self.config.keys():
            docdict = get_contenttypes()
            self.config['docdict'] = docdict
        #full fn will look like this: 'edinet/{doctypename}/{dtstr}'/{fext}/{docID}.{fullext}'

class ParseLargeHolders:
    def __init__(self, dt:pd.Timestamp):
        self.date = dt
        self.fndf = None
        self.meta = None
        self.df = None
        self.sumrytbl = None
    
    def load_files_and_meta_data(self):
        fdir = 'edinet/tairyohoyu/{dt}'\
               .format(dt=self.date.strftime('%Y%m%d'))
        ldir = gc.listdir(os.environ['BUCKETNAME'], fdir)
        xdf = pd.DataFrame([x.name for x in ldir], columns=['fn'])
        xdf.loc[:, 'date'] = pd.to_datetime(xdf['fn'].str.split('/').str.get(2), errors='coerce')
        filt = xdf['date'] == self.date
        filt &= xdf['fn'].str.contains('csv')
        filt &= ~xdf['fn'].str.contains('meta')
        self.fndf = xdf.loc[filt]
        metafn = xdf.loc[xdf['fn'].str.contains('meta'), 'fn'].iloc[0]
        fn = 'gs://{bucketname}/{fn}'.format(bucketname=os.environ['BUCKETNAME'],
                                             fn=metafn)
        print(fn)
        self.meta = pd.read_csv(fn)
        return self

    def parse_all_csvs(self):
        dfs = []
        for _, row in self.meta.iterrows():
            fn = self.fndf.loc[self.fndf['fn'].str.contains(row['docID']), 'fn'].iloc[0]
            zfil = gc.load_zipfile_from_cloud(os.environ['BUCKETNAME'], fn)
            with zfil.open(zfil.filelist[0]) as zip_ext_file:
                strm = io.BytesIO(zip_ext_file.read())
                df = pd.read_csv(strm, sep='\t', encoding='utf-16')
            df.loc[:, 'docID'] = fn.split('/')[-1].split('.')[0]
            df.loc[:, 'id'] = df['要素ID'].str.split(':').str.get(-1) + '_' + df['要素ID'].str.split(':').str.get(0)
            df.loc[:, 'holdnum'] = df['コンテキストID'].str.split('FilerLargeVolume').str.get(-1)
            df.loc[:, 'holdnum'] = df['holdnum'].str.replace('FilingDateInstant', 'Holder0')
            df.loc[:, 'holdnum'] = df['holdnum'].str.replace('Member', '')
            df.loc[:, 'mcat'] = df['要素ID'].str.split(':').str.get(0)
            df.loc[:, 'value'] = df['値']
            dfs.append(df)
        df = pd.concat(dfs)
        df = df.drop_duplicates()
        import re
        def split_text(text):
            result = re.findall(r'[A-Z][^A-Z]*', text)
            return '_'.join([x.lower() for x in result])
        df.loc[:, 'id'] = [split_text(x.replace('DEI', 'Dei')\
                                       .replace('EDINET', 'Edinet')\
                                       .replace('NA', 'Na')) for x in df['id']]
        #df = df.set_index(['id', 'docID', 'holdnum'])['value'].unstack('id')
        self.df = df
        return self
    
    def rename_fields(self):
        rendict = {
        'security_code_of_issuer_jplvh_cor': 'ticker',
        'name_of_issuer_jplvh_cor': 'name',
        'codedei_jplvh_cor': 'holder_edinetcode',
        'filer_name_in_japanesedei_jpdei_cor': 'holder_jname',
        'filer_name_in_englishdei_jpdei_cor': 'holder_name',
        'residential_address_or_address_of_registered_headquarter_cover_page_jplvh_cor': 'holder_address',
        'description_of_business_jplvh_cor': 'holder_business_desc',
        'name_of_representative_jplvh_cor': 'holder_representative',
        'purpose_of_holding_jplvh_cor': 'holder_reason',
        'act_of_making_important_proposal_etc_jplvh_cor': 'holder_proposal_text',
        'act_of_making_important_proposal_etc_na_jplvh_cor': 'holder_proposal_textna',
        'stocks_or_investment_securities_etc_article27233_item2_jplvh_cor': 'shares_held_total_27233',
        'total_article27233_item2_jplvh_cor': 'shares_held_total',
        'total_number_of_outstanding_stocks_etc_jplvh_cor': 'shares_outstanding',
        'holding_ratio_of_share_certificates_etc_jplvh_cor': 'shares_pct_outstanding',
        'holding_ratio_of_share_certificates_etc_per_last_report_jplvh_cor': 'prev_shares_pct_outstanding',
        'total_amount_of_funding_for_acquisition_jplvh_cor': 'total_amount_funding_jpy',
        'date_when_filing_requirement_arose_cover_page_jplvh_cor': 'calcdate',
        'filing_date_cover_page_jplvh_cor':'filingdate', 
        }
        return rendict

    def prepare_for_parse(self):
        df = self.df.copy(deep=True)
        df = df.drop_duplicates()
        filt = df['holdnum'] == 'Holder0'
        parent = df.loc[filt]
        kids = df.loc[~filt]
        sdfd = parent.loc[parent['mcat'].str.contains('dei_cor')].set_index(['id', 'docID', 'holdnum'])['value'].unstack('id')
        sdfdcols = ['edinet_code_dei_jpdei_cor',
                    'filer_name_in_english_dei_jpdei_cor',
                    'filer_name_in_japanese_dei_jpdei_cor',
                    'security_code_dei_jpdei_cor']
        #sdfd = sdfd.loc[:, sdfdcols]
        self.sdfd = sdfd
        sdfl = parent.loc[parent['mcat'].str.contains('lvh_cor')].set_index(['id', 'docID', 'holdnum'])['value'].unstack('id')
        self.sdfl = sdfl
        kdfd = kids.loc[kids['mcat'].str.contains('dei_cor')].set_index(['id', 'docID', 'holdnum'])['value'].unstack('id')
        assert kdfd.shape[0] == 0
        #kdfl = kids.loc[kids['mcat'].str.contains('lvh_cor')].set_index(['id', 'docID', 'holdnum'])['value'].unstack('id')
        self.kids = kids
        kids.loc[:, 'value'] = kids.loc[:, 'value'].astype(str)
        kdfl = kids.loc[kids['mcat'].str.contains('lvh_cor')].groupby(['id', 'docID', 'holdnum'])['value'].agg(lambda x: ', '.join(x))
        kdfl = kdfl.unstack('id')
        self.kdfl = kdfl
        smrymeta = sdfl.loc[:, ['total_number_of_filers_and_joint_holders_cover_page_jplvh_cor',
                                'holding_ratio_of_share_certificates_etc_jplvh_cor']]
        filt = smrymeta['total_number_of_filers_and_joint_holders_cover_page_jplvh_cor'].astype(int) > 1
        promotedocIDs = smrymeta.loc[filt].reset_index()['docID'].unique().tolist()
        self.promotedocIDs = promotedocIDs
        filt = smrymeta['total_number_of_filers_and_joint_holders_cover_page_jplvh_cor'].astype(int) == 1
        sumrydocIDs = smrymeta.loc[filt].reset_index()['docID'].unique().tolist()
        self.sumrydocIDs = sumrydocIDs
        sumryH1 = kdfl.loc[sumrydocIDs].reset_index().drop('holdnum', axis=1)
        sumryH1.columns = sumryH1.columns.str.replace('_jplvh_cor', '')
        sumryH0 = sdfl.loc[promotedocIDs].reset_index().drop('holdnum', axis=1)
        sumryH0.columns = sumryH0.columns.str.replace('_jplvh_cor', '')
        kcols = ['stocks_or_investment_securities_etc_article27233_item2',
        'total_article27233_item2',
        'total_number_of_outstanding_stocks_etc',
        'holding_ratio_of_share_certificates_etc',
        'holding_ratio_of_share_certificates_etc_per_last_report']
        commoncols = sumryH0.columns[sumryH0.columns.isin(sumryH1.columns)]
        csumry = pd.concat([sumryH0.loc[:, commoncols],
                sumryH1.loc[:, commoncols]])
        csumry = csumry.set_index('docID').loc[:, kcols].reset_index()
        self.csumry = csumry
        return self
    
    def _get_special_summary_df(self, kdfl):
        """
        those columns sit within the subholders, but we want to combine and bring it up to the summary df
        """
        specialkcols = ['purpose_of_holding',
                'act_of_making_important_proposal_etc',
                'act_of_making_important_proposal_etc_na',
                'significant_contracts_related_to_said_stocks_etc_such_as_collateral_agreements_na',
                'significant_contracts_related_to_said_stocks_etc_such_as_collateral_agreements_text_block']
        kdfs = []
        for x in specialkcols:
            tkdfl = kdfl.copy(deep=True)
            tkdfl = kdfl.reset_index('holdnum', drop=True)\
                        .reset_index().drop_duplicates(subset=['docID', x])\
                        .loc[:, ['docID', x]]
            tkdfl = tkdfl.dropna()
            tkdfl.loc[:, x] = tkdfl[x].astype(str)
            kdfs.append(tkdfl.groupby('docID')[x].agg(lambda x: ', '.join(x)))
        amntcols = ['total_amount_from_other_sources',
            'total_amount_of_funding_for_acquisition']
        for x in amntcols:
            tkdfl = kdfl.copy(deep=True)
            tkdfl = kdfl.reset_index('holdnum', drop=True)\
                        .reset_index().drop_duplicates(subset=['docID', x])\
                        .loc[:, ['docID', x]]
            tkdfl.loc[:, x] = pd.to_numeric(tkdfl[x], errors='coerce')
            kdfs.append(tkdfl.groupby('docID')[x].sum())
        kdf = pd.concat(kdfs, axis=1).reset_index()
        return kdf

    def parse_full_summary_table(self):
        sdfdfilercols = ['edinet_code_dei_jpdei_cor', 
                 'filer_name_in_english_dei_jpdei_cor',
                 'filer_name_in_japanese_dei_jpdei_cor',
                 'security_code_dei_jpdei_cor']
        sdflissuercols = ['arrangement_of_filing_cover_page', 'clause_of_stipulation_cover_page',
            'date_when_filing_requirement_arose_cover_page',
            'document_title_cover_page', 'filing_date_cover_page',
            'listed_or_o_t_c', 'name_cover_page', 'name_of_issuer',
            'place_of_filing_cover_page',
            'reason_for_filing_change_report_cover_page',
            'reason_for_filing_change_report_cover_page_na',
            'residential_address_or_address_of_registered_headquarter_cover_page',
            'security_code_of_issuer', 'stock_listing',
            'total_number_of_filers_and_joint_holders_cover_page']
        commoncols = ['docID', 'base_date', 'convertible_bonds_article27233_item2',
            'convertible_bonds_article27233_main_clause',
            'exchangeable_bonds_article27233_item1',
            'exchangeable_bonds_article27233_item2',
            'exchangeable_bonds_article27233_main_clause',
            'holding_ratio_of_share_certificates_etc',
            'holding_ratio_of_share_certificates_etc_per_last_report',
            'notes_holding_ratio_of_share_certificates_etc_text_block',
            'notes_number_of_stocks_etc_held_text_block',
            'number_of_residual_stocks_held',
            'number_of_stocks_etc_to_deduct_as_rights_to_demand_exist_between_joint_holders',
            'number_of_stocks_etc_to_deduct_as_sold_on_margin_trading',
            'stock_depository_receipts_article27233_item1',
            'stock_depository_receipts_article27233_item2',
            'stock_depository_receipts_article27233_main_clause',
            'stock_related_depository_receipts_article27233_item1',
            'stock_related_depository_receipts_article27233_item2',
            'stock_related_depository_receipts_article27233_main_clause',
            'stock_related_trust_beneficiary_rights_article27233_item1',
            'stock_related_trust_beneficiary_rights_article27233_item2',
            'stock_related_trust_beneficiary_rights_article27233_main_clause',
            'stock_trust_beneficiary_rights_article27233_item1',
            'stock_trust_beneficiary_rights_article27233_item2',
            'stock_trust_beneficiary_rights_article27233_main_clause',
            'stocks_or_investment_securities_etc_article27233_item1',
            'stocks_or_investment_securities_etc_article27233_item2',
            'stocks_or_investment_securities_etc_article27233_main_clause',
            'subscription_rights_to_shares_article27233_item2',
            'subscription_rights_to_shares_article27233_main_clause',
            'target_security_covered_warrants_article27233_item1',
            'target_security_covered_warrants_article27233_item2',
            'target_security_covered_warrants_article27233_main_clause',
            'target_security_redeemable_bonds_article27233_item1',
            'target_security_redeemable_bonds_article27233_item2',
            'target_security_redeemable_bonds_article27233_main_clause',
            'total_article27233_item1', 'total_article27233_item2',
            'total_article27233_main_clause',
            'total_number_of_outstanding_stocks_etc',
            'total_number_of_stocks_etc_held']
            #sdfd.loc[:, sdfd]
        sdfd = self.sdfd.copy(deep=True)
        sdfl = self.sdfl.copy(deep=True)
        kdfl = self.kdfl.copy(deep=True)
        pids = self.promotedocIDs
        sumryids = self.sumrydocIDs
        kdfl.columns = kdfl.columns.str.replace('_jplvh_cor', '')
        sumryidskdfl = kdfl.loc[sumryids].reset_index().loc[:, commoncols]
        sdfl = sdfl.copy(deep=True)
        sdfl.columns = sdfl.columns.str.replace('_jplvh_cor', '')
        pssdfl = sdfl.loc[pids].reset_index().loc[:, commoncols]
        pssdfl = pd.concat([sumryidskdfl, pssdfl])
        pssdfl = sdfd.merge(pssdfl, on='docID', how='left')
        pssdfl = pssdfl.merge(sdfl.loc[:, sdflissuercols], on='docID', how='left')
        kdf = self._get_special_summary_df(kdfl)
        pssdfl = pssdfl.merge(kdf, on='docID', how='left')
        self.sumrytbl = pssdfl
        return self
    
    def save_summary(self):
        fn = 'gs://{bucketname}/tairyohoyu/parsed/summary/{YYYYMMDD}.csv.gz'\
             .format(bucketname=os.environ['BUCKETNAME'],
                     YYYYMMDD=self.date.strftime('%Y%m%d'))
        print(fn)
        self.sumrytbl.to_csv(fn, index=False)
        fn = 'gs://{bucketname}/tairyohoyu/parsed/kids/{YYYYMMDD}.csv.gz'\
             .format(bucketname=os.environ['BUCKETNAME'],
                     YYYYMMDD=self.date.strftime('%Y%m%d'))
        print(fn)
        self.kdfl.to_csv(fn)
        return self
        

    def get_summary_table(self):
        kcols = ['docID',
        'edinet_code_dei_jpdei_cor',
        'filer_name_in_japanese_dei_jpdei_cor',
        'filer_name_in_english_dei_jpdei_cor',
        'purpose_of_holding',
        'security_code_of_issuer','name_of_issuer','filing_date_cover_page',
        'date_when_filing_requirement_arose_cover_page','holding_ratio_of_share_certificates_etc',
        'holding_ratio_of_share_certificates_etc_per_last_report',
        'reason_for_filing_change_report_cover_page','document_title_cover_page',
        'reason_for_filing_change_report_cover_page_na','security_code_dei_jpdei_cor',
        'document_type_dei_jpdei_cor','stocks_or_investment_securities_etc_article27233_item2',
        'stocks_or_investment_securities_etc_article27233_main_clause','total_article27233_item2',
        'total_article27233_main_clause','total_number_of_outstanding_stocks_etc','total_number_of_stocks_etc_held',
        'residential_address_or_address_of_registered_headquarter_cover_page','name_cover_page',
        'number_of_submission_dei_jpdei_cor','base_date','place_of_filing_cover_page',
        'total_number_of_filers_and_joint_holders_cover_page',
        'act_of_making_important_proposal_etc',
        'act_of_making_important_proposal_etc_na',
        'significant_contracts_related_to_said_stocks_etc_such_as_collateral_agreements_na',
        'significant_contracts_related_to_said_stocks_etc_such_as_collateral_agreements_text_block',
        'total_amount_from_other_sources',
        'total_amount_of_funding_for_acquisition']
        return self.sumrytbl.loc[:, kcols]

def run_parser_for_date(dt:pd.Timestamp):
    plh = ParseLargeHolders(dt)
    plh = plh.load_files_and_meta_data()
    plh = plh.parse_all_csvs()
    plh = plh.prepare_for_parse()
    plh = plh.parse_full_summary_table()
    plh = plh.save_summary()
    #kdf = plh.get_summary_table()

def parse_for_all_dates():
    fdir = 'edinet/tairyohoyu/'
    ldir = gc.listdir(os.environ['BUCKETNAME'], fdir)
    ldirdf = pd.DataFrame(ldir)
    ldirdf = pd.DataFrame([x.name for x in ldirdf[0]])
    ldirdf.columns = ['fn']
    ldirdf.loc[:, 'date'] = ldirdf['fn'].str.split('/').str.get(2)
    ldirdf.loc[:, 'date'] = pd.to_datetime(ldirdf['date'], errors='coerce')
    ldirdf = ldirdf.loc[ldirdf['date'] < '2020-07-20', ['date']].drop_duplicates()
    ldirdf = ldirdf.sort_values('date', ascending=False)
    for _, row in tqdm(ldirdf.iterrows(), total=ldirdf.shape[0], desc='Processing'):
        try:
            run_parser_for_date(row['date'])
        except (ValueError, IndexError):
            print('value error for date={dtstr}'.format(dtstr=row['date'].strftime('%Y%m%d')))


def run_edinet_downloads_for_date(dt:pd.Timestamp, cfg=None):
    if cfg is None:
        cfg = {'doctypecode':350}
    ed = MetaDataProcessor(dt, config=cfg)
    ed = ed.read_all_meta_data()
    if ed.meta.shape[0] != 0:
        ed = ed._filter_metadata()
        ed = ed.download_all_data()

def run_tairyohoyu_download_for_date(dt:pd.Timestamp):
    cfg = {'doctypecode':350}
    run_edinet_downloads_for_date(dt, cfg)


def filter_by_topix_function_yuho_logic(df):
    """
    keep file if it is either in topix or a REIT
    """
    tpx = tt.load_current_topix_file_from_tse()
    tpx.loc[:, 'secCode'] = tpx['コード'].astype(str) + '0'
    filt = (df['secCode'].astype(str).isin(tpx['secCode']) | df['formCode'].astype(str).isin(['07B000']))
    filt &= (df['docTypeCode'] == '120')
    return df.loc[filt]


def run_yuho_downloads_for_date(dt:pd.Timestamp):
    cfg = {'doctypecode':120,
           'formCodes': ['030000', '07B000'],
           'filterfun': filter_by_topix_function_yuho_logic}
    run_edinet_downloads_for_date(dt, cfg)

#doctype=160 doctype=140 and or formCode = '043000' and docDescription.str.contains('
def filter_by_topix_function_hanki_logic(df):
    """
    keep file if it is either in topix or a REIT
    """
    tpx = tt.load_current_topix_file_from_tse()
    tpx.loc[:, 'secCode'] = tpx['コード'].astype(str) + '0'
    filt = df['secCode'].astype(str).isin(tpx['secCode'])
    filt &= (df['docTypeCode'] == '160') | ((df['docTypeCode'] == '140') & df['docDescription'].str.contains('第2四半期'))
    return df.loc[filt]

def run_hanki_downloads_for_date(dt:pd.Timestamp):
    cfg = {'doctypecode':160,
           'filterfun': filter_by_topix_function_hanki_logic}
    run_edinet_downloads_for_date(dt, cfg)

def run_tairyohoyu_download(arg1=None, arg2=None):
    dt = pd.Timestamp.now().normalize()
    run_tairyohoyu_download_for_date(dt)

def download_and_parse_tairyohoyu(arg1=None, arg2=None):
    dt = pd.Timestamp.now().normalize()
    run_tairyohoyu_download_for_date(dt)
    run_parser_for_date(dt)

def get_relevant_columns_for_summary(sdf):
    selcols = ['docID',
                'edinet_code_dei_jpdei_cor',
                'filer_name_in_english_dei_jpdei_cor',
                'filer_name_in_japanese_dei_jpdei_cor',
                'number_of_submission_dei_jpdei_cor',
                'security_code_dei_jpdei_cor',
                'base_date',
                'holding_ratio_of_share_certificates_etc',
                'holding_ratio_of_share_certificates_etc_per_last_report',
                'total_number_of_outstanding_stocks_etc',
                'total_number_of_stocks_etc_held',
                'date_when_filing_requirement_arose_cover_page',
                'filing_date_cover_page',
                'name_of_issuer',
                'reason_for_filing_change_report_cover_page',
                'residential_address_or_address_of_registered_headquarter_cover_page',
                'security_code_of_issuer',
                'purpose_of_holding',
                'significant_contracts_related_to_said_stocks_etc_such_as_collateral_agreements_text_block',
                'total_amount_of_funding_for_acquisition']
    rencols = ['docid',
                'edinet_code',
                'name',
                'jname',
                'submissions',
                'code',
                'base_date',
                'holding_pct',
                'prev_holding_pct',
                'sharesout',
                'total_shares',
                'effdate',
                'datadate',
                'issuer',
                'reason',
                'address',
                'secCode',
                'purpose',
                'contracts',
                'acquisition_amount_jpy',
                ]
    sdf = sdf.loc[:, selcols]
    sdf.columns = rencols
    return sdf

def screening_for_investment_funds(sdf):
    filteroutnames = ['バークレイズ', '三菱ＵＦＪ']
    #sdf = plh.sumrytbl
    df = get_relevant_columns_for_summary(sdf)
    numcols = ['holding_pct', 'prev_holding_pct']
    for x in numcols:
        df.loc[:, x] = pd.to_numeric(df[x], errors='coerce')
    df.loc[:, 'd_holding_pct'] = df['holding_pct'].fillna(0.0) - df['prev_holding_pct'].fillna(0.0)
    df.loc[:, 'purpose'] = df['purpose'].astype(str)
    invholdfilt = df['purpose'].str.contains('投資')
    invholdfilt &= ~df['purpose'].str.contains('安定保有')
    invholdfilt &= ~df['purpose'].str.contains('経営安定')
    invholdfilt &= ~df['purpose'].str.contains('政策投資')
    #df.loc[df['jname'].str.contains('株式会社三菱'), 'purpose']
    df['jname'].unique()
    #likely better to track funds explicitly
    return df.loc[invholdfilt]

class HankiHolders:
    def __init__(self):
        self.top10 = None
        #contains the raw text
        self.csvdf = None
    
    def read_from_cloud(self, fn = 'edinet/hanki/20241111/csv/S100UN3D.csv.zip'):
        zfil = gc.load_zipfile_from_cloud(os.environ['BUCKETNAME'], fn)
        largestfileidx = pd.Series([x.compress_size for x in zfil.filelist]).idxmax()
        with zfil.open(zfil.filelist[largestfileidx]) as zip_ext_file:
            strm = io.BytesIO(zip_ext_file.read())
            df = pd.read_csv(strm, sep='\t', encoding='utf-16')
        self.csvdf = df
        return self
    
    def _extract_top10holders_text(self, komoku='MajorShareholdersTextBlock'):
        ookabustr = self.csvdf.loc[self.csvdf['要素ID'].str.contains(komoku), '値'].iloc[0]
        return ookabustr
    
    def _parse_text_with_chatgpt(self, komoku='MajorShareholdersTextBlock', dfname='top10', txtstr=None):
        if txtstr is None:
            txtstr = 'Convert the text after this sentence into a csv file format, using | as a separator. '
        #I expect you to set the api key in the enviroment
        client = OpenAI()
        ookabustr = self._extract_top10holders_text(komoku=komoku)
        completion = client.chat.completions.create(
        model="gpt-4o-mini",
        store=True,
        messages=[
            {"role": "user", "content": txtstr + ookabustr}
        ]
        )
        strdata = completion.choices[0].to_dict()['message']['content'].split('```')[1]
        #thing might have some garbage - which we need to get rid of
        strdata = '\n'.join([x for x in strdata.split('\n') if '|' in x])
        self.strdata = strdata
        stream = io.StringIO(strdata)
        df = pd.read_csv(stream, sep='|')
        setattr(self, dfname, df)
        return self

def run_yuho_for_year(arg1=None, arg2=None):
    #name = request.args.get('name', 'World')
    yrstr = arg1.args.get('year', '2024')
    for dt in pd.bdate_range('{year}-06-20'.format(year=yrstr),
                          '{year}-12-31'.format(year=yrstr)):
        run_yuho_downloads_for_date(dt)

def run_hanki_for_year(arg1=None, arg2=None):
    yrstr = '2024'
    for dt in pd.bdate_range('{year}-01-01'.format(year=yrstr),
                             '{year}-10-01'.format(year=yrstr)):
        run_hanki_downloads_for_date(dt)

if __name__ == '__main__':
    run_tairyohoyu_download()