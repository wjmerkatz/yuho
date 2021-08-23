import os
import pandas as pd

class IndexMaster:
    """
    Parses Index Master files that can be purchased on the JPX webpage http://db-ec.jpx.co.jp/category/C700/.
    As of 2021/06/28, the page only sells 12 months of historical data.
    """
    def __init__(self, fdir=None):
        if fdir is None:
            self.fdir = './DATA/index_master/'
        else:
            self.fdir = fdir
        self.df = None
    
    def parse_files(self):
        def parse_file(fn):
            df = pd.read_csv(fn, encoding='shift-jis',
                             parse_dates=['Date'])
            return df
        fns = os.listdir(self.fdir)
        df = [parse_file(os.path.join(self.fdir, fn)) for fn in fns]
        df = pd.concat(df).reset_index(drop=True)
        self.df = df
        return self
    
    def rename_columns(self):
        """
        convert column names to lower case. Identify data type of columns
        """
        self.df.columns = self.df.columns.str.lower().str.replace(' ', '')
        rendict = {'indexclassification':'indexname'}
        self.df = self.df.rename(columns=rendict)
        return self

    def add_ts_features(self):
        """
        Add features to Index Master:
        'code': standard 4-digit ticker code as a string
        'dffw': dffw = ffw(t) - ffw(t-1)
        'abs_dffw': abs_dffw = |dffw|
        '
        """
        df = self.df
        df.loc[:, 'ticker'] = df['localcode'].astype(str).str.slice(0, 4)
        df = df.sort_values(['date', 'localcode', 'indexname'])
        df.loc[:, 'prev_indexname'] = df.groupby('ticker')['indexname'].shift(1)
        df.loc[:, 'prev_no.ofsharesbeforeffw'] = df.groupby('ticker')['no.ofsharesbeforeffw'].shift(1)
        groupcols = ['ticker', 'indexname']
        df.loc[:, 'prev_ffw'] = df.groupby(groupcols)['ffw'].shift(1)
        df.loc[:, 'dffw'] = df.groupby(groupcols)['ffw'].diff()
        df.loc[:, 'abs_dffw'] = df['dffw'].abs()
        df.loc[:, 'old_shares'] = df['no.ofshares'] - df['changeinno.ofshares']
        df.loc[:, 'old_shares'] = df['no.ofshares'] - df['changeinno.ofshares']
        self.df = df
        return self

    def add_event_flags(self):
        df = self.df
        topix_ffw_changes = (df['indexname'] == 'TOPIX') &\
                            (df['indexname'] == df['prev_indexname']) &\
                            (df['old_shares'] > 0)
        df.loc[topix_ffw_changes, 'eventtype'] = 'topix_ffw_change'
        self.df = df
        return self

    def get_topix_ffw_changes(self):
        df = self.df
        kcols = ['date', 'ticker', 'name', 'changeinno.ofshares', 'no.ofshares',
                 'ffw', 'prev_ffw', 'dffw', 'abs_dffw']
        df = df.query('eventtype == "topix_ffw_change"')\
               .sort_values('abs_dffw', ascending=False).loc[:, kcols]
        return df


