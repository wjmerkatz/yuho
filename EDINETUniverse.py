import pandas as pd

class EDINETUniverse:
    def __init__(self):
        self.df = None # Universe
        self.edf = None # Edinet list https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?
                        # uji.bean=ee.bean.W1E62071.EEW1E62071Bean&uji.verb=
                        # W1E62071InitDisplay&TID=W1E62071&PID=W0EZ0001&SESSIONKEY=&lgKbn=2&dflg=0&iflg=0
                
    def get_edinet_list(self):
        fn = 'DATA/edinet_list/20210713.csv'
        df = pd.read_csv(fn, encoding='cp932', skiprows=1, dtype=str)
        df = df[df['上場区分']=='上場']
        df.loc[:, 'ticker'] = df['証券コード'].str.slice(0, 4)
        self.edf = df
        return self
    
    def get_topix_universe(self):
        fn = 'https://www.jpx.co.jp/markets/indices/topix/tvdivq00000030ne-att/TOPIX_weight_jp.xlsx'
        tpx = pd.read_excel(fn, engine='openpyxl', dtype=str)
        tpx = tpx.loc[~tpx['コード'].isnull()]
        tpx = tpx.rename(columns={'コード':'ticker'})
        self.df = tpx
        return self
    
    def add_edinet_code(self):
        self.df = self.df.merge(self.edf, on='ticker', how='left')
        return self
    
    def add_features(self):
        self.df.loc[:, 'wgt'] = self.df['TOPIXに占める個別銘柄のウェイト'].astype(float)
        self.df = self.df.sort_values('wgt', ascending=False)
        return self

def test_edinetuniverse():
    euni = EDINETUniverse().get_edinet_list().get_topix_universe().add_edinet_code()\
                           .add_features()
    uni = euni.df
    return uni