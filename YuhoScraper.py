from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException
import pandas as pd
import tqdm
import os
import re


def download_all_yuhos_by_edinetcode(ecode, code=None):
    if code is None:
        code = ecode
    with YuhoScraper() as ys:
        ys.get_search_page_from_edinetcode(ecode)
        ylinks = ys.get_yuho_links()
        linktexts = [ylink.text for ylink in ylinks if ylink.text[0] == '有']
        #skip the Teisei filings
        mdf = []
        for linktext in linktexts:
            try:
                ys.close_all_but_first_window()
                ylink = ys.get_yuho_link_by_text(linktext)
                idf = ys.get_metadata_for_ylink_as_dataframe(ylink)
                ys.click_yuho_link(ylink)
                ys.click_holdings_section()
                fn = ['output', 'kaishajokyo',
                      code + '_' + str(idf['filingdatetime'].dt.date.iloc[0])\
                                    .replace('-', '')+'.html']
                fn = os.path.join(*fn)
                ys.save_content(fn)
                ys.close_all_but_first_window()
                idf.loc[:, 'status'] = 'SUCCESS'
            except:
                idf.loc[:, 'status'] = 'FAILED'
            mdf.append(idf)
    mdf = pd.concat(mdf)
    return mdf
    
class YuhoScraper:

    def __init__(self):
        self.edinetcode = None
        # url below takes edinetcode as input and pull page with last 5 edinet filings
        self.urltemplate = 'https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.verb=W1E63021CXP002002DSPSch&uji.bean=ee.bean.parent.EECommonSearchBean&PID=W1E63021&TID=W1E63021&SESSIONKEY=1626160755834&lgKbn=2&pkbn=0&skbn=1&dskb=&askb=&dflg=0&iflg=0&preId=1&sec={edinetcode}&scc=&shb=&snm=&spf1=1&spf2=1&iec=&icc=&inm=&spf3=1&fdc=&fnm=&spf4=1&spf5=2&otd=120&cal=1&era=R&yer=&mon=&psr=1&pfs=5&row=100&idx=0&str=&kbn=1&flg=&syoruiKanriNo='
        self.until = None
        self.driver = None
        self.startwindow = None
    
    def __enter__(self):
        # Call this only when starting the browser
        options = Options()
        # Chromeのパス（Stableチャネルで--headlessが使えるようになったら不要なはず）
        options.binary_location = ''
        # ヘッドレスモードを有効にする（次の行をコメントアウトすると画面が表示される）。
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-gpu')
        options.add_argument('--allow-insecure-localhost')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--disable-web-security')
        DesiredCapabilities.CHROME.update({'acceptInsecureCerts': True})
        fn = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
        self.driver = webdriver.Chrome(fn, chrome_options=options,
                                       desired_capabilities=DesiredCapabilities.CHROME)
        self.startwindow = self.driver.window_handles[0]
        return self

    def __exit__(self, type, value, traceback):
        self.driver.quit()

    def get_search_page_from_edinetcode(self, edinetcode):
        self.edinetcode = edinetcode
        url = self.urltemplate.format(edinetcode=edinetcode)
        self.driver.get(url)
        self.wait = WebDriverWait(self.driver, 10)
        return self

    def get_metadata_for_ylink_as_dataframe(self, ylink):
        """
        Given a link, it will extract the metadata associated with this link
        """
        def wareki_ascii_to_year(date_str):
            year = int(re.findall(r"(\d+)\.", date_str)[0])
            if 'H' in date_str:
                year = year + 1988
            elif 'R' in date_str:
                year = year + 2018
            date_str = str(year) + '.' + '.'.join(date_str.split('.')[1:])
            return date_str
        dlist = ylink.find_element_by_xpath('../..').text.splitlines()
        colnames = ['filingdatetime', 'name', 'edinetcode', 'issuername', '']
        dlist[0] = wareki_ascii_to_year(dlist[0])
        df = pd.DataFrame(dlist, index=colnames).T
        df.loc[:, 'filingdatetime'] = pd.to_datetime(df['filingdatetime'], errors='coerce')
        return df

    def click_yuho_link(self, ylink):
        """
        click the link and move focus to the newly opened window
        (which is the expected behaviour in most cases if we click a link)
        """
        handles = self.driver.window_handles
        ylink.click()
        self.wait.until(EC.new_window_is_opened(handles))
        self.driver.switch_to.window(self.driver.window_handles[-1])
        return self

    def get_yuho_links(self):
        searchstr = "//A[contains(text(),'有価証券報告書－第')]"
        link_is_there = EC.presence_of_element_located((By.XPATH, searchstr))
        self.wait.until(link_is_there)
        ylinks = self.driver.find_elements_by_xpath(searchstr)
        return ylinks
    
    def get_yuho_link_by_text(self, linktext):
        ylinks = self.get_yuho_links()
        ylink = [x for x in ylinks if x.text == linktext][0]
        return ylink

        
    def click_first_yuho_link(self):
        searchstr = "//A[contains(text(),'有価証券報告書－第')]"
        link_is_there = EC.presence_of_element_located((By.XPATH, searchstr))
        self.wait.until(link_is_there)
        handles = self.driver.window_handles
        self.driver.find_element_by_xpath(searchstr).click()
        self.wait.until(EC.new_window_is_opened(handles))
        self.driver.switch_to.window(self.driver.window_handles[-1])
        return self
    
    def click_holdings_section(self):
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('viewFrame'))
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('menuFrame2'))
        searchstr = "//A[contains(text(),'提出会社の状況')]"
        link_is_there = EC.presence_of_element_located((By.XPATH, searchstr))
        self.wait.until(link_is_there)
        self.driver.find_element_by_xpath(searchstr).click()
        self.driver.switch_to.default_content()
        return self
    
    def save_content(self, fn=None):
        """
        fn is the name of the file you want to save the front_page as.
        it will be something like '_holdings.html'
        or '_hyoushi.html'
        """            
        if fn is None:
            fn = ['output', 'kaishajokyo', self.edinetcode +'.html']
            fn = os.path.join(*fn)
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('viewFrame'))
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('mainFrame'))
        txt = self.driver.page_source
        with open(fn, 'w', encoding='utf-8') as f:
            f.write(self.driver.page_source)
        self.driver.switch_to.default_content()
        return self
    
    def close_all_but_first_window(self):
        otherwindows = [x for x in self.driver.window_handles if x!=self.startwindow]
        for x in otherwindows:
            self.driver.switch_to.window(x)
            self.driver.close()
        self.driver.switch_to.window(self.startwindow)


from playwright.sync_api import sync_playwright
import pandas as pd

def run_for_topix():
    tpx = load_current_topix_file_from_tse()
    df, failedlist = run_sokai_meta_downloads_for_codes(tpx['code'].tolist())
    df.to_csv('c:/tmp/20230528_tpx_sokai_df.csv')
    return failedlist


def load_current_topix_file_from_tse():
    tpxurl = 'https://www.jpx.co.jp/markets/indices/topix/tvdivq00000030ne-att/topixweight_j.csv'
    tpx = pd.read_csv(tpxurl, encoding='shift-jis').dropna(subset=['コード'])
    tpx.loc[:, 'code'] = tpx['コード'].astype(str).str.replace('.0', '', regex=False) 
    return tpx

def get_sokai_meta_df(browser, code:str):
    page = browser.new_page()
    url = 'https://www2.jpx.co.jp/tseHpFront/JJK010010Action.do?Show=Show'
    page.goto(url)
    page.click("input[value='011']")
    page.click("input[value='012']")
    page.click("input[value='013']")
    page.click("input[value='RET']")
    page.locator("input[name='eqMgrCd']").fill(code)
    page.click("input[name='searchButton']")
    page.click("input[value='基本情報']")
    page.click("[href*=\"javascript:changeTab('4')\"]")
    dfs = pd.read_html(page.content())
    df = search_for_sokai(dfs)
    ldf = extract_all_links(page)
    df = df.merge(ldf, on='text', how='left')
    df.loc[:, 'code'] = code
    page.close()
    return df

def run_sokai_meta_downloads_for_codes(codes):
    idfs = []
    failedlist = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for code in tqdm.tqdm(codes):
            try:
                idf = get_sokai_meta_df(browser, code)
                idfs.append(idf)
            except:
                print('We failed on code {code}.'.format(code=code))
                failedlist.append(code)
    return pd.concat(idfs), failedlist

def search_for_sokai(dfs):
    searchstr = '総会'
    for df in dfs:
        for x in df.columns:
            strcheck = (df.loc[:, x].astype(str).str.contains(searchstr) &\
                        (~df.loc[:, x].astype(str).str.contains('予定'))).any()
            if strcheck:
                df.columns = ['date', 'text', 'blank_link', 'xbrl']
                df.loc[:, 'date'] = pd.to_datetime(df['date'], errors='coerce')
                df.loc[:, 'text'] = df.loc[:, 'text'].str.replace('\s+', '', regex=True)
                return df
            
def extract_all_links(page):
    hrefs_of_page = page.eval_on_selector_all("a[href^='/disc/']", "elements => elements.map(element => element.href)")
    texts_of_page = page.eval_on_selector_all("a[href^='/disc/']", "elements => elements.map(element => element.text)")
    ldf = pd.DataFrame([texts_of_page, hrefs_of_page]).T
    ldf.columns = ['text', 'link']
    ldf.loc[:, 'text'] = ldf.loc[:, 'text'].str.replace('\s+', '', regex=True)
    return ldf

class SokaiScraper:

    def __init__(self):
        self.edinetcode = None
        # url below takes edinetcode as input and pull page with last 5 edinet filings
        self.url = 'https://www2.jpx.co.jp/tseHpFront/JJK010010Action.do?Show=Show'
        self.until = None
        self.driver = None
        self.startwindow = None
    
    def __enter__(self):
        # Call this only when starting the browser
        options = Options()
        # Chromeのパス（Stableチャネルで--headlessが使えるようになったら不要なはず）
        options.binary_location = ''
        # ヘッドレスモードを有効にする（次の行をコメントアウトすると画面が表示される）。
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-gpu')
        options.add_argument('--allow-insecure-localhost')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--disable-web-security')
        DesiredCapabilities.CHROME.update({'acceptInsecureCerts': True})
        fn = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
        self.driver = webdriver.Chrome(fn, chrome_options=options,
                                       desired_capabilities=DesiredCapabilities.CHROME)
        self.startwindow = self.driver.window_handles[0]
        return self

    def __exit__(self, type, value, traceback):
        self.driver.quit()

    def enter_meigara_code(self, code):
        self.driver.get(self.url)
        searchstr = "eqMgrCd"
        element_is_there = EC.presence_of_element_located((By.NAME, searchstr))
        self.wait.until(element_is_there)
        textbox = self.driver.find_element(By.NAME, searchstr)
        textbox.send_keys(code)
        return self

        self.wait = WebDriverWait(self.driver, 10)
        return self

    def get_metadata_for_ylink_as_dataframe(self, ylink):
        """
        Given a link, it will extract the metadata associated with this link
        """
        def wareki_ascii_to_year(date_str):
            year = int(re.findall(r"(\d+)\.", date_str)[0])
            if 'H' in date_str:
                year = year + 1988
            elif 'R' in date_str:
                year = year + 2018
            date_str = str(year) + '.' + '.'.join(date_str.split('.')[1:])
            return date_str
        dlist = ylink.find_element_by_xpath('../..').text.splitlines()
        colnames = ['filingdatetime', 'name', 'edinetcode', 'issuername', '']
        dlist[0] = wareki_ascii_to_year(dlist[0])
        df = pd.DataFrame(dlist, index=colnames).T
        df.loc[:, 'filingdatetime'] = pd.to_datetime(df['filingdatetime'], errors='coerce')
        return df

    def click_yuho_link(self, ylink):
        """
        click the link and move focus to the newly opened window
        (which is the expected behaviour in most cases if we click a link)
        """
        handles = self.driver.window_handles
        ylink.click()
        self.wait.until(EC.new_window_is_opened(handles))
        self.driver.switch_to.window(self.driver.window_handles[-1])
        return self

    def get_yuho_links(self):
        searchstr = "//A[contains(text(),'有価証券報告書－第')]"
        link_is_there = EC.presence_of_element_located((By.XPATH, searchstr))
        self.wait.until(link_is_there)
        ylinks = self.driver.find_elements_by_xpath(searchstr)
        return ylinks
    
    def get_yuho_link_by_text(self, linktext):
        ylinks = self.get_yuho_links()
        ylink = [x for x in ylinks if x.text == linktext][0]
        return ylink

        
    def click_first_yuho_link(self):
        searchstr = "//A[contains(text(),'有価証券報告書－第')]"
        link_is_there = EC.presence_of_element_located((By.XPATH, searchstr))
        self.wait.until(link_is_there)
        handles = self.driver.window_handles
        self.driver.find_element_by_xpath(searchstr).click()
        self.wait.until(EC.new_window_is_opened(handles))
        self.driver.switch_to.window(self.driver.window_handles[-1])
        return self
    
    def click_holdings_section(self):
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('viewFrame'))
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('menuFrame2'))
        searchstr = "//A[contains(text(),'提出会社の状況')]"
        link_is_there = EC.presence_of_element_located((By.XPATH, searchstr))
        self.wait.until(link_is_there)
        self.driver.find_element_by_xpath(searchstr).click()
        self.driver.switch_to.default_content()
        return self
    
    def save_content(self, fn=None):
        """
        fn is the name of the file you want to save the front_page as.
        it will be something like '_holdings.html'
        or '_hyoushi.html'
        """            
        if fn is None:
            fn = ['output', 'kaishajokyo', self.edinetcode +'.html']
            fn = os.path.join(*fn)
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('viewFrame'))
        self.wait.until(EC.frame_to_be_available_and_switch_to_it('mainFrame'))
        txt = self.driver.page_source
        with open(fn, 'w', encoding='utf-8') as f:
            f.write(self.driver.page_source)
        self.driver.switch_to.default_content()
        return self
    
    def close_all_but_first_window(self):
        otherwindows = [x for x in self.driver.window_handles if x!=self.startwindow]
        for x in otherwindows:
            self.driver.switch_to.window(x)
            self.driver.close()
        self.driver.switch_to.window(self.startwindow)
