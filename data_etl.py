# import and load necessary package and mosules
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
from datetime import datetime
import tabula
from tabula import read_pdf


URL = "http://sec.gov.ng/statistical-bulletin/"


def scrape_data_links(sec_url=URL):
    """
    Get links where our data is housed
    """
    # scraping to get required links to data of interest
    res = requests.get(sec_url)
    resp = res.text
    soup = BeautifulSoup(resp, "html.parser")
    containerz = soup.find('div', {'class': 'the-contain'})
    child = containerz.find_all('p')
    sec_statiscal_bulletine = []
    for i in child:
        for j in i.find_all('a'):
            if j.has_attr('href'):
                sec_statiscal_bulletine.append(j.attrs['href'])
    for link in sec_statiscal_bulletine:
        print(f"Our link:{link}")
    return sec_statiscal_bulletine


data = scrape_data_links()


# injest and transform selected items of  financial statements
def clean_col(col):
    return col.strip().replace("(N'000)", "N(K)").replace("/", "").replace(" ", "_").replace("(%)", "pct").replace("\n", "").replace("-", "").lower()


def company_financials(fin_data=data[-2]):
    """
    Selected Items from companies Income and Financial position of Quoted Companies
    Quarterly fillings of listed companies Financials with SEC and NSE
    NOTE:

    # 1) QUARTER: Note that since the Financial-year of different companies can vary, it is possible that a company's Q1 is March while it falls into another month for a different company.
    # 2) NUMBER OF MONTHS: This is the number of months for which a quarter's account is presented. It is usually 3-, 6-, 9- and 12-months for Q1, Q2, Q3 and Q4 respectively.
    # In some few cases however, a company may report 3-months account for Q2 or Q3. 3) TURNOVER/REVENUE/ GROSS EARNINGS/ GROSS PREMIUM INCOME: Turnover/Revenue, Gross Earnings and Gross Premium
    # Income are used to capture the Income of Non-financial firms, Banks and Insurance companies respectively. When 'Gross Earnings' is not supplied by a bank,
    # it is computed as: Gross Earnings = Interest Income + Fee and Commission Income + Net Gain/ (Losses) on Financial Instruments + Other Income. In this case,
    # the summation may slightly vary from the actual Gross Earnings when a bank presents some of these components in the net form; e.g. 'Net Interest Income' instead of 'Interest Income'.
    # 4) STOCK PRICES: Month3=3rd and end month of a quarter; Month2=2nd month of a quarter; Month1=1st month of a quarter. For example, if the Q2 of a firm ends in June, then Month1 are Apr,
    # Month2 are May and Month3 are Jun end prices respectively
    """
    df = pd.read_excel(fin_data, sheet_name='D.2', skiprows=[0, 1, 2], dtype='object')

    cleancol_df = df.rename(columns=clean_col)
    # transformws to csv
    cleancol_df.to_csv('./docs/abridge-financials.csv', index=False)


def money_mkt_indicators(mm_data=data[-1]):
    """
    Money market indicator updated monthly by CBN
    """
    # transformed money mkt indicator data
    money_mkt = pd.read_excel(mm_data, sheet_name='E.1', skiprows=[0, 1], dtype='object')
    mm_rep_name = money_mkt.rename(columns={'Unnamed: 0': 'date'})
    clean_col_mm = mm_rep_name.rename(columns=clean_col)
    clean_col_mm.to_csv('./docs/money-mkt-indicators.csv', index=False)


def money_credit_stat(mc_data=data[-1]):
    """
    Money and credit statistics updated monthly by CBN
    """
    # transformed money and credit statistics data
    money_credit = pd.read_excel(mc_data, sheet_name='E.2', skiprows=[
                                 0, 1], usecols=[0, 1, 2, 3, 4, 5, 6], dtype='object')

    m_c_name = money_credit.rename(columns={"Unnamed: 0": "date"})
    mc_clean_col = m_c_name.rename(columns=clean_col)
    mc_clean_col.to_csv('./docs/money-credit-stat.csv', index=False)


def gross_domestic_prod(gdp_data=data[-1]):
    """
    Gross Domestic Product,production updated yearly by NBS
    """
    # transformed the GDP-Yearly data
    gdpby_yearly = pd.read_excel(gdp_data, sheet_name='E.4', skiprows=[0, 1], dtype='object')
    gdpname = gdpby_yearly.rename(columns={"Unnamed: 0": "sectors"})
    gdp_drop_nacol = gdpname.dropna(axis=1)
    gdp_drop_nacol.to_csv('./docs/gdp-yearly.csv', index=False)


def labour_force_stats(labour_data=data[-1]):
    """
    Unemployment and Underemployment watch Updated quarterly by NBS
    """
    # Transform the Labour force-Quarterly  data

    labour_stat = pd.read_excel(labour_data, sheet_name='E.8', skiprows=[0, 1], usecols=[
                                0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype='object')
    labour_clean_col = labour_stat.rename(columns=clean_col)
    labour_clean_col.to_csv('./docs/unemployment-underemployment-watch.csv', index=False)


def crude_oil_production(crude_data=data[-1]):
    """
    Oil Market report Updated monthly by OPEC
    """
    # Transformed crude oil production data
    crude_production = pd.read_excel(crude_data, sheet_name='E.9', skiprows=[
                                     0, 1], usecols=[0, 1, 2, 3], dtype='object')

    crude_chg_name = crude_production.rename(columns={"Unnamed: 0": "date"})
    crude_prod_clean = crude_chg_name.rename(columns=clean_col)
    crude_prod_clean.to_csv('./docs/crude-oil-production.csv', index=False)


def nigeria_top_traders(top_trader_data=data[-1]):
    """
    Nigeria top trade partners updated Quarterly by NBS
    """
    # Transformed top trader partners data
    top_trade_partners = pd.read_excel(top_trader_data, sheet_name='E.10', skiprows=[
                                       0, 1], usecols=[0, 1, 2, 3, 4, 6, 7, 8], dtype='object')
    top_dropna = top_trade_partners.dropna(thresh=5)
    top_partner_clean = top_dropna.rename(columns=clean_col)
    top_partner_clean.to_csv('./docs/top-trade-partners.csv', index=False)


def summary_foreign_trade(sum_trade_data=data[-1]):
    """
    summary od foreign trade data updated monthly by NBS (N'millions)
    """
    # Transformed summarized foreign trade data
    sumary_foreign_trade = pd.read_excel(sum_trade_data, sheet_name='E.11', skiprows=[
                                         0, 1], usecols=[0, 1, 2, 3, 4, 5, 6], dtype='object')
    sum_foreign_clean = sumary_foreign_trade.rename(columns=clean_col)
    sum_foreign_clean.to_csv('./docs/summarized-foreign-trade.csv', index=False)


def external_reserve(ext_res_data=data[-1]):
    """
    Movement in Foreign Reserves-30 day moving average-updated monthly by CBN
    """
    # Transformed foreign reserves data
    ext_reserved = pd.read_excel(ext_res_data, sheet_name='E.14', skiprows=[0, 1], dtype='object')

    ext_res_clean = ext_reserved.rename(columns=clean_col)
    ext_res_clean.to_csv('./docs/foreign-reserves.csv', index=False)


def nse_allshare_index(nse_index_data=data[2]):
    """
    NSE all share index data updated monthly
    """
    # transformed NSE all share index data
    nse_all_share_index = pd.read_excel(
        nse_index_data, sheet_name='B.4', skiprows=[0, 1], dtype='object')
    nse_index_clean = nse_all_share_index.rename(columns=clean_col)
    nse_index_clean.to_csv('./docs/nse-all-share-index.csv', index=False)


def equities_mkt_cap(equity_cap_data=data[2]):
    """
    Nigerian Stock Exchange Market Capitalization-Equities (since 1985)
    """
    # transformed nse equity capitalization data
    mkt_cap_equities = pd.read_excel(equity_cap_data, sheet_name='B.5', skiprows=[0, 1], usecols=[
                                     0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], dtype='object')
    mkt_cap_clean = mkt_cap_equities.rename(columns=clean_col)
    mkt_cap_clean.to_csv('./docs/nse-equities-mkt-cap.csv', index=False)


def nse_foreign_port_movt(foreign_port_data=data[3]):
    """
    Foreign and domestic Transctions on the NSE
    Foreign portfolio investment report
    """
    # Transformed foreign portfolio transaction data
    nse_foreign_portfolio = pd.read_excel(foreign_port_data, sheet_name='C.2', skiprows=[
                                          0, 1], usecols=[0, 1, 2, 3, 4, 5, 6, 7], dtype='object')
    foreign_port_clean = nse_foreign_portfolio.rename(columns=clean_col)
    foreign_port_clean.to_csv('./docs/foreign-portfolio.csv', index=False)


def capital_import_by_invest(capt_imp_data=data[3]):
    """
    Capital importation by type of investment (USD'millions)
    capital importation report
    """
    # transformed capital importation data

    cap_imp_inv = pd.read_excel(capt_imp_data, sheet_name='C.3', skiprows=[0, 1], usecols=[
                                0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype='object')

    cap_imp_col = cap_imp_inv.rename(columns={'Unnamed: 0': 'date'})
    cap_imp_clean = cap_imp_col.rename(columns=clean_col)
    cap_imp_clean.to_csv('./docs/capital-importation-investment.csv', index=False)


def pension_assetby_invest(pen_asset_data=data[3]):
    """
    Pension Fund Asset by Investment classes (N'Billions)
    """
    # trandformed pension Fund by investment data
    pension_asset_invest = pd.read_excel(pen_asset_data, sheet_name='C.5', skiprows=[0, 1], usecols=[
                                         0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23], dtype='object')

    pen_asset_col = pension_asset_invest.rename(columns={'Row Labels': 'date'})
    pen_asset_clean = pen_asset_col.rename(columns=clean_col)
    pen_asset_clean.to_csv('./docs/pension-investments.csv', index=False)


# the control
def main():
    company_financials()
    money_mkt_indicators()
    money_credit_stat()
    gross_domestic_prod()
    labour_force_stats()
    crude_oil_production()
    nigeria_top_traders()
    summary_foreign_trade()
    external_reserve()
    nse_allshare_index()
    equities_mkt_cap()
    nse_foreign_port_movt()
    capital_import_by_invest()
    pension_assetby_invest()
    print(f'Data refreshed!')


if __name__ == "__main__":
    main()
