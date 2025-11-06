import streamlit as st
import matplotlib.pyplot as plt

from myCountryModelPackages.Economic_Research import *

def warning_message():
    warning_message = 'To use the Market Share Analysis a Country Model Needs to be Generated in the Single Study tab'
    st.markdown(
        "<div style='color: #856404; background-color: #fff3cd; padding: 10px; border-radius: 5px; font-size: 30px;'>" + \
        warning_message + \
        "</div>",
        unsafe_allow_html=True
    )
    return

if 'base_year' not in st.session_state:
    warning_message()
    st.stop()

if 'share_country_model' not in st.session_state:
    warning_message()
    st.stop()
else:
    country_model_shares = st.session_state['share_country_model']

if country_model_shares is None:
    warning_message()
    st.stop()

st.title("Country Model Market Share Analysis ")
title_caption = str(st.session_state['market_report']) + " - " + str(st.session_state['base_year'])
st.markdown("<h3 style='font-size:24pt;'>" + title_caption + "</h3>", unsafe_allow_html=True)

if 'button' not in st.session_state:
    st.session_state.button = False
def click_button():
    st.session_state.button = not st.session_state.button

country_model_shares  = country_model_shares.drop(['Segment','Study','BaseYear'],axis=1)
company_list = country_model_shares['Company'].drop_duplicates()
region_list = country_model_shares['Region'].drop_duplicates()
industry_list = country_model_shares['Industry'].drop_duplicates()

col1, col2, col3, col4 = st.columns(4)
with col1:
    selected_company = st.multiselect('Select Company', company_list, default=company_list)
    st.session_state['company_choice'] = selected_company
with col2:
    selected_region = st.multiselect('Select Region', region_list,default=region_list)
    st.session_state['region_choice'] = selected_region
with col3:
    default_industry = industry_list[0]  # or any index you want
    selected_industry = st.multiselect(
        "Choose Industries:",
        industry_list,
        default= industry_list #[default_industry]
    )
    # selected_industry = st.multiselect("Choose Industries:", industry_list, index=0)
with col4:
    st.button('Generate Chart', on_click=click_button)

col_chart, col_table = st.columns([3,1])
if st.session_state.button:
   with st.spinner("Loading data..."):
        if country_model_shares.empty:
            st.write('A Country Model Needs to be Generated in the Single Study tab')
        else:
            if not selected_industry is None:
                share_table_filtered = country_model_shares.loc[
                    (country_model_shares['Region'].isin(selected_region)) &  (country_model_shares['Industry'].isin(selected_industry))]
            else:
                share_table_filtered = country_model_shares.loc[
                    (country_model_shares['Region'].isin(selected_region))]
        with col_chart:
            market_shares = share_table_filtered.groupby('Company')['Size'].sum().reset_index()
            market_shares = market_shares.sort_values(by='Size', ascending=False)
            fig, ax = plt.subplots(figsize=(6, 4))
            bars = ax.barh(market_shares['Company'], market_shares['Size'], color='skyblue')
            ax.invert_yaxis()
            for bar in bars:
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height() / 2, f'{width:.2f}', ha='left', va='center',fontsize = 4)
            ax.set_xlabel('Size', fontsize = 4)
            ax.set_title('Market Share by Company', fontsize= 6)
            ax.tick_params(axis='both', which='major', labelsize=4)
            st.pyplot(fig)
        with col_table:
            caption  = "Market Share Table will all Industries Selected"
            st.markdown("<h3 style='font-size:16pt;'>" + caption + "</h3>", unsafe_allow_html=True)
            st.write(share_table_filtered)



