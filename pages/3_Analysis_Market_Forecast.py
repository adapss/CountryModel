import streamlit as st
import matplotlib.pyplot as plt
from PIL import Image

Image.MAX_IMAGE_PIXELS = None

def warning_message():
    warning_message = 'To use the Forecast Analysis a Country Model Needs to be Generated in the Single Study tab'
    st.markdown(
        "<div style='color: #856404; background-color: #fff3cd; padding: 10px; border-radius: 5px; font-size: 30px;'>" + \
        warning_message + \
        "</div>",
        unsafe_allow_html=True
    )
    return
from myCountryModelPackages.Economic_Research import *
if 'base_year' not in st.session_state:
    st.session_state.base_year = None
    warning_message()
    st.stop()

if 'forecast_country_model' not in st.session_state:
    st.session_state.forecast_country_model = None
    warning_message()
    st.stop()
else:
    country_model_forecast = st.session_state['forecast_country_model']

if country_model_forecast.empty:
    warning_message()
    st.stop()

st.title("Country Model Forecast Analysis " )
title_caption = st.session_state['market_report'] + " - "+ str(st.session_state['base_year'])
st.markdown("<h3 style='font-size:24pt;'>" + title_caption + "</h3>", unsafe_allow_html=True)

if 'button' not in st.session_state:
    st.session_state.button = False
def click_button():
    st.session_state.button = not st.session_state.button

country_model_forecast = country_model_forecast.drop(['Segment','Study','BaseYear'],axis=1)
country_list = country_model_forecast['Country'].drop_duplicates()
industry_list = country_model_forecast['Industry'].drop_duplicates()

col1, col2, col3 = st.columns(3)
with col1:
    selected_country = st.multiselect('Select Country', country_list, default=country_list)
    st.session_state['country_choice'] = selected_country
with col2:
    default_industry = industry_list[0]  # or any index you want
    selected_industry = st.multiselect(
        "Choose Industries:",
        industry_list,
        default=industry_list
        )
    #selected_industry = st.multiselect("Choose Industries:", industry_list, index=0)
with col3:
    st.button('Generate Chart', on_click=click_button)

col_vchart, col_table = st.columns([5,1])

if st.session_state.button:
   with st.spinner("Loading data..."):
        if country_model_forecast.empty:
            st.write('A Country Model Needs to be Generated in the Single Study tab')
        else:
            if not selected_industry is None:
                forecast_table_filtered = country_model_forecast.loc[
                    (country_model_forecast['Country'].isin(selected_country)) &  (country_model_forecast['Industry'].isin(selected_industry))]
            else:
                forecast_table_filtered = country_model_forecast.loc[
                    (country_model_forecast['Country'] == selected_country)]
            forecast_table_grouped = forecast_table_filtered.groupby(['Year'])['Forecast'].sum().reset_index()
            with col_vchart:
                fig, ax = plt.subplots(figsize=(10, 6))
                bars = ax.bar(forecast_table_grouped['Year'], forecast_table_grouped['Forecast'], color='skyblue')
                plt.xticks(rotation=45, ha='right', fontsize=8)
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.2f}', ha='center', va='bottom',
                            fontsize=8)
                ax.set_xlabel('Year', fontsize=10)
                ax.set_ylabel('Forecast', fontsize=10)
                ax.set_title('Market Forecast', fontsize=12)
                plt.tight_layout()
                st.pyplot(fig)
            with col_table:
                st.write(forecast_table_filtered)
