import streamlit as st

from myCountryModelPackages.CM_SessionStates import global_session_states_initialize

st.set_page_config(layout="wide")

def display_page_info():
    st.markdown("<h1 style='text-align: center;'>Country Model Generator - V10.4</h1>", unsafe_allow_html=True)
    text_message = "This application is designed to generate a Country Model from a published World Wide market Report.  \
                  Country Model Economic Factors can be modified in several of the tabs on the left "
    st.markdown("<h3 style='font-size:14pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
    st.markdown("""
    <h3 style='font-size:14pt;'>Recent Updates:</h3>
    <ul style='font-size:12pt;'>
        <li>Automation Degree and GDP Country Multiplier Partitioned by Technology Group </li>
        <li>Industry Fractions and GDP Regional Remainders are NOT partitioned, but considered a Common Economic Profile </li>
        <li>Version Management by Technology Group tables or Common Economic Profile Tables   </li>
        <li>Restore Partitions by Version for Technology Groups and/or Common Economic Profile</li>
        <li>Product Description Table with Technology Groups</li>
        <li>Copy All Country Model Parameters from one Year to another.  Primarily for New Year creation, but useful for other situations.</li>
        <li>Copy and Create Technology Groups from one Year to another.  </li>
        <li>Iterative Proportional Fitting (IPF) procedure developed to align Industry & Region with Worldwide Report. (under test)  </li>
    </ul>
    <h3 style='font-size:12pt;'>Please note: Technology Group partitioning is now available for all reports.  </h3>
    """, unsafe_allow_html=True)
    st.markdown("""
    <h3 style='font-size:14pt;'>Working On:</h3>
    <ul style='font-size:12pt;'>
        <li>Waiting on Technology Group Testing before adding more features</li>
    </ul>
    """, unsafe_allow_html=True)

    # st.title("Industry Concentration by Country Factors:")
    #  text_message = " Economic Industry Weights - this allows modification of the Industry weights "
    #  st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
    #  text_message = " Economic Automation Degree - this allows modification the Automation degree  "
    #  st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)
    #
    #  st.title("Country GDP Fraction:")
    #  text_message = \
    #      "This factor allows for adding a multiplier to the GDP for the entire country.  \
    #      This effectively allows you to reduce the GDP of a country if you think the GDP is not representative of the Automation in that country"
    #  st.markdown("<h3 style='font-size:16pt;'>" + text_message + "</h3>", unsafe_allow_html=True)

if 'global_session_states_initialized' not in st.session_state:
    st.session_state.global_session_states_initialized = False

if __name__ == '__main__':
    display_page_info()
    if not st.session_state.global_session_states_initialized:
        with st.spinner("Initializing global variables and validating consistency of the Economic Model Factors ... please do not leave the page till complete."):
            global_session_states_initialize()




