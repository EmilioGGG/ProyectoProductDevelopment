import page1
import page2
import streamlit as st
import pandas as pd
import altair as alt
import mysql.connector as mysql

PAGES = {
    "Resumen Por País": page1,
    "Comparativa Mundial": page2
}
st.sidebar.title('Dashboard Evolución Covid 19')
selection = st.sidebar.radio("Opciones", list(PAGES.keys()))
page = PAGES[selection]
page.app()