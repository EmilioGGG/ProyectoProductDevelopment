import streamlit as st
import pandas as pd
import altair as alt
import mysql.connector as mysql

def app():
    db = mysql.connect(
        host="db",
        user="test",
        passwd="test123",
        database="test"
    )

    CC_Select = "SELECT * FROM time_series_covid19_confirmed_global"
    confirmed_global = pd.read_sql(CC_Select, db)

    CD_Select = "SELECT * FROM time_series_covid19_deaths_global"
    deaths_global = pd.read_sql(CD_Select, db)

    CR_Select = "SELECT * FROM time_series_covid19_recovered_global"
    recovered_global = pd.read_sql(CR_Select, db)

    confirmed_global = confirmed_global.rename(columns={'Lat': 'lat', 'Long': 'lon'})
    deaths_global = deaths_global.rename(
        columns={'Lat': 'lat', 'Long': 'lon'})  # Este me tira error, no le da rename a lon sino long
    recovered_global = recovered_global.rename(columns={'Lat': 'lat', 'Long': 'lon'})


    # ------------------------------------
    # --------Casos de Contagios--------
    # ------------------------------------
    df_country_unique = confirmed_global.groupby(['Country/Region'], sort=False)['Covid_Cases'].max()
    df_country_unique = df_country_unique.reset_index()
    df_country_unique = df_country_unique.sort_values(by=['Covid_Cases'], ascending=False)
    c = alt.Chart(df_country_unique.head(10)).mark_circle().encode(x='Country/Region', y='Covid_Cases',
                                                                   size='Covid_Cases', color='Country/Region')
    st.write('Top 10 países con más contagios')
    st.altair_chart(c, use_container_width=True)

    # ------------------------------------
    # -------_-Casos de Muertes----------
    # ------------------------------------
    df_country_unique1 = deaths_global.groupby(['Country/Region'], sort=False)['Covid_Cases'].max()
    df_country_unique1 = df_country_unique1.reset_index()
    df_country_unique1 = df_country_unique1.sort_values(by=['Covid_Cases'], ascending=False)
    d = alt.Chart(df_country_unique1.head(10)).mark_circle().encode(x='Country/Region', y='Covid_Cases',
                                                                    size='Covid_Cases', color='Country/Region')
    st.write('Top 10 países con más muertes')
    st.altair_chart(d, use_container_width=True)

    # ------------------------------------
    # -------Casos de Recuperados--------
    # ------------------------------------
    df_country_unique2 = recovered_global.groupby(['Country/Region'], sort=False)['Covid_Cases'].max()
    df_country_unique2 = df_country_unique2.reset_index()
    df_country_unique2 = df_country_unique2.sort_values(by=['Covid_Cases'], ascending=False)
    c = alt.Chart(df_country_unique2.head(10)).mark_circle().encode(x='Country/Region', y='Covid_Cases',
                                                                    size='Covid_Cases', color='Country/Region')
    st.write('Top 10 países con más recuperados')
    st.altair_chart(c, use_container_width=True)

    # Información para gráfico de países
    data_mapa = confirmed_global[['lat', 'lon', 'Covid_Cases']]

    data_mapa['lat'] = (pd.to_numeric(data_mapa['lat'], errors='coerce').fillna(0))
    data_mapa['lon'] = (pd.to_numeric(data_mapa['lon'], errors='coerce').fillna(0))

    """
    ## Países con casos Covid-19
    """
    st.map(data_mapa)

    #
