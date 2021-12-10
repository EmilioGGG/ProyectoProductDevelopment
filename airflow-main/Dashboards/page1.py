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

    st.title('Información Covid - 19')

    # seleccion de país y que muestre el comportamiento por fecha
    option_list_pais = (confirmed_global['Country/Region'].unique())
    option_side_pais = st.sidebar.selectbox('Seleccione País a Visualizar', option_list_pais)

    """
    ## Incremento de casos diario por país
    """

    # Unificacion de los 3 DF para mostrar en graficas
    # " Cambiar nombre de variables"
    data_complete = pd.merge(confirmed_global, deaths_global, how='left',
                             left_on=['Province/State', 'Country/Region', 'lat', 'lon', 'Covid_Date'],
                             right_on=['Province/State', 'Country/Region', 'lat', 'lon', 'Covid_Date'])
    # data_complete = data_complete.drop('lon',1)

    # esto lo puse porque me jala el año como 20 y no 2020
    recovered_global['Covid_Date'] = pd.to_datetime(recovered_global['Covid_Date'], errors='coerce')
    data_complete['Covid_Date'] = pd.to_datetime(data_complete['Covid_Date'], errors='coerce')

    recovered_global['Covid_Date'] = recovered_global['Covid_Date'].dt.date
    data_complete['Covid_Date'] = data_complete['Covid_Date'].dt.date

    data_complete = pd.merge(data_complete, recovered_global, how='left',
                             left_on=['Province/State', 'Country/Region', 'lat', 'lon', 'Covid_Date'],
                             right_on=['Province/State', 'Country/Region', 'lat', 'lon', 'Covid_Date'])

    data_complete = data_complete.rename(
        columns={'Covid_Cases_x': 'Confirmed', 'Covid_Cases_y': 'deaths', 'Covid_Cases': 'recovered'})

    # Se utiliza para ver comportamiento diario por país seleccionado
    evolution_daily = data_complete[(confirmed_global['Country/Region'] == option_side_pais)]
    # st.write(evolution_daily)
    evolution_daily['confirmed_daily'] = 0
    evolution_daily['deaths_daily'] = 0
    evolution_daily['recovered_daily'] = 0
    evolution_daily = evolution_daily.fillna(0)

    evolution_daily = data_complete[(confirmed_global['Country/Region'] == option_side_pais)]

    evolution_daily = evolution_daily.drop('Country/Region', 1)
    evolution_daily = evolution_daily.drop('Province/State', 1)
    evolution_daily = evolution_daily.drop('lat', 1)
    evolution_daily = evolution_daily.drop('lon', 1)
    evolution_daily = evolution_daily.drop('Covid_Date', 1)
    evolution_daily = evolution_daily.reset_index(drop=True)

    row_d = range(1, evolution_daily.shape[0])

    for i in row_d:
        if i > 0:
            if evolution_daily.loc[i, 'Confirmed'] > 0:
                evolution_daily.loc[i, 'confirmed_daily'] = evolution_daily.loc[i, 'Confirmed'] - evolution_daily.loc[
                    (i - 1), 'Confirmed']

            if evolution_daily.loc[i, 'deaths'] > 0:
                evolution_daily.loc[i, 'deaths_daily'] = evolution_daily.loc[i, 'deaths'] - evolution_daily.loc[
                    (i - 1), 'deaths']

            if evolution_daily.loc[i, 'recovered'] > 0:
                evolution_daily.loc[i, 'recovered_daily'] = evolution_daily.loc[i, 'recovered'] - evolution_daily.loc[
                    (i - 1), 'recovered']

    evolution_daily['Confirmed'] = pd.to_numeric(evolution_daily['Confirmed'], errors='coerce')
    evolution_daily['deaths'] = pd.to_numeric(evolution_daily['deaths'], errors='coerce')
    evolution_daily['recovered'] = pd.to_numeric(evolution_daily['recovered'], errors='coerce')
    evolution_daily['confirmed_daily'] = pd.to_numeric(evolution_daily['confirmed_daily'], errors='coerce')
    evolution_daily['deaths_daily'] = pd.to_numeric(evolution_daily['deaths_daily'], errors='coerce')
    evolution_daily['recovered_daily'] = pd.to_numeric(evolution_daily['recovered_daily'], errors='coerce')

    evolution_dailyv2 = evolution_daily
    evolution_dailyv2 = evolution_dailyv2.drop('Confirmed', 1)
    evolution_dailyv2 = evolution_dailyv2.drop('deaths', 1)
    evolution_dailyv2 = evolution_dailyv2.drop('recovered', 1)

    evolution_dailyv2 = evolution_dailyv2.drop('id', 1)
    evolution_dailyv2 = evolution_dailyv2.drop('id_x', 1)
    evolution_dailyv2 = evolution_dailyv2.drop('id_y', 1)

    evolution_dailyv2 = evolution_dailyv2.reset_index(drop=True)
    evolution_dailyv2 = evolution_dailyv2.fillna(0)

    evolution_daily = evolution_daily.drop('confirmed_daily', 1)
    evolution_daily = evolution_daily.drop('deaths_daily', 1)
    evolution_daily = evolution_daily.drop('recovered_daily', 1)
    evolution_daily = evolution_daily.drop('id', 1)
    evolution_daily = evolution_daily.drop('id_x', 1)
    evolution_daily = evolution_daily.drop('id_y', 1)

    st.write("Incremento por día Casos-Muertes-Recuperados")
    st.line_chart(evolution_daily)
    st.write("Evolución por día Casos-Muertes-Recuperados")
    st.line_chart(evolution_dailyv2)
