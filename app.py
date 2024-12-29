from ipyleaflet import Map ,Marker,Popup,Icon,AwesomeIcon,DivIcon,Polyline
from shiny import App, ui,reactive,render
from shinywidgets import output_widget, render_widget  
from pymongo import MongoClient
import pandas as pd
import math
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
import plotly.express as px
from shiny.ui import HTML
import warnings
warnings.filterwarnings('ignore')
#connect to Mongo Db and choosing the right collection 
client = MongoClient("mongodb://localhost:27017/")
db = client["flights_states"]
collection = db["flights_states"]
db2 = client["rl_flight_dashboard"]
db3 = client["arrival_info"]
ICAOS=["LFPG" ,"OMDB" ,"EGLL"]
REALNAMES=["Charles de Gaulle" ,"Dubai" ,"Heathrow"]
choices = {ICAOS[i]:REALNAMES[i] for i in range(len(ICAOS))}
choices.update({"combined":"All aeroports combined","compared":"comparing aeroports"})
app_ui = ui.page_fluid(
    ui.tags.style("""
        /* Hide the marker background */
        .leaflet-div-icon {
            background: none !important;
            border: none !important;
            box-shadow: none !important;
        }
        body {
            background-color: #EAF6FF;
        }
        .card {
            background-color: #F7F9FC;
            border: 1px solid #DCDCDC;
            border-radius: 10px;
        }
        .header {
            color: #003366;
            font-weight: bold;
        }
        .btn {
            background-color: #008080;
            color: #FFFFFF;
            border-radius: 5px;
        }
        .btn:hover {
            background-color: #005f5f;
        }
    """),
        ui.navset_tab(
            ui.nav_panel("Flights",
                        ui.layout_columns(
                            ui.card(
                                    ui.layout_columns(
                                        ui.input_selectize(  
                                            "aero",  
                                            "Select an aeroport or combination",  
                                            choices,  
                                        ),
                                        ui.input_selectize(  
                                            "period",  
                                            "Select the period",  
                                            {"1d":"last day", "7d":"last 7 days","1m":"last month","all":"all available historical data"},  
                                        ),
                                    ),
                                    output_widget("dep_plot")),
                            ui.card(ui.input_selectize(  
                                            "period_arr",  
                                            "Select the period",  
                                            {"7d":"last 7 days","1m":"last month","all":"all available historical data"},  
                                        )
                                ,output_widget("arr_plot"))
                        ),
                        ui.card(
                            ui.div(output_widget("map")),
                            ui.div(
                                ui.layout_columns(
                                    ui.output_data_frame("flights_table"),
                                    ui.row(
                                        ui.div(
                                            ui.input_text("icao","Enter the ICAO of the flight to visualize",value=""),
                                            style = "width :100%;"
                                        ),
                                        ui.div(
                                            ui.input_action_button("visualize","Visualize the flight in map"),
                                            style = "width :100%;"
                                        ),
                                        ui.div(
                                            ui.input_action_button("initialize","Visualize a sample of flights"),
                                            style = "width :100%; margin-top :15px;"
                                        ),
                                        ui.div(
                                            ui.p("⚠️ The displayed aircraft are 1,000 chosen randomly to avoid overloading the website."),
                                            ui.p("ℹ️ Search for a flight in the dataFrame and visualize its route."),
                                            style = "width :100%; margin-top :15px;"
                                        )
                                    ),
                                    col_widths=[8, 4],
                                )
                                )
                            ),
                        ),
            ui.nav_panel("Meteo",
                        #ui components for meteo data here
                        ),
            ui.nav_panel("Social Media",
                         #ui components for social media data here),
                        )
    )  
    )

def server(input, output, session):
    
    data  = reactive.Value(pd.DataFrame())
    flight_to_visu = reactive.Value(None)
    data_dep = reactive.Value(pd.DataFrame())
    data_arr = reactive.Value(pd.DataFrame())
    def poll_func3():
        #get the number of docs to detect changes
        total_documents = collection.count_documents({})
        return total_documents

    #this function executes each 24h
    @reactive.effect
    @reactive.poll(poll_func3,86400)
    def fetch_data_arr() -> pd.DataFrame:
        data_arr_df = pd.DataFrame()
        data_arr_d = []
        for icao in ICAOS : 
            collection_arr = db3[icao]
            cursor = collection_arr.find({}) 
            for doc in cursor:
                doc["aero"] = icao
                data_arr_d.append(doc)
        data_arr_df = pd.DataFrame(data_arr_d)
        data_arr_df = data_arr_df.drop("_id",axis=1)
        data_arr.set(data_arr_df)

    def poll_func2():
        #get the number of docs to detect changes
        total_documents = collection.count_documents({})
        return total_documents

    #this function executes each 24h
    @reactive.effect
    @reactive.poll(poll_func2,86400)
    def fetch_data_dep() -> pd.DataFrame:
        
        data_dep_df = pd.DataFrame()
        for icao in ICAOS : 
            collection_dep = db2[icao]
            cursor = collection_dep.find({}) 
            for doc in cursor:
                date = doc["date"] 
                inter_df = pd.DataFrame(doc["flights"])
                inter_df["date"] = date
                # Convert the columns to datetime format
                inter_df['hour'] = pd.to_datetime(inter_df['hour'], format='%H:%M')
                inter_df['firstSeen'] = pd.to_datetime(inter_df['firstSeen'], format='%H:%M')

                # Calculate the difference and convert it to minutes
                inter_df['delay'] = (inter_df['firstSeen'] - inter_df['hour']).dt.total_seconds() / 60
                inter_df[inter_df['delay']<0] = 0
                data_dep_df = pd.concat([data_dep_df,inter_df],ignore_index=True)
                inter_df.loc[inter_df["delay"]<0,"delay"] = 0
        data_dep.set(data_dep_df)

    def poll_func():
        # Retur n the latest capture_time or _id to detect changes
        first_document = collection.find_one()
        return first_document["capture_time"][-1] if first_document else None


    @reactive.effect
    @reactive.poll(poll_func,300)
    def fetch_data() -> pd.DataFrame:
        # Query to find documents where the last element of 'lon' and 'lat' arrays is not null
        query = {
            "$expr": {
                "$and": [
                    {"$ne": [{"$arrayElemAt": ["$lon", -1]}, None]},
                    {"$ne": [{"$arrayElemAt": ["$lat", -1]}, None]}
                ]
            }
        }

        results = list(collection.find(query)) 
        results = pd.DataFrame(results)
        results["callsign"] = results["callsign"].apply(str.strip)
        data.set(results)


    # Table interactive
    @output
    @render.data_frame
    def flights_table():
        df = data()
        df = df[["icao24","callsign","capture_time","velocity","lon","lat","geo_alt","true_track"]]
        #get only last info
        df["capture_time"] = df["capture_time"].apply(lambda x : x[-1])
        df["velocity"] = df["velocity"].apply(lambda x : x[-1])
        df["lon"] = df["lon"].apply(lambda x : x[-1])
        df["lat"] = df["lat"].apply(lambda x : x[-1])
        df["geo_alt"] = df["geo_alt"].apply(lambda x : x[-1])
        df["true_track"] = df["true_track"].apply(lambda x : x[-1])
        df["capture_time"] = pd.to_datetime(df['capture_time'], unit='s')
        return df
    
    @reactive.effect 
    @reactive.event(input.visualize)
    def one_flight_map():
        if input.icao() is not None :
            df = data()
            flight_to_visu.set(df[df["icao24"] == input.icao()].squeeze())

    @reactive.effect 
    @reactive.event(input.initialize)
    def initialize_map():
        flight_to_visu.set(None)

         
    @render_widget  
    def map():
        df = data()
        # Function to calculate the angle
        def calculate_angle(lat_t, lon_t, lat_t1, lon_t1):
            delta_phi = lat_t - lat_t1
            delta_lambda = lon_t - lon_t1
            angle = math.atan2(delta_lambda, delta_phi)  
            return math.degrees(angle) 

        map = Map(center=(50.6252978589571, 0.34580993652344), zoom=3)          
        
        #drawing one flight
        if flight_to_visu() is not None :
            flight_info =flight_to_visu()
            points = [(flight_info["lat"][i],flight_info["lon"][i])  for i in range(len(flight_info["lat"])) if (flight_info["lat"][i] is not None and flight_info["lon"][i]is not None)]
            map.add_layer(Polyline(locations=points,color="blue",fill=False,weight=4,opacity=0.7))
            point = flight_info
            if point["lat"][-1] and point["lon"][-1]:  # Ensure valid coordinates
                #get the first position not null except last:
                previous_lats = point["lat"][:-1][::-1]
                previous_lons = point["lon"][:-1][::-1]
                prev_lat = None
                prev_lon = None

                for i in range(len(previous_lats)):
                    if (previous_lats[i] is not None) and (previous_lons[i] is not None):
                        prev_lat = previous_lats[i] 
                        prev_lon = previous_lons[i]
                        break
                if prev_lat is None or prev_lon is None :
                    angle = 0
                else:
                    angle = calculate_angle(point["lat"][-1], point["lon"][-1],prev_lat,prev_lon)
                
                icon = f"""
                        <div class="fa fa-plane" aria-hidden="true" style="transform: rotate({angle-45}deg); transform-origin: center;"></div> 
                    """
                point = Marker(location=(point["lat"][-1], point["lon"][-1]), draggable=False,
                        icon = DivIcon(html = icon) 
                        )
                map.add_layer(point)
        else:
            #random 1000 flights 
            random_numbers = np.random.randint(0, len(df), size=1000)
            for i,point in df.iloc[random_numbers].iterrows():
                if point["lat"][-1] and point["lon"][-1]:  # Ensure valid coordinates
                    #get the first position not null except last:
                    previous_lats = point["lat"][:-1][::-1]
                    previous_lons = point["lon"][:-1][::-1]
                    prev_lat = None
                    prev_lon = None
#  
                    for i in range(len(previous_lats)):
                        if (previous_lats[i] is not None) and (previous_lons[i] is not None):
                            prev_lat = previous_lats[i] 
                            prev_lon = previous_lons[i]
                            break
                    if prev_lat is None or prev_lon is None :
                        angle = 0
                    else:
                        angle = calculate_angle(point["lat"][-1], point["lon"][-1],prev_lat,prev_lon)
                    
                    icon = f"""
                            <div class="fa fa-plane" aria-hidden="true" style="transform: rotate({angle-45}deg); transform-origin: center;"></div> 
                        """
                    point = Marker(location=(point["lat"][-1], point["lon"][-1]), draggable=False,
                            icon = DivIcon(html = icon) 
                            )
                    map.add_layer(point)
        return map 

    @render_widget
    def dep_plot():
        df = data_dep()
        df =  df.dropna(subset=["estDepartureAirport"])
        df = df[df["estDepartureAirport"]!=0]
        start_period = None
        match input.period():
            case "1d":
                day_ago = datetime.now() - timedelta(days = 1) 
                start_period = day_ago.strftime("%Y-%m-%d")
            case "7d":
                ago = datetime.now() - timedelta(days = 7) 
                start_period = ago.strftime("%Y-%m-%d")
            case "1m":
                ago = datetime.now() - relativedelta(months=1)
                start_period = ago.strftime("%Y-%m-%d") 
            case "all":
                ago = datetime(1900,1,1) 
                start_period = ago.strftime("%Y-%m-%d") 
        
        df = df[df["date"]>=start_period]
        match input.aero():
            case x if x in ICAOS:
                fig = px.histogram(
                       df[df["estDepartureAirport"] == input.aero()] , x="delay", nbins=50, title=f"Histogram of delay"
                    )
            case "combined" :
                fig = px.histogram(
                       df , x="delay", nbins=50, title="Histogram of delay"
                    )
            case "compared":
                bins = [0, 10, 20, 30, 40,50, 60, float('inf')]
                labels = ['0-10','10-20', '20-30', '30-40','40-50','50-60','60+']
                df['delay_cat'] = pd.cut(df['delay'], bins=bins, labels=labels, right=False)
                df = df[['delay_cat',"delay","estDepartureAirport"]]
                df = df.groupby(["delay_cat","estDepartureAirport"])['delay'].count()
                df = df.reset_index()
                # Créer un bar plot
                fig = px.bar(df, x='delay_cat', y='delay', color='estDepartureAirport',
                             labels={'delay': 'Number of flight', 'delay_cat': 'Delay category'})
        return fig
    @render_widget
    def arr_plot():
        df = data_arr()
        start_period = None
        match input.period_arr():
            case "1d":
                day_ago = datetime.now() - timedelta(days = 1) 
                start_period = day_ago.strftime("%Y-%m-%d")
            case "7d":
                ago = datetime.now() - timedelta(days= 7) 
                start_period = ago.strftime("%Y-%m-%d")
            case "1m":
                ago = datetime.now() - relativedelta(months=1) 
                start_period = ago.strftime("%Y-%m-%d") 
            case "all":
                ago = datetime(1900,1,1) 
                start_period = ago.strftime("%Y-%m-%d") 
        
        df = df[df["date"]>=start_period]
        df = df.sort_values(by='date')
        fig = px.line(
            df,
            x='date',
            y='flights',
            color='aero',
            title='Daily number of arrivals'
        )
        return fig
        ## Server functions for meteo here
        #
        #
        #
        #
        ##
        ## Server functions for socail media here
        #
        #
        #
        #
        ##
app = App(app_ui, server)