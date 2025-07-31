import pandas as pd
import geopandas as gpd
import pydeck as pdk
import numpy as np
from shapely.geometry import MultiPolygon, Polygon

zoneid_map = {0: "Unknown", 1: 'Newark Airport', 2: 'Jamaica Bay', 3: 'Allerton/Pelham Gardens', 4: 'Alphabet City', 5: 'Arden Heights', 6: 'Arrochar/Fort Wadsworth', 7: 'Astoria', 8: 'Astoria Park', 9: 'Auburndale', 10: 'Baisley Park', 11: 'Bath Beach', 12: 'Battery Park', 13: 'Battery Park City', 14: 'Bay Ridge', 15: 'Bay Terrace/Fort Totten', 16: 'Bayside', 17: 'Bedford', 18: 'Bedford Park', 19: 'Bellerose', 20: 'Belmont', 21: 'Bensonhurst East', 22: 'Bensonhurst West', 23: 'Bloomfield/Emerson Hill', 24: 'Bloomingdale', 25: 'Boerum Hill', 26: 'Borough Park', 27: 'Breezy Point/Fort Tilden/Riis Beach', 28: 'Briarwood/Jamaica Hills', 29: 'Brighton Beach', 30: 'Broad Channel', 31: 'Bronx Park', 32: 'Bronxdale', 33: 'Brooklyn Heights', 34: 'Brooklyn Navy Yard', 35: 'Brownsville', 36: 'Bushwick North', 37: 'Bushwick South', 38: 'Cambria Heights', 39: 'Canarsie', 40: 'Carroll Gardens', 41: 'Central Harlem', 42: 'Central Harlem North', 43: 'Central Park', 44: 'Charleston/Tottenville', 45: 'Chinatown', 46: 'City Island', 47: 'Claremont/Bathgate', 48: 'Clinton East', 49: 'Clinton Hill', 50: 'Clinton West', 51: 'Co-Op City', 52: 'Cobble Hill', 53: 'College Point', 54: 'Columbia Street', 55: 'Coney Island', 56: 'Corona', 57: 'Corona', 58: 'Country Club', 59: 'Crotona Park', 60: 'Crotona Park East', 61: 'Crown Heights North', 62: 'Crown Heights South', 63: 'Cypress Hills', 64: 'Douglaston', 65: 'Downtown Brooklyn/MetroTech', 66: 'DUMBO/Vinegar Hill', 67: 'Dyker Heights', 68: 'East Chelsea', 69: 'East Concourse/Concourse Village', 70: 'East Elmhurst', 71: 'East Flatbush/Farragut', 72: 'East Flatbush/Remsen Village', 73: 'East Flushing', 74: 'East Harlem North', 75: 'East Harlem South', 76: 'East New York', 77: 'East New York/Pennsylvania Avenue', 78: 'East Tremont', 79: 'East Village', 80: 'East Williamsburg', 81: 'Eastchester', 82: 'Elmhurst', 83: 'Elmhurst/Maspeth', 84: "Eltingville/Annadale/Prince's Bay", 85: 'Erasmus', 86: 'Far Rockaway', 87: 'Financial District North', 88: 'Financial District South', 89: 'Flatbush/Ditmas Park', 90: 'Flatiron', 91: 'Flatlands', 92: 'Flushing', 93: 'Flushing Meadows-Corona Park', 94: 'Fordham South', 95: 'Forest Hills', 96: 'Forest Park/Highland Park', 97: 'Fort Greene', 98: 'Fresh Meadows', 99: 'Freshkills Park', 100: 'Garment District', 101: 'Glen Oaks', 102: 'Glendale', 103: "Governor's Island/Ellis Island/Liberty Island", 104: "Governor's Island/Ellis Island/Liberty Island", 105: "Governor's Island/Ellis Island/Liberty Island", 106: 'Gowanus', 107: 'Gramercy', 108: 'Gravesend', 109: 'Great Kills', 110: 'Great Kills Park', 111: 'Green-Wood Cemetery', 112: 'Greenpoint', 113: 'Greenwich Village North', 114: 'Greenwich Village South', 115: 'Grymes Hill/Clifton', 116: 'Hamilton Heights', 117: 'Hammels/Arverne', 118: 'Heartland Village/Todt Hill', 119: 'Highbridge', 120: 'Highbridge Park', 121: 'Hillcrest/Pomonok', 122: 'Hollis', 123: 'Homecrest', 124: 'Howard Beach', 125: 'Hudson Sq', 126: 'Hunts Point', 127: 'Inwood', 128: 'Inwood Hill Park', 129: 'Jackson Heights', 130: 'Jamaica', 131: 'Jamaica Estates', 132: 'JFK Airport', 133: 'Kensington', 134: 'Kew Gardens', 135: 'Kew Gardens Hills', 136: 'Kingsbridge Heights', 137: 'Kips Bay', 138: 'LaGuardia Airport', 139: 'Laurelton', 140: 'Lenox Hill East', 141: 'Lenox Hill West', 142: 'Lincoln Square East', 143: 'Lincoln Square West', 144: 'Little Italy/NoLiTa', 145: 'Long Island City/Hunters Point', 146: 'Long Island City/Queens Plaza', 147: 'Longwood', 148: 'Lower East Side', 149: 'Madison', 150: 'Manhattan Beach', 151: 'Manhattan Valley', 152: 'Manhattanville', 153: 'Marble Hill', 154: 'Marine Park/Floyd Bennett Field', 155: 'Marine Park/Mill Basin', 156: 'Mariners Harbor', 157: 'Maspeth', 158: 'Meatpacking/West Village West', 159: 'Melrose South', 160: 'Middle Village', 161: 'Midtown Center', 162: 'Midtown East', 163: 'Midtown North', 164: 'Midtown South', 165: 'Midwood', 166: 'Morningside Heights', 167: 'Morrisania/Melrose', 168: 'Mott Haven/Port Morris', 169: 'Mount Hope', 170: 'Murray Hill', 171: 'Murray Hill-Queens', 172: 'New Dorp/Midland Beach', 173: 'North Corona', 174: 'Norwood', 175: 'Oakland Gardens', 176: 'Oakwood', 177: 'Ocean Hill', 178: 'Ocean Parkway South', 179: 'Old Astoria', 180: 'Ozone Park', 181: 'Park Slope', 182: 'Parkchester', 183: 'Pelham Bay', 184: 'Pelham Bay Park', 185: 'Pelham Parkway', 186: 'Penn Station/Madison Sq West', 187: 'Port Richmond', 188: 'Prospect-Lefferts Gardens', 189: 'Prospect Heights', 190: 'Prospect Park', 191: 'Queens Village', 192: 'Queensboro Hill', 193: 'Queensbridge/Ravenswood', 194: 'Randalls Island', 195: 'Red Hook', 196: 'Rego Park', 197: 'Richmond Hill', 198: 'Ridgewood', 199: 'Rikers Island', 200: 'Riverdale/North Riverdale/Fieldston', 201: 'Rockaway Park', 202: 'Roosevelt Island', 203: 'Rosedale', 204: 'Rossville/Woodrow', 205: 'Saint Albans', 206: 'Saint George/New Brighton', 207: 'Saint Michaels Cemetery/Woodside', 208: 'Schuylerville/Edgewater Park', 209: 'Seaport', 210: 'Sheepshead Bay', 211: 'SoHo', 212: 'Soundview/Bruckner', 213: 'Soundview/Castle Hill', 214: 'South Beach/Dongan Hills', 215: 'South Jamaica', 216: 'South Ozone Park', 217: 'South Williamsburg', 218: 'Springfield Gardens North', 219: 'Springfield Gardens South', 220: 'Spuyten Duyvil/Kingsbridge', 221: 'Stapleton', 222: 'Starrett City', 223: 'Steinway', 224: 'Stuy Town/Peter Cooper Village', 225: 'Stuyvesant Heights', 226: 'Sunnyside', 227: 'Sunset Park East', 228: 'Sunset Park West', 229: 'Sutton Place/Turtle Bay North', 230: 'Times Sq/Theatre District', 231: 'TriBeCa/Civic Center', 232: 'Two Bridges/Seward Park', 233: 'UN/Turtle Bay South', 234: 'Union Sq', 235: 'University Heights/Morris Heights', 236: 'Upper East Side North', 237: 'Upper East Side South', 238: 'Upper West Side North', 239: 'Upper West Side South', 240: 'Van Cortlandt Park', 241: 'Van Cortlandt Village', 242: 'Van Nest/Morris Park', 243: 'Washington Heights North', 244: 'Washington Heights South', 245: 'West Brighton', 246: 'West Chelsea/Hudson Yards', 247: 'West Concourse', 248: 'West Farms/Bronx River', 249: 'West Village', 250: 'Westchester Village/Unionport', 251: 'Westerleigh', 252: 'Whitestone', 253: 'Willets Point', 254: 'Williamsbridge/Olinville', 255: 'Williamsburg (North Side)', 256: 'Williamsburg (South Side)', 257: 'Windsor Terrace', 258: 'Woodhaven', 259: 'Woodlawn/Wakefield', 260: 'Woodside', 261: 'World Trade Center', 262: 'Yorkville East', 263: 'Yorkville West', 264: 'N/A', 265: 'Outside of NYC'}



pd.set_option('display.max_columns', None)

from tqdm import tqdm
from collections import defaultdict
import pandas as pd

def aggregate_hour_chunked(csv_path, target_hour="10:00:00", chunksize=100_000):
    """
    Incrementally aggregates counts per location across all days at a target hour.
    Also computes average movement per day for that hour.
    """
    agg_counts = defaultdict(int)
    days_seen = set()

    # Estimate number of lines (optional for tqdm total)
    total_lines = sum(1 for _ in open(csv_path)) - 1  # minus header

    for chunk in tqdm(pd.read_csv(csv_path, parse_dates=["hour"], chunksize=chunksize),
                      total=total_lines // chunksize + 1,
                      desc=f"Processing {csv_path} for {target_hour}"):
        chunk["hour"] = pd.to_datetime(chunk["hour"], errors="coerce")
        chunk = chunk.dropna(subset=["hour"])  # drop rows where parsing failed
        chunk["hour_of_day"] = chunk["hour"].dt.strftime("%H:%M:%S")
        filtered = chunk[chunk["hour_of_day"] == target_hour]

        # Count unique dates
        days_seen.update(filtered["hour"].dt.date.unique())

        # Aggregate counts
        for _, row in filtered.iterrows():
            loc = row["location_id"]
            cnt = row["count"]
            agg_counts[loc] += cnt

    # Build DataFrame
    result = pd.DataFrame(list(agg_counts.items()), columns=["location_id", "total_count"])
    result["avg_per_day"] = result["total_count"] / len(days_seen) if days_seen else 0
    return result, len(days_seen)


def load_and_process_zones(shapefile_path):
    """Loads NYC taxi zones shapefile and computes centroids for plotting."""
    try:
        zones_gdf = gpd.read_file(shapefile_path).set_crs(epsg=2263)
        zones_gdf = zones_gdf.to_crs(epsg=4326)  # WGS84 for mapping
        zones_gdf["centroid"] = zones_gdf.geometry.centroid
        zones_gdf["lat"] = zones_gdf["centroid"].y
        zones_gdf["lon"] = zones_gdf["centroid"].x
        zones_gdf["LocationID"] = zones_gdf.index
        return zones_gdf[["LocationID", "geometry", "lat", "lon"]]
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        return None


#def create_deck_map(zones_gdf, pickup_df, dropoff_df, hour_to_plot):
def create_deck_map(zones_gdf, pickups_agg_df, dropoffs_agg_df, label="aggregated_10am"):
    """Creates a pydeck map showing net movement for the selected hour."""
    try:
        # Filter to the selected hour
        # pickups = pickup_df[pickup_df["hour"] == hour_to_plot].set_index("location_id")["count"]
        # dropoffs = dropoff_df[dropoff_df["hour"] == hour_to_plot].set_index("location_id")["count"]

        pickups = pickups_agg_df.set_index("location_id")["count"]
        dropoffs = dropoffs_agg_df.set_index("location_id")["count"]

        # Combine into one DataFrame
        zone_stats = pd.DataFrame(index=zones_gdf["LocationID"])
        zone_stats["incoming"] = dropoffs
        zone_stats["outgoing"] = pickups
        zone_stats.fillna(0, inplace=True)

        zone_stats["net_movement"] = zone_stats["incoming"] - zone_stats["outgoing"]
        max_abs = zone_stats["net_movement"].abs().max()
        zone_stats["net_movement_norm"] = zone_stats["net_movement"] / max_abs if max_abs != 0 else 0

        # Merge with geometry
        zone_props = zones_gdf.merge(zone_stats, on="LocationID", how="left").fillna(0)

        # Build visual layer
        zones_for_viz = []
        for _, row in zone_props.iterrows():
            geom = row["geometry"]
            if isinstance(geom, (Polygon, MultiPolygon)):
                coords = list(geom.exterior.coords) if isinstance(geom, Polygon) else list(max(geom.geoms, key=lambda p: p.area).exterior.coords)

                norm = row["net_movement_norm"]
                red = int(255 * max(0, -norm))       # Outflow
                blue = int(255 * max(0, norm))       # Inflow
                green = int(255 * (1 - abs(norm)))   # Neutrality

                zones_for_viz.append({
                    "coordinates": [coords],
                    "zone_id": zoneid_map[row["LocationID"]],
                    "net_movement": round(row["net_movement"], 1),  # ✅ Rounded
                    "fill_color": [red, green, blue, 180]
                })

        polygon_layer = pdk.Layer(
            "PolygonLayer",
            zones_for_viz,
            get_polygon="coordinates",
            get_fill_color="fill_color",
            get_line_color=[255, 255, 255],
            get_line_width=20,
            pickable=True,
            auto_highlight=True
        )

        view_state = pdk.ViewState(
            latitude=40.7589,
            longitude=-73.9851,
            zoom=10,
            pitch=30
        )

        deck = pdk.Deck(
            layers=[polygon_layer],
            initial_view_state=view_state,
            description="""
            <div style="position:absolute; bottom:20px; right:20px; background-color:black; color:white; padding:10px; font-size:12px; border-radius:5px;">
                <b>Legend</b><br/>
                <span style="color:rgb(0,0,255)">Blue</span>: Net Inflow<br/>
                <span style="color:rgb(0,255,0)">Green</span>: Neutral<br/>
                <span style="color:rgb(255,0,0)">Red</span>: Net Outflow
            </div>
            """,
            tooltip={
                "html": "<b>Zone:</b> {zone_id} <br/>"
                        "<b>Net Movement:</b> {net_movement}",
                "style": {
                    "backgroundColor": "black",
                    "color": "white",
                    "fontSize": "12px"
                }
            }
        )

        deck.to_html(f"nyc_net_movement_{label}.html", open_browser=True)
        print(f"Map saved to nyc_net_movement_{label}.html")

    except Exception as e:
        print(f"Error creating map: {e}")
        import traceback
        traceback.print_exc()


def main():
    shapefile_path = "data/taxi_zones.shp"
    pickup_csv = "C:/Users/willi/Downloads/T9/pickup_counts_by_hour.csv"
    dropoff_csv = "C:/Users/willi/Downloads/T9/dropoff_counts_by_hour.csv"
    hour_to_plot = "2022-08-10 10:00:00"  # ⏰ Change to desired hour

    zones_gdf = load_and_process_zones(shapefile_path)
    if zones_gdf is None:
        return

    # Load pickup/dropoff tallies
    # pickup_df = pd.read_csv(pickup_csv, parse_dates=["hour"])
    # dropoff_df = pd.read_csv(dropoff_csv, parse_dates=["hour"])

    # create_deck_map(zones_gdf, pickup_df, dropoff_df, pd.Timestamp(hour_to_plot))

    pickups_agg, num_days = aggregate_hour_chunked(pickup_csv, "10:00:00")
    dropoffs_agg, _ = aggregate_hour_chunked(dropoff_csv, "10:00:00")

    print(f"Aggregated over {num_days} days at 10:00 AM.")

    # Create map using average per day instead of total
    pickups_agg.rename(columns={"avg_per_day": "count"}, inplace=True)
    dropoffs_agg.rename(columns={"avg_per_day": "count"}, inplace=True)

    create_deck_map(zones_gdf, pickups_agg, dropoffs_agg, label="avg_10am")


if __name__ == "__main__":
    main()
