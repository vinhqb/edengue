import geopandas as gpd
import matplotlib.pyplot as plt
import json
from matplotlib.widgets import Slider, RadioButtons
import matplotlib.colors as mcolors
import numpy as np
import calendar

# Function to load and process the GeoJSON file
def load_geojson(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    return geojson_data

# Function to create a color map based on dengue cases with enhanced contrast
def get_color_map(max_cases):
    cmap = plt.get_cmap("YlOrRd")  # 'YlOrRd' provides good contrast
    norm = mcolors.Normalize(vmin=0, vmax=max_cases)
    return cmap, norm

# Function to plot dengue cases over time with options to compare by month, year, and total by month across all years
def plot_dengue_cases(geojson_data):
    # Convert the GeoJSON into a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(geojson_data['features'])

    # Calculate the maximum number of cases in any single year and overall for color mapping
    #yearly_max_cases = max(max(sum(c[0] for c in year_data.values())) for year_data in gdf['total_cases'])
    #monthly_max_cases = max(max(sum(c[0] for c in zip(*[year_data[month][0] for year_data in gdf['total_cases'] if month in year_data]))) for month in range(12))
    yearly_max_cases = 1500
    monthly_max_cases = 500

    cmap, norm_yearly = get_color_map(yearly_max_cases)
    _, norm_monthly = get_color_map(monthly_max_cases)

    # Create a plot with sliders for year and month selection, and radio buttons for selecting view mode
    fig, ax = plt.subplots(figsize=(10, 10))
    plt.subplots_adjust(left=0.1, bottom=0.3)

    # Draw polygons for the features
    gdf.plot(ax=ax, edgecolor='black', facecolor='none')

    # Create sliders for selecting the year and month
    axcolor = 'lightgoldenrodyellow'
    ax_year = plt.axes([0.1, 0.2, 0.65, 0.03], facecolor=axcolor)
    ax_month = plt.axes([0.1, 0.15, 0.65, 0.03], facecolor=axcolor)
    
    years = list(gdf['total_cases'].iloc[0].keys())
    year_slider = Slider(ax_year, 'Year', 0, len(years)-1, valinit=0, valfmt='%0.0f')
    month_slider = Slider(ax_month, 'Month', 1, 12, valinit=1, valfmt='%0.0f')

    # Radio buttons to switch between yearly, monthly, and total by month views
    ax_radio = plt.axes([0.8, 0.15, 0.15, 0.15], facecolor=axcolor)
    radio = RadioButtons(ax_radio, ('Yearly', 'Monthly', 'Total by Month'), active=0)

    # Labels for the sliders
    year_label = plt.text(0.1, 0.25, f"Year: {years[0]}", transform=plt.gcf().transFigure)
    month_label = plt.text(0.1, 0.1, f"Month: {calendar.month_name[1]}", transform=plt.gcf().transFigure)

    # Update function to redraw the dengue cases when the sliders or radio button are changed
    def update(val):
        ax.clear()
        view_mode = radio.value_selected
        
        # Show or hide the year slider based on the selected mode
        if view_mode == 'Yearly':
            year_slider.ax.set_visible(True)
            year_label.set_visible(True)
            month_slider.ax.set_visible(False)
            month_label.set_visible(False)
        elif view_mode == 'Total by Month':
            year_slider.ax.set_visible(False)
            year_label.set_visible(False)
            month_slider.ax.set_visible(True)
            month_label.set_visible(True)
        else:
            year_slider.ax.set_visible(True)
            year_label.set_visible(True)        
            month_slider.ax.set_visible(True)
            month_label.set_visible(True)

        year_index = int(year_slider.val)
        selected_year = years[year_index]
        selected_month = int(month_slider.val) - 1
        month_name = calendar.month_name[selected_month + 1]
        view_mode = radio.value_selected

        # Update slider labels
        year_label.set_text(f"Year: {selected_year}")
        month_label.set_text(f"Month: {month_name}")
        
        gdf.plot(ax=ax, edgecolor='black', facecolor='none')
        
        for _, row in gdf.iterrows():
            polygon = row.geometry
            cases = row['total_cases'].get(selected_year, [[0, 0, 0]] * 12)
            
            if view_mode == 'Yearly':
                total_cases = sum(c[0] for c in cases)
                color = cmap(norm_yearly(total_cases))
            elif view_mode == 'Monthly':
                total_cases = cases[selected_month][0] if selected_month < len(cases) else 0
                color = cmap(norm_yearly(total_cases))
            elif view_mode == 'Total by Month':
                total_cases = sum(year_data[selected_month][0] if selected_month < len(year_data) else 0 for year_data in row['total_cases'].values())
                color = cmap(norm_monthly(total_cases))

            if total_cases > 0:
                centroid = polygon.centroid
                ax.text(centroid.x, centroid.y, str(total_cases), ha='center', va='center', fontsize=8, color='black')
                gdf[gdf.index == _].plot(ax=ax, color=color, edgecolor='black')
        
        ax.set_title(f"Dengue Cases in {month_name} {selected_year} - {view_mode} Mode")
        plt.draw()

    year_slider.on_changed(update)
    month_slider.on_changed(update)
    radio.on_clicked(update)
    
    update(0)  # Initial plot with the first year and month
    plt.show()

# Main function
def main():
    # Load the GeoJSON file
    geojson_file = "ED_MDR_Dengue_Level3_Data_2000_2023_merged.geojson"
    geojson_data = load_geojson(geojson_file)
    
    # Plot dengue cases over time with comparison options
    plot_dengue_cases(geojson_data)

if __name__ == "__main__":
    main()
