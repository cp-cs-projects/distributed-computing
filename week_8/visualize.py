import folium

def map_data(res):
    m = folium.Map(location=[20.5937, 78.9629], zoom_start=5)
    cities = {}
    # Add city markers
    for r in res:
        if r[0] not in cities:
            cities[r[0]] = r
        else:
            print(r[0])
            continue
        if r[2] is not None and r[3] is not None:
            folium.Marker(
                location=[r[2], r[3]], 
                popup=f"{r[0]}: {r[1]}°C",
                tooltip=r[0]
            ).add_to(m)

    m.save("map.html")