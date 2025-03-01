# Data Cleaning and Exploratory Analysis for Temperature Trends in Urban and Rural India

## Analysis Goal
Examine **temperature trends between urban and rural areas in India**, considering increasing infrastructure and high population density. **hypothesis:** Urban areas experience higher temperatures due to the urban heat island effect, while rural areas may exhibit more stable or lower temperature fluctuations.

---

## Challenges in Data Processing and Formatting  

### Data Parsing / Content Issues  
One of the main challenges with processing this dataset was the sheer size. The data consists of three separate datasets of weather data ordered alphabetically by city name, each approximately 30GB in size (94 GB total!) I had to choose a subset of cities (A-G) to keep the data manageable, so my conclusions are unfortunately not representative of the entire country and some major cities may be excluded. I hope this set still provides a solid sample of the different regions in India. 

 Additionally, parsing separate CSV files for each city was a time-consuming process; I had to use batching and filtering to efficiently construct a parquet file. The original CSV's were labelled with the city name as well, so I needed to add a string column to include the city name in the data set. The dataset also lacks direct population or density indicators, so I will have to do additional research after the initial weather analysis.

### Data Formatting Considerations  
Each city's dataset contained **21 columns**, many of which included redundant or unnecessary data. To streamline the analysis, I researched what each column measureed and narrowed down the columns to eliminate duplicate data, (e.g., "rain" vs. "precipitation") and remove variables that were not directly relevant to temperature trends. Here are the columns I kept:

- **City**
- **Date & Time** – Hourly records from **2010-01-01 to 2024-12-31**, essential for long-term trend analysis.
- **Temperature 2m Above Surface** – Essential for analyzing heat patterns.
- **Humidity 2m Above Surface** – High humidity levels indicate a likelihood of precipitation or fog, while low humidity suggests drier conditions
- **Apparent Temperature** – Perceived temperature by humans
- **Precipitation** – Measures total liquid accumulation, useful for identifying rainfall trends
- **Snowfall**
- **Pressure (Mean Sea Level Pressure)** – Adjusted atmospheric pressure at sea level, a key factor in weather system identification
- **Cloud Cover** - % of sky covered by clouds 
- **Wind Speed 10m Above Surface**
- **Wind Direction 10m Above Surface**

#### Excluded Columns: 
- Cloud Cover (Low, Mid, and High)
- Wind Speed and Direction at 100m
- Wind Gusts at 10m
- Snow Depth
- Surface pressure
- Dew Point

These columns have limited impact on the hypothesis and were excluded to reduce processing/querying complexities.

---
