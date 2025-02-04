# week 4 analysis

```python
def canvas_size():

    connection = duckdb.connect()
    
    data_query = """
    SELECT MIN(coordinate), MAX(coordinate)
    FROM '2022_pyarrow.parquet'
    """
    rows = connection.execute(data_query).fetchall()
    print (rows)
  
```

**output: [([0, 0], [1999, 1999])]**

r/place consists of 2000 x 2000 pixel square canvas with 4 million pixels to be painted. Let’s dive into which pixels were painted the most, and the days with the most activity. 

```python
import duckdb

def top_20_pixels():
    connection = duckdb.connect()

    data_query = """
        SELECT coordinate, COUNT(*) AS occurrence_count
        FROM '2022_pyarrow.parquet'
        GROUP BY coordinate
        ORDER BY occurrence_count DESC
        LIMIT 20
    """
    rows = connection.execute(data_query).fetchall()
    
    for i in range(len(rows)):
        print(i+1, rows[i])

def highest_time(coord):
    connection = duckdb.connect()

    data_query = f"""
    SELECT CAST(timestamp AS DATE) AS day, COUNT(*) AS occurrence_count
    FROM '2022_pyarrow.parquet'
    WHERE coordinate = {coord}
    GROUP BY day
    ORDER BY occurrence_count DESC
    LIMIT 10;
    """
    rows = connection.execute(data_query).fetchall()
    return rows
```

**Top Pixels:**

1. **[0, 0]:**  98807 pixels
- 4/1/2022: 9052 pixels, 4/2/2022: 17256 pixels, 4/3/2022: 19599 pixels, **4/4/2022: 37282 pixels**, 4/5/2022: 15618 pixels
1. **[359, 564]:** 69198 pixels
- 4/1/2022: 7073 pixels, 4/2/2022: 21530 pixels, **4/3/2022: 23988 pixels**, 4/4/2022: 16607 pixels
1. **[349, 564]:** 55230 pixels
- 4/1/2022: 5182 pixels, 4/2/2022: 16663 pixels, **4/3/2022: 20045 pixels**, 4/4/2022: 13340 pixels

**Why were these pixels chosen so often?** 

Starting with [0,0], this coordinate points to the top left corner of the canvas. 

Here is the image at the top left corner. It is a loading message in the game RuneScape. 

![Screenshot 2025-02-03 at 6.30.09 PM.png](week%204%20analysis%20190c0c3e446e8049876eef252b4d5be2/Screenshot_2025-02-03_at_6.30.09_PM.png)

I dove into r/2007scape and queried r/place to find some interesting info documenting the creation of this piece:

- In the 2017 r/place, this community created the same loading message in the same spot. **The RuneScape community was likely protective over this area of the canvas in 2022 r/place.**

posts:

![Screenshot 2025-02-03 at 6.38.42 PM.png](week%204%20analysis%20190c0c3e446e8049876eef252b4d5be2/Screenshot_2025-02-03_at_6.38.42_PM.png)

https://www.reddit.com/r/2007scape/comments/63839n/rplace_has_ended_we_are_immortalized/

https://www.reddit.com/r/2007scape/comments/tthbyc/rplace_returns_on_april_1st_are_you_ready_to/

**Why were they protective over this area?** - 

The community likely wanted to maintain a tradition of staying in that spot and showcase a very accurate artwork of the loading message. This is seen through how the most placed pixel color was white (with 59282 white pixels) in order to maintain the white border against others trying to change the color or remove the border by placing black pixels. 

```python
def coordinate_color(coord):

    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    data_query = """
    SELECT pixel_color, COUNT(*) as occurrence_count
    FROM '2022_pyarrow.parquet'
    WHERE coordinate = {coord}
    GROUP BY pixel_color
    ORDER BY occurrence_count DESC
    LIMIT 10;
    """
    rows = connection.execute(data_query).fetchall()
    print(rows)
 
coordinate_color([0,0])
```

**output:** [('#FFFFFF', 59282), ('#000000', 8715), ('#FF4500', 4209), ('#811E9F', 2200), ('#BE0039', 2189), ('#51E9F4', 2105), ('#D4D7D9', 2104), ('#7EED56', 1910), ('#FF99AA', 1692), ('#2450A4', 1567)]

Additionally, `top_20_pixels()` shows that the corners are some of the most popular pixel locations on the canvas, resulting in increased competition from users to take over pixel (0,0).

8th most placed: ([1999, 1999], 31437)
9th most placed: ([1999, 0], 30882)

14th most placed: ([0, 1999], 22763)

---

**Why are the other two pixels hit so often?**

With the help of the [r/place atlas](https://2022.place-atlas.stefanocoding.me/#//370/559/3.345), I found out that [349, 564] and [359, 564] mapped to the eyes of the skull of this onepiece artwork.

![Screenshot 2025-02-03 at 7.55.22 PM.png](week%204%20analysis%20190c0c3e446e8049876eef252b4d5be2/Screenshot_2025-02-03_at_7.55.22_PM.png)

![image.png](week%204%20analysis%20190c0c3e446e8049876eef252b4d5be2/image.png)

The r/onepiece community was very active on the r/place canvas, having the 8th highest pixel count on the entire canvas with 54000 pixels. The community put together many intricate pixel artworks on the canvas. 

It follows that they would also have a competitive spirit and be protective over their artwork. 

**`coordinate_color([349,564])`  output:**

[('#000000', 27804), ('#51E9F4', 19404), ('#FF4500', 2120), ('#FFFFFF', 1512), ('#BE0039', 846), ('#3690EA', 461), ('#FFD635', 398), ('#7EED56', 310), ('#00CCC0', 267), ('#FF99AA', 246)]

 **`coordinate_color([359,564])` output:**

[('#000000', 34726), ('#51E9F4', 26940), ('#FF4500', 1656), ('#FFFFFF', 1391), ('#3690EA', 860), ('#BE0039', 680), ('#FFD635', 357), ('#00CCC0', 343), ('#2450A4', 288), ('#7EED56', 236)]

For both coordinates, the top colors were black and an aquamarine color. **The battle between these two colors suggests another community was targeting this drawing.**

**Why would another community want a teal color for the eyes?**  

General discussion on r/place and r/onepiece revealed users were likely trying to make a meme of the skull by turning it into the undertale character with glowing teal eyes, Sans:

![image.png](week%204%20analysis%20190c0c3e446e8049876eef252b4d5be2/image%201.png)

**Why is the right eye hit more than the left?**

The right eye is often seen glowing, hence why the right coordinate likely had 13968 more hits.

posts

https://www.reddit.com/r/place/comments/tuj5w8/gotta_respect_their_persistence/

https://www.reddit.com/r/Undertale/comments/twk965/after_a_grueling_4_day_battle_between_sans_eyes/