import sys
import csv
import heapq
from time import perf_counter_ns

'''
- analyze (start, end) : most placed color , most placed pixel location
- start and end hrs as CLI arguments, YYYY-MM-DD HH, validate end hour is after start

data example:
['2022-04-04 01:31:15.044 UTC', 'D23Fyt66c0/bremR1gMZYfxiSSH10/jlFKilZjZJVtCPL+okOejfsERw3ccyVgjQhlPJzKoxc05QyJ2jFm1btA==', '#000000', '810,198']
['2022-04-04 01:31:15.728 UTC', 'owEzYWLgsOiIq3WO7iLU8ixeKVRr7OPsrENPaTRB8XZT9r6bDnMbl8ls07xddiI3a3lEq6C3I40qSbZD5GYL3g==', '#000000', '1544,1664']
['2022-04-04 01:31:16.241 UTC', 'yT72l6ID1dUsDPY2igb5X9e2FgpQFdLHjx51twkkkN/+nrb+ZNmSQ/9zS5/M6SQjcw9+QZIYVXfghjH/60f6Yg==', '#000000', '1784,1361']
['2022-04-04 01:31:16.328 UTC', '4U1+gnHu6F6TwVpntpZgnYsb3QSs2H77v+ae34FFiIqGHWIMv8ZZEkaQ3lmla+491WBoI7mdM/bvcdlu4jyQ8Q==', '#000000', '29,137']
['2022-04-04 01:31:17.103 UTC', 'Zb8hND1l9t/YRUe3b6BuX2ZtpTS6yJzLpqI8/i2XYjnYZeVBTyaDUTHgwOXxfOMaslk++OSf9VS+g/NmaC253g==', '#000000', '1985,1674']

'''
def analyze_data(start, end):
    color_freq = {}
    pixel_freq = {}

    # measure execution time
    start_counter = perf_counter_ns()

    with open('2022_place_canvas_history.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        max_color = ['#000000', 0]
        max_pixel = ['0,0', 0]

        for row in csvreader:
            color = row[2]
            pixel = row[3]

            if start <= row[0] <= end:
                # keep up a dict of frequencies of colors
                if color in color_freq:
                    color_freq[color] += 1
                    if color_freq[color] > max_color[1]:
                        max_color = (color, color_freq[color])
                else:
                    color_freq[color] = 1

                # keep up a dict of frequencies of pixels
                if pixel in pixel_freq:
                    pixel_freq[pixel] += 1
                    if pixel_freq[pixel] > max_pixel[1]:
                        max_pixel = (pixel, pixel_freq[pixel])
                else:
                    pixel_freq[pixel] = 1

        end_counter = perf_counter_ns()

        return max_color, max_pixel, end_counter - start_counter
    

'''
    in test_results_week_1.md, doc the results with timeframes
    * selected timeframe
    * millisecs to compute results
    * output
'''

if __name__ == '__main__':
    start = sys.argv[1] + " " + sys.argv[2] # 2022-04-04 01
    end = sys.argv[3] + " " + sys.argv[4]

    if start > end:
        print("End hour must be after start hour")
        sys.exit(1)
    else:
        max_color, max_pixel, time_elapsed = analyze_data(start, end)
        print(f"Time elapsed: {time_elapsed} ns")
        print(f"Most placed color: {max_color}")
        print(f"Most placed pixel: {max_pixel}")
        
    # direct comps work - print('2022-04-04 01:35:54.071 UTC' < '2022-04-04 01:35:55.263 UTC')