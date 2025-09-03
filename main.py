one_million = open('measurements_1m.txt', 'r')

cities = {}

for row in one_million:
    entry = row.strip().split(";")
    city = entry[0]
    temp = float(entry[1])

    if city in cities:
        if cities[city]['min'] > temp:
            max = cities[city]['max']
            cities.update({city : {'min': temp, 'max': max}})

        if cities[city]['max'] < temp:
            min = cities[city]['min']
            cities.update({city : {'min': min, 'max': temp}})

        
    else:
        cities.update({city : {'min': temp, 'max': temp}})

for key, value in cities.items():
    print(key, value)

