def iterate(file_path):
    cities = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        for row in file:
            city, temp = row.strip().split(";")
            temp = float(temp)

            if city not in cities:
                cities[city] = {'min': temp, 'max': temp, 'sum': temp, 'count': 1}
            else:
                stats = cities[city]
                stats['min'] = min(stats['min'], temp)
                stats['max'] = max(stats['max'], temp)
                stats['sum'] += temp
                stats['count'] += 1
        return cities

def display(cities):
    for city in sorted(cities):
        stats = cities[city]
        avg = int(stats['sum'] / stats['count'])
        print(f"{city}={stats['min']}/{avg}/{stats['max']}")


def main():
    cities = iterate('measurements_1m.txt')
    display(cities)

if __name__ == '__main__':
    main()
