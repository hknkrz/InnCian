import csv
import glob
import math
import os
from geopy.distance import great_circle as GC
import yaml
import pandas as pd

FINAL_CSV_PATH = os.path.normpath(os.path.join(os.getcwd(), 'final_csv.csv'))
FINAL_PATH = os.path.normpath(os.path.join(os.getcwd(), 'final.csv'))
CSV_PATH = os.path.normpath(os.path.join(os.getcwd(), "Csv"))
NORMALIZED_CSV_PATH = os.path.normpath(os.path.join(os.getcwd(), 'normalized_csv.csv'))
TEST_CSV_PATH = os.path.normpath(os.path.join(os.getcwd(), 'test_csv.csv'))
WALK_SPEED = 0.07923076923
TRANSPORT_SPEED = 1.02083333333


def Unite():
    with open(FINAL_CSV_PATH, mode='w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)

        first_line = ['total_area', 'kitchen_area', 'living_area', 'build_date', 'floor', 'floors_number', 'decoration',
                      'rooms_area', 'rooms_count', 'balcony', 'offer_date', 'bedrooms_count', 'longitude', 'latitude',
                      'url',
                      'description', 'cost', 'building_type', 'passenger_elevator', 'cargo_elevator',
                      'material', 'heating', 'parking', 'metro', 'metro_distance', 'metro_transport', 'district',
                      'region']
        writer.writerow(first_line)

        for file_name in os.listdir(CSV_PATH):
            with open(os.path.join(CSV_PATH, file_name, ), 'r', encoding='utf-8') as input_file:
                reader = csv.reader(input_file)
                j = 0
                for row in reader:
                    j = j + 1
                    if j == 1:
                        continue
                    writer.writerow(row)


def GetRowsCount():
    i = 0
    with open(FINAL_CSV_PATH, 'r', encoding='utf-8') as input_file:
        reader = csv.reader(input_file)
        for row in reader:
            i = i + 1

    return i


def NormalizeDataset():
    undergrounds_coord = dict()
    with open("stations.yaml", "r", encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    i = 0
    for element in yaml_data:
        i += 1
        undergrounds_coord[element['name']] = element['coords']

    with open(FINAL_CSV_PATH, mode='w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        first_line = ['total_area', 'kitchen_area', 'living_area', 'build_date', 'floor', 'floors_number', 'decoration',
                      'rooms_area', 'rooms_count', 'balcony', 'offer_date', 'bedrooms_count', 'longitude', 'latitude',
                      'url',
                      'description', 'cost', 'building_type', 'passenger_elevator', 'cargo_elevator',
                      'material', 'heating', 'parking', 'metro', 'metro_distance', 'district',
                      'region', 'is_apartments', 'flat_type', 'is_auction']
        writer.writerow(first_line)
        for file_name in os.listdir(CSV_PATH):
            with open(os.path.join(CSV_PATH, file_name, ), 'r', encoding='utf-8') as input_file:
                reader = csv.reader(input_file)
                i = 0
                for row in reader:
                    index_modifier = 3
                    if len(row) <= 28:
                        index_modifier = 0
                    # else:
                    #    pass

                    i += 1
                    if i == 1:
                        continue
                    distances = []
                    try:
                        for element in zip(row[-(4 + index_modifier)].split(','), row[-(3 + index_modifier)].split(','),
                                           row[-(5 + index_modifier)].split(',')):
                            if not isinstance(element[1], str) or not isinstance(element[0], str):
                                break
                            elif element[1] == 'walk':
                                distances.append([str(WALK_SPEED * 1000 * int(element[0])), element[2]])
                            elif element[1] == 'transport':
                                distances.append([str(TRANSPORT_SPEED * 1000 * int(element[0])), element[2]])
                        if distances == []:
                            row[-(4 + index_modifier)] = None
                        else:
                            distance_value = None
                            for element in sorted(distances, reverse=True, key=lambda x: x[0]):
                                try:
                                    x = (float(row[-(15 + index_modifier)]), float(row[-(16 + index_modifier)]))
                                    y = (undergrounds_coord[element[1]]['lat'], (undergrounds_coord[element[1]]['lon']))
                                    distance_value = math.ceil(GC(x, y).m)
                                    row[-(4 + index_modifier)] = distance_value
                                    row[-(5 + index_modifier)] = element[1]
                                    break
                                except:
                                    continue
                            if distance_value == None:
                                row[-(4 + index_modifier)] = math.ceil(
                                    float(sorted(distances, reverse=True, key=lambda x: x[0])[0][0]))
                                row[-(5 + index_modifier)] = sorted(distances, reverse=True, key=lambda x: x[0])[0][1]


                    except:
                        row[-(4 + index_modifier)] = None
                    row[-(18 + index_modifier)] = '.'.join(row[-(18 + index_modifier)].split('T')[0].split('-')[::-1])
                    row.pop(-(3 + index_modifier))
                    if len(row) <= 28:
                        row.append(None)
                        row.append(None)
                        row.append(None)
                    writer.writerow(row)


def GetRowsSubset(left_border, right_border):
    i = 0
    with open(TEST_CSV_PATH, 'w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        first_line = ['total_area', 'kitchen_area', 'living_area', 'build_date', 'floor', 'floors_number', 'decoration',
                      'rooms_area', 'rooms_count', 'balcony', 'offer_date', 'bedrooms_count', 'longitude', 'latitude',
                      'url',
                      'description', 'cost', 'building_type', 'passenger_elevator', 'cargo_elevator',
                      'material', 'heating', 'parking', 'metro', 'metro_distance', 'district',
                      'region', 'is_apartments', 'flat_type', 'is_auction']
        writer.writerow(first_line)
        with open(FINAL_CSV_PATH, 'r', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            for row in reader:
                i = i + 1
                if i >= left_border and i <= right_border:
                    writer.writerow(row)


def FinalNorm():
    data = pd.read_csv(FINAL_CSV_PATH)
    #data.drop('heating', inplace=True, axis=1)
    #data.drop('rooms_area', inplace=True, axis=1)
    #data.drop('bedrooms_count', inplace=True, axis=1)
    #data.drop('building_type', inplace=True, axis=1)

    data.to_csv(FINAL_PATH,  sep='\t', encoding='utf-8',index=False,header=True)

    print(1)


# NormalizeDataset()
# print(GetRowsCount())

# GetRowsSubset(100000, 100500)
FinalNorm()
