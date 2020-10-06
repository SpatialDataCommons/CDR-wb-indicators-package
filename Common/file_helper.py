import json
import matplotlib.pyplot as plt
import csv
import os
import pandas
from matplotlib.widgets import TextBox


def export_to_csv(file_path,cursor):
    print('export_to_csv: {}'.format(file_path))
    if os.path.exists(file_path):
        os.remove(file_path)

    with open(file_path, "w", newline='') as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONE)
        writer.writerow(col[0] for col in cursor.description)
        for row in cursor:
            writer.writerow(row)
            # print('1')
        print('###finished export.###')
    # print('3')

