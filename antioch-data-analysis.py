import csv
from cycler import cycler
import dataset
from datetime import datetime
from dateutil import tz
from io import BytesIO
import matplotlib.pyplot as plt
import numpy as np
import os
import pycurl

# Global Variables (because I am lazy and have no face)
dataDirectory = "C:\\project_files\\ADA_Data"
db = dataset.connect('sqlite:///:memory:')
cc = (cycler(color=['tab:blue',
                    'tab:orange',
                    'tab:green',
                    'tab:red',
                    'tab:purple',
                    'tab:brown',
                    'tab:pink',
                    'tab:gray',
                    'tab:olive',
                    'tab:cyan']) +
      cycler(linestyle=[(0, (1, 1)),
                        (0, (5, 5)),
                        (0, (3, 5, 1, 5)),
                        (0, (3, 5, 1, 5, 1, 5)),
                        (0, (5, 1)),
                        (0, (3, 1, 1, 1)),
                        (0, (3, 1, 1, 1, 1, 1)),
                        (0, (1, 10)),
                        (0, (5, 10)),
                        (0, (3, 10, 1, 10))]))


def Load_Database():
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    table = db['data']

    for root, subdirs, files in os.walk(dataDirectory):
        for filename in files:
            if filename.endswith(".csv"):
                with open(os.path.join(root, filename), 'r') as f:
                    firstRow = True
                    csvReader = csv.reader(f, delimiter=',', quotechar='"')
                    for row in csvReader:
                        if firstRow:
                            firstRow = False
                        else:
                            table.insert(dict(timestamp=row[0],
                                              localDatestamp=datetime.strptime(str(row[0])[0:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=from_zone).astimezone(to_zone).date(),
                                              ipAddress=row[1],
                                              city=row[2],
                                              state=row[3],
                                              country=row[4],
                                              latitude=row[5],
                                              longitude=row[6],
                                              watchTimeMinutes=row[7],
                                              resolution=row[8],
                                              userAgent=row[9],
                                              service=filename[:-4],
                                              weekNumber=
                                              datetime.strptime(str(row[0])[0:19], '%Y-%m-%dT%H:%M:%S').replace(
                                                  tzinfo=from_zone).astimezone(to_zone).isocalendar()[1],
                                              weekday=datetime.strptime(str(row[0])[0:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=from_zone).astimezone(to_zone).weekday()))


def Is_VPN():
    #buffer = BytesIO()
    #c = pycurl.Curl()
    #c.setopt(c.URL, 'http://ipinfo.io/43.241.71.120/privacy?token=$TOKEN')
    #c.setopt(c.WRITEDATA, buffer)
    #c.perform()
    #c.close()

    #body = buffer.getvalue()
    # Body is a byte string.
    # We have to know the encoding in order to print it to a text file
    # such as standard output.
    #print(body.decode('iso-8859-1'))
    exit()


def CountByCountry():
    # Get count by country
    result = db.query("SELECT COUNT(ipAddress), country from data GROUP BY country ORDER BY country")
    print("Country Breakdown:")
    for row in result:
        if row["country"] == "":
            print(f'  <unspecified>: {row["COUNT(ipAddress)"]}')
        else:
            print(f'  {row["country"]}: {row["COUNT(ipAddress)"]}')
    print()


def CountByState():
    # Get count by state
    print("State Breakdown:")
    result = db.query("SELECT COUNT(ipAddress), state, country from data GROUP BY state ORDER BY state")
    for row in result:
        if row["state"] == "":
            print(f'  <unspecified>, {row["country"]}: {row["COUNT(ipAddress)"]}')
        else:
            print(f'  {row["state"]}, {row["country"]}: {row["COUNT(ipAddress)"]}')
    print()


def CountByDateAndService():
    # Count by date and service
    print("Count by Date and Service:")
    datestamps = GetDatestamps()
    plt.xticks(list(range(len(datestamps))), datestamps, rotation='vertical')
    print(datestamps)
    services = db.query("SELECT service FROM data GROUP BY service ORDER BY service")
    for service in services:
        datapoints = {}
        datapoints['x'] = []
        datapoints['y'] = []
        for datestamp in datestamps:
            # Want to remove out any Sunday-morning viewers that accidentally clicked the Saturday service
            if "Sat" in service["service"] and datetime.strptime(datestamp, '%Y-%m-%d').weekday() == 6:
                continue
            datapoints['x'].append(datestamps.index(datestamp))
            result = db.query(f'SELECT COUNT(*) FROM data WHERE localDatestamp=\'{datestamp}\' AND service=\'{service["service"]}\' AND weekday>=5 GROUP BY localDatestamp')
            hadRows = False
            for row in result:
                hadRows = True
                datapoints['y'].append(row["COUNT(*)"])
            if not hadRows:
                datapoints['x'].pop(-1)
        print(service["service"] + " " + str(datapoints))
        QuickPlotWithTrendline(datapoints['x'], datapoints['y'], service["service"], datestamps)
        for x, y in zip(datapoints['x'], datapoints['y']):
            label = "{:d}".format(y)
            plt.annotate(label, (x, y), textcoords="offset points", xytext=(0, 10), ha='center')
    plt.legend()
    plt.title("Viewer Count by Service")
    plt.xlabel("Date")
    plt.ylabel("Viewer Count")
    # COLORBLIND EXPERIMENT plt.rc('axes', prop_cycle=cc)
    plt.savefig(os.path.join(dataDirectory, "CountByService.png"))
    plt.show()
    print()

def CountTopTenLocations():
    print("Top Ten Remote Locations (excludes Cedar Rapids, Marion, & Hiawatha)")
    weekNumbers = GetWeekNumbers()
    count = 0
    locations = db.query('SELECT COUNT(*), city, state FROM data WHERE city<>\'\' AND city<>\'Cedar Rapids\'  AND city<>\'Marion\'  AND city<>\'Hiawatha\' GROUP BY city, state ORDER BY COUNT(*) DESC')
    for row in locations:
        if count >= 10:
            break
        count += 1
        print(f'{row["city"]}, {row["state"]} : {row["COUNT(*)"]}')
        index = 0
        x = []
        y = []
        locationStats = db.query(f'SELECT COUNT(*), weekNumber FROM data WHERE city=\'{row["city"]}\' AND state=\'{row["state"]}\' GROUP BY weekNumber ORDER BY weekNumber')
        for rowStats in locationStats:
            while weekNumbers[index] < rowStats["weekNumber"]:
                x.append(str(weekNumbers[index]))
                y.append(0)
                index += 1
            print(f'  {rowStats["weekNumber"]}: {rowStats["COUNT(*)"]}')
            x.append(str(rowStats["weekNumber"]))
            y.append(rowStats["COUNT(*)"])
            index += 1
        print(x)
        print(y)
        plt.plot(x, y, label=f'{row["city"]}, {row["state"]}')
    plt.legend()
    plt.title("Top Ten Remote Locations (excludes Cedar Rapids, Marion, & Hiawatha)")
    plt.xlabel("ISO Week Number")
    plt.ylabel("Viewer Count")
    plt.show()
    print()

def CountLocationsByService():
    print("Remote Locations by Service (excludes Cedar Rapids, Marion, & Hiawatha)")
    print("city, state service: total (average)")
    count = len(GetWeekNumbers())
    result = db.query('SELECT COUNT(*), city, state, service FROM data WHERE city<>\'\' AND city<>\'Cedar Rapids\' AND city<>\'Marion\'  AND city<>\'Hiawatha\' GROUP BY city, state, service ORDER BY COUNT(*) DESC')
    for row in result:
        averageResult = db.query(f'SELECT COUNT(*) FROM data WHERE city=\'{row["city"]}\' AND state=\'{row["state"]}\' AND service=\'{row["service"]}\' GROUP BY weekNumber ORDER BY weekNumber')
        average = 0
        for avgRow in averageResult:
            average += avgRow["COUNT(*)"]
        print(f'{row["city"]}, {row["state"]} {row["service"]}: {row["COUNT(*)"]} ({average/count})')
    print()

def GetIPsTopTenLocations():
    print("Top Ten Remote Location IPs (excludes Cedar Rapids, Marion, & Hiawatha)")
    count = 0
    locations = db.query('SELECT COUNT(*), city, state FROM data WHERE city<>\'\' AND city<>\'Cedar Rapids\'  AND city<>\'Marion\'  AND city<>\'Hiawatha\' GROUP BY city, state ORDER BY COUNT(*) DESC')
    for row in locations:
        if count >= 10:
            break
        count += 1
        print(f'{row["city"]}, {row["state"]}')
        ipAddresses = db.query(f'SELECT ipAddress FROM data WHERE city=\'{row["city"]}\' AND state=\'{row["state"]}\' GROUP BY ipAddress ORDER BY ipAddress')
        for ipAddress in ipAddresses:
            print(f'   {ipAddress["ipAddress"]}')
        print()
    print()

def WatchtimeHistogram():
    result = db.query("SELECT watchTimeMinutes FROM data")
    watchtimes = []
    for row in result:
        watchtimes.append(row["watchTimeMinutes"])
    plt.hist(watchtimes, bins=5)
    plt.show()

def GetWeekNumbers():
    ret = []
    result = db.query("SELECT weekNumber FROM data GROUP BY weekNumber ORDER BY weekNumber")
    for row in result:
        ret.append(row["weekNumber"])
    return ret

def GetDatestamps():
    ret = []
    result = db.query("SELECT localDatestamp FROM data WHERE weekday>=5 GROUP BY localDatestamp ORDER BY localDatestamp")
    for row in result:
        ret.append(row["localDatestamp"])
    return ret


def QuickPlot(x, y, label=None, xticks=None):
    plt.plot(x, y, label=label)
    plt.xticks(range(len(xticks)), xticks)


def QuickPlotWithTrendline(x, y, label=None, xticks=None):
    QuickPlot(x, y, label, xticks)
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    if label is None:
        plt.plot(x, p(x), "--")
    else:
        plt.plot(x, p(x), "--", label=f'{label} - Trend')

if __name__ == "__main__":
    print("ADA: Welcome to ADA, the Antioch Data Analysis tool\n")
    Load_Database()
    CountByDateAndService()
    CountTopTenLocations()
    CountLocationsByService()
    #GetIPsTopTenLocations()
    exit()

    datapoints = {}
    xlabels = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
    datapoints['x'] = [0, 1, 2, 3, 4, 5, 6, 7]
    datapoints['y'] = [0, 1, 2, 3, 4, 5, 6, 7]
    QuickPlotWithTrendline(datapoints['x'], datapoints['y'], "x^1", xlabels)
    datapoints['x'] = [0, 2, 4, 6]
    datapoints['y'] = [0, 4, 16, 36]
    QuickPlotWithTrendline(datapoints['x'], datapoints['y'], "x^2", xlabels)
    datapoints['x'] = [0, 1, 3, 4, 6, 7]
    datapoints['y'] = [0, 10, 0, -10, 0, 10]
    QuickPlotWithTrendline(datapoints['x'], datapoints['y'], "x^w/e", xlabels)
    plt.legend()
    plt.show()
    plt.savefig(os.path.join(dataDirectory, "TestFigure.png"))
    plt.close()
