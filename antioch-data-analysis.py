import csv
from cycler import cycler
import dataset
from datetime import datetime
from dateutil import tz
from io import BytesIO
import matplotlib.pyplot as plt
import os
import pycurl

# Global Variables (because I am lazy and have no face)
dataDirectory = "C:\\Users\\gwschive\\Downloads\\ADA_Data"
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
                                                  tzinfo=from_zone).astimezone(to_zone).isocalendar()[1]))


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

def CountByCity():
    # Get count by city
    print("City Breakdown:")
    result = db.query("SELECT COUNT(ipAddress), city, state, country from data GROUP BY city ORDER BY city")
    for row in result:
        if row["city"] == "":
            print(f'  <unspecified>, {row["state"]}, {row["country"]}: {row["COUNT(ipAddress)"]}')
        else:
            print(f'  {row["city"]}, {row["state"]}, {row["country"]}: {row["COUNT(ipAddress)"]}')
    print()

def CountByDatestamp():
    # Count by localDatestamp
    print("Count by Datestamp:")
    result = db.query("SELECT COUNT(localDatestamp), localDatestamp from data GROUP BY localDatestamp ORDER BY localDatestamp")
    for row in result:
        print(f'  {row["localDatestamp"]}: {row["COUNT(localDatestamp)"]}')
    print()

def CountByDateAndService():
    # Count by date and service
    print("Count by Date and Service:")
    datestampList = []
    x = []
    xcurr = 1
    datestamps = db.query("SELECT localDatestamp FROM data GROUP BY localDatestamp ORDER BY localDatestamp")
    for datestamp in datestamps:
        datestampList.append(datestamp["localDatestamp"])
        x.append(xcurr)
        xcurr += 1
    plt.xticks(x, datestampList, rotation='vertical')
    print(datestampList)
    services = db.query("SELECT service FROM data GROUP BY service ORDER BY service")
    for service in services:
        data = []
        datestampList = []
        datestamps = db.query("SELECT localDatestamp FROM data GROUP BY localDatestamp ORDER BY localDatestamp")
        for datestamp in datestamps:
            datestampList.append(datestamp["localDatestamp"])
            result = db.query(f'SELECT COUNT(*) FROM data WHERE localDatestamp=\'{datestamp["localDatestamp"]}\' AND service=\'{service["service"]}\' GROUP BY localDatestamp')
            hadRows = False
            for row in result:
                hadRows = True
                data.append(row["COUNT(*)"])
            if not hadRows:
                datestampList.pop(-1)
        print(service["service"] + " " + str(data))
        plt.plot(datestampList, data, label=service["service"])
        for x, y in zip(datestampList, data):
            label = "{:d}".format(y)
            plt.annotate(label, (x, y), textcoords="offset points", xytext=(0, 10), ha='center')
    plt.legend()
    plt.title("Viewer Count by Service")
    plt.xlabel("Date")
    plt.ylabel("Viewer Count")
    # COLORBLIND EXPERIMENT plt.rc('axes', prop_cycle=cc)
    plt.show()
    print()

def CountByWeekNumberAndLocation():
    # Create a chart showing X: ViewerCount, Y: weekNumber, and Legend: Location
    print("Count by Location")
    locations = []
    x = []
    y = []
    # City-wise
    result = db.query("SELECT state, city FROM data GROUP BY state, city ORDER BY state, city")
    # State-wise
    #result = db.query("SELECT state FROM data GROUP BY state ORDER BY state")
    for row in result:
        # City-wise
        locations.append(f'{row["city"]}, {row["state"]}')
        # State-wise
        #locations.append(f'{row["state"]}')
    result = db.query("SELECT weekNumber FROM data GROUP BY weekNumber ORDER BY weekNumber")
    for row in result:
        x.append(row["weekNumber"])
    for location in locations:
        # Remove Cedar Rapids and Marion from graph
        if location == "Cedar Rapids, IA" or location == "Marion, IA":
            continue
        y = []
        for currentWeekNumber in x:
            # City-wise
            result = db.query(f'SELECT COUNT(*), weekNumber FROM data WHERE state=\'{location.split(", ")[1]}\' AND city=\'{location.split(", ")[0]}\' GROUP BY weekNumber ORDER BY weekNumber')
            # State-wise
            #result = db.query(f'SELECT COUNT(*), weekNumber FROM data WHERE state=\'{location}\' GROUP BY weekNumber ORDER BY weekNumber')
            dataFound = False
            for row in result:
                if row["weekNumber"] == currentWeekNumber:
                    y.append(row["COUNT(*)"])
                    dataFound = True
            if not dataFound:
                y.append(0)
        plt.plot(x, y, label=location)
    plt.legend()
    plt.show()
    print()

# This function excludes Cedar Rapids, Marion, and Hiawatha
def CountTopTenLocations():
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



def GetWeekNumbers():
    ret = []
    result = db.query("SELECT weekNumber FROM data GROUP BY weekNumber ORDER BY weekNumber")
    for row in result:
        ret.append(row["weekNumber"])
    return ret

def GetDatestamps():
    ret = []
    result = db.query("SELECT localDatestamp FROM data GROUP BY localDatestamp ORDER BY localDatestamp")
    for row in result:
        ret.append(row["localDatestamp"])
    return ret

if __name__ == "__main__":
    print("ADA: Welcome to ADA, the Antioch Data Analysis tool\n")
    Load_Database()
    CountByDateAndService()
    #CountByWeekNumberAndLocation()
    CountTopTenLocations()
