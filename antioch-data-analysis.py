import csv
import dataset
from datetime import datetime
from dateutil import tz
from io import BytesIO
import matplotlib.pyplot as plt
import os
import pycurl

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
#exit()

dataDirectory = "C:\\Users\\gwschive\\Downloads\\ADA_Data"

from_zone = tz.tzutc()
to_zone = tz.tzlocal()

print("ADA: Welcome to ADA, the Antioch Data Analysis tool\n")

db = dataset.connect('sqlite:///:memory:')
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
                                          service=filename[:-4]))

# Get count by country
result = db.query("SELECT COUNT(ipAddress), country from data GROUP BY country ORDER BY country")
print("Country Breakdown:")
for row in result:
    if row["country"] == "":
        print(f'  <unspecified>: {row["COUNT(ipAddress)"]}')
    else:
        print(f'  {row["country"]}: {row["COUNT(ipAddress)"]}')
print()

# Get count by state
print("State Breakdown:")
result = db.query("SELECT COUNT(ipAddress), state, country from data GROUP BY state ORDER BY state")
for row in result:
    if row["state"] == "":
        print(f'  <unspecified>, {row["country"]}: {row["COUNT(ipAddress)"]}')
    else:
        print(f'  {row["state"]}, {row["country"]}: {row["COUNT(ipAddress)"]}')
print()

# Get count by city
print("City Breakdown:")
result = db.query("SELECT COUNT(ipAddress), city, state, country from data GROUP BY city ORDER BY city")
for row in result:
    if row["city"] == "":
        print(f'  <unspecified>, {row["state"]}, {row["country"]}: {row["COUNT(ipAddress)"]}')
    else:
        print(f'  {row["city"]}, {row["state"]}, {row["country"]}: {row["COUNT(ipAddress)"]}')
print()

# Convert timestamps
#print("Convert Timestamps:")
#from_zone = tz.tzutc()
#to_zone = tz.tzlocal()
#result = db.query("SELECT timestamp from data ORDER BY timestamp")
#for row in result:
#    utc = datetime.strptime(str(row["timestamp"])[0:19], '%Y-%m-%dT%H:%M:%S')
#    utc = utc.replace(tzinfo=from_zone)
#    print("  " + str(row["timestamp"])[0:19].replace('T', ' ') + " --> " + str(utc.astimezone(to_zone)))
#print()

# Count by localDatestamp
print("Count by Datestamp:")
result = db.query("SELECT COUNT(localDatestamp), localDatestamp from data GROUP BY localDatestamp ORDER BY localDatestamp")
for row in result:
    print(f'  {row["localDatestamp"]}: {row["COUNT(localDatestamp)"]}')
print()


# Count by date and service
print("Count by Date and Service:")
datestampList = []
datestamps = db.query("SELECT localDatestamp FROM data GROUP BY localDatestamp ORDER BY localDatestamp")
for datestamp in datestamps:
    datestampList.append(datestamp["localDatestamp"])
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
            #data.append(0)
    print(service["service"] + " " + str(data))
    plt.plot(datestampList, data, label=service["service"])
plt.legend()
plt.show()
exit()


for datestamp in datestamps:
    print(datestamp["localDatestamp"])
    result = db.query(f'SELECT COUNT(*), service FROM data WHERE localDatestamp=\'{datestamp["localDatestamp"]}\' GROUP BY service ORDER BY service')
    for row in result:
        print(row)
exit()




result = db.query("SELECT COUNT(*), localDatestamp, service FROM data WHERE watchTimeMinutes > 7 GROUP BY localDatestamp, service ORDER BY localDatestamp, service")
for row in result:
    if row["service"] not in data.keys():
        data[row["service"]] = []
    data[row["service"]].append(row["COUNT(*)"])
    if row["localDatestamp"] not in datestamps:
        datestamps.append(row["localDatestamp"])
    print(f'  {row["localDatestamp"]} - {row["service"]}: {row["COUNT(*)"]}')
print()
# Plot
print(data)
print(datestamps)
exit()
plt.plot()
plt.legend()
plt.show()


# DON'T NEED A PLOT FOR NOW
exit()
# Display a state-wise plot (in-progress)
plt.style.use('ggplot')
result = db.query("SELECT COUNT(ipAddress), state, country from data GROUP BY state ORDER BY state")
states = []
viewers = []
for row in result:
    if row["state"] == "":
        states.append("<unspecified>, " + row["country"])
    else:
        states.append(row["state"] + ", " + row["country"])
    viewers.append(row["COUNT(ipAddress)"])
x_pos = [i for i, _ in enumerate(states)]
plt.bar(x_pos, viewers)
plt.xlabel("State")
plt.ylabel("Viewer Count")
plt.title("Viewer Count by State")
plt.xticks(x_pos, states, rotation=60)
plt.savefig(os.path.join(dataDirectory, "plot.png"))
plt.show()
exit()

