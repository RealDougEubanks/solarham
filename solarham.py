import os
import json
import pytz
import time
import datetime
import requests
import xml.etree.ElementTree as ET
from influxdb_client import InfluxDBClient, WriteApi, WriteOptions

influxURL = os.environ['url']
influxToken = os.environ['token']

influx = InfluxDBClient(url=influxURL, token=influxToken, org='Home')
writeAPI = influx.write_api(write_options=WriteOptions(batch_size=1))

utcTime = datetime.datetime.utcnow()
localTime = datetime.datetime.now(pytz.timezone('America/New_York'))

def getSolarXML():
    URL = "https://www.hamqsl.com/solarxml.php"
    response = requests.get(URL)
    with open('/tmp/solar.xml', 'wb') as file:
        file.write(response.content)

def parseSolarXMLBandConditions():
    xmlfile = "/tmp/solar.xml"
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    for item in root.findall('./solardata/calculatedconditions'):
        for child in item:
            influxJSON = [
                           {
                           "measurement" : "bandconditions",
                           "tags" : { "band" : child.attrib['name'], "timeofday" : child.attrib['time'] },
                           "time" : int(time.time() * 1000000000),
                           "fields" : { "value" : child.text }
                           }
            ]
            writeAPI.write(bucket="solarham", record=influxJSON)
    return

def parseSolarConditions():
    xmlfile = "/tmp/solar.xml"
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    for item in root.findall('./solardata/solarflux'):
        solarflux = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "solarflux" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : solarflux }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/aindex'):
        aindex = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "aindex" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : aindex }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/kindex'):
        kindex = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "kindex" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : kindex }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/xray'):
        xray = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "xray" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : xray }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/sunspots'):
        sunspots = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "sunspots" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : sunspots }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/heliumline'):
        heliumline = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "heliumline" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : heliumline }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/protonflux'):
        protonflux = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "protonflux" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : protonflux }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/electronflux'):
        electronflux = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "electronflux" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : electronflux }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/aurora'):
        aurora = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "aurora" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : aurora }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/normalization'):
        normalization = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "normalization" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : normalization }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/latdegree'):
        latdegree = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "latdegree" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : latdegree }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/solarwind'):
        solarwind = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "solarwind" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : solarwind }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/magneticfield'):
        magneticfield = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "magneticfield" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : magneticfield }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/geomagneticfield'):
        geomagneticfield = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "geomagneticfield" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : geomagneticfield }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/signalnoise'):
        signalnoise = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "signalnoise" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : signalnoise }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/fof2'):
        fof2 = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "fof2" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : fof2 }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/mufffactor'):
        muffactor = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "muffactor" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : muffactor }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    for item in root.findall('./solardata/muff'):
        muff = item.text
        influxJSON = [
                        {
                            "measurement" : "solardata",
                            "tags" : { "tag" : "muff" },
                            "time" : int(time.time() * 1000000000),
                            "fields" : { "value" : muff }
                        }
        ]
        writeAPI.write(bucket="solarham", record=influxJSON)
    return

def main():
    getSolarXML()
    parseSolarXMLBandConditions()
    parseSolarConditions()
    influx.close

while True:
  main()
  time.sleep(30)
