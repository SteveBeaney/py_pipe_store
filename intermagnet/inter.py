import datetime
import pandas as pd
import numpy as np
import math
import gzip
import io


class Intermag_File():

    def __init__(self, filename):
        self.d = pd.DataFrame()
        self.d = self.d.filter(items=['datetime', 'x', 'y', 'z', 'f'])
        skips = 0
        self.obs = ''
        self.station_name = ''
        self.source = ''
        self.lat = -1
        self.long = -1
        with gzip.open(filename, 'rt', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if line.startswith(' IAGA CODE'):
                    self.obs = line[10:len(line) - 2].strip()
                if line.startswith(' Station Name'):
                    self.station_name = line[13:len(line) - 2].strip()
                if line.startswith(' Source of Data'):
                    self.source = line[15:len(line) - 2].strip()
                if line.startswith(' Geodetic Latitude'):
                    self.lat = float(line[18:len(line) - 2].strip())
                if line.startswith(' Geodetic Longitude'):
                    self.long = float(line[19:len(line) - 2].strip())
                if line.startswith(' Reported'):
                    self.form = line[9:len(line) - 2].strip()
                if line.startswith('DATE'):
                    skips = i;
                    self.date = datetime.datetime.strptime(next(f)[0:10], '%Y-%m-%d')
                    break
        if skips > 0 and self.obs != '':
            with gzip.open(filename, 'rt', encoding='utf-8', errors='ignore') as f:
                st = f.read()
            self.d = pd.read_fwf(io.StringIO(st), skiprows=skips)

            if self.form == 'HDZF':
                self.HDZF_to_xyz()
            if self.form == 'XYZF':
                self.XYZF_to_xyz()

    def XYZF_to_xyz(self):
        self.d.columns = ['DATE', 'TIME', 'DOY', 'x', 'y', 'z', 'f']
        self.d['h'] = np.sqrt(self.d['x'] * self.d['x'] + self.d['y'] * self.d['y'])
        self.d['d'] = np.arctan2(self.d['y'] , self.d['x']) * (180 / math.pi)
        self.form_xyz()

    def HDZF_to_xyz(self):
        self.d.columns = ['DATE', 'TIME', 'DOY', 'H', 'D', 'Z', 'F']
        self.d['x'] = np.cos((self.d['D'] * (math.pi / 180))) * self.d['H']
        self.d['y'] = np.sin((self.d['D'] * (math.pi / 180))) * self.d['H']
        self.d.columns = ['DATE', 'TIME', 'DOY', 'h', 'd', 'z', 'f', 'x', 'y']
        self.form_xyz()

    def form_xyz(self):
        self.d['datetime'] = pd.to_datetime(self.d['DATE'] + ' ' + self.d['TIME'])
        self.d['iaga'] = self.obs

        indexNames = self.d[(self.d['h'] == 99999.00) |
                            (self.d['d'] == 99999.00) |
                            (self.d['z'] == 99999.00) |
                            (self.d['f'] == 99999.00) |
                            (self.d['x'] == 99999.00) |
                            (self.d['y'] == 99999.00)].index
        self.d.drop(indexNames, inplace=True)

        self.d = self.d.filter(items=['datetime', 'x', 'y', 'z', 'h', 'f', 'd'])

    def output_xyz(self, filename):
        print(self.d)

    def get_site(self):
        return self.obs

    def get_rows(self):
        return len(self.d)

    def get_station(self):
        return self.station_name

    def get_source(self):
        return self.source

    def get_lat(self):
        return self.lat

    def get_long(self):
        return self.long

    def get_form(self):
        return self.form

    def get_data(self):
        return self.d

    def get_date(self):
        return self.date.date()


#ifile = Intermag_File('/home/steve/repos/py_pipe_store/testdata/val20200117vmin.min.gz')
#ifile = Intermag_File('/home/steve/repos/py_pipe_store/testdata/ups20200102vmin.min.gz')

#ifile = Intermag_File('/home/steve/repos/py_pipe_store/testdata/ded20190905vmin.min.gz')
#
#ifile.output_xyz('')
