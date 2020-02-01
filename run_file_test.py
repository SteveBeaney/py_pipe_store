import luigi
from ftplib import FTP
import datetime
import subprocess
import io
import shutil
import pandas as pd
import numpy as np
import math
import gzip

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)

class GetInterMag(luigi.Task):
    year = luigi.IntParameter()
    mon = luigi.IntParameter()
    day = luigi.IntParameter()
    obs = luigi.Parameter()
    magdir = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        mag_file="{}/{}{}{:02d}{:02d}vmin.min.gz".format(self.magdir,self.obs,self.year,self.mon,self.day)
        return luigi.LocalTarget(mag_file)

    def run(self):
        ftp_file= "intermagnet/minute/variation/IAGA2002/{}/{:02d}/{}{}{:02d}{:02d}vmin.min".format(self.year,self.mon,self.obs,self.year,self.mon,self.day)
        mag_file="{}/{}{}{:02d}{:02d}vmin.min".format(self.magdir,self.obs,self.year,self.mon,self.day)
        ftp = FTP('ftp.seismo.nrcan.gc.ca')
        ftp.login()
        self.download_zip_mag(ftp, ftp_file, mag_file)
        ftp.quit()

    def download_zip_mag(self, ftp, ftp_file, mag_file):
        try:
            l = ftp.retrlines('LIST ' + ftp_file)
            self.get_mag_file(ftp, ftp_file, mag_file)
            subprocess.run(["gzip", mag_file])
        except Exception as e:
            try:
                l = ftp.retrlines('LIST ' + ftp_file+".gz")
                self.get_mag_file(ftp, ftp_file+".gz", mag_file+".gz")
            except Exception as e:
                dd = datetime.date.today() - datetime.date(year=self.year,month=self.mon,day=self.day)
                if dd.days > 30 :
                    with open(mag_file,'w') as f:
                        f.write("missing\n")
                    subprocess.run(["gzip", mag_file])
        # time.sleep(random.randint(0, 6))

    def get_mag_file(self, ftp, ftp_file, mag_file):
        with open(mag_file, 'wb') as fp:
            ftp.retrbinary('RETR ' + ftp_file, fp.write)


class GetProcessedMag(luigi.Task):
    year = luigi.IntParameter()
    mon = luigi.IntParameter()
    day = luigi.IntParameter()
    obs = luigi.Parameter()
    magdir = luigi.Parameter()
    xyzdir = luigi.Parameter()

    def requires(self):
        return [GetInterMag(self.year,self.mon,self.day,self.obs,self.magdir)]

    def output(self):
        processed_file="{}/{}{}{:02d}{:02d}xyz.min.gz".format(self.xyzdir,self.obs,self.year,self.mon,self.day)
        return luigi.LocalTarget(processed_file)

    def run(self):
        mag_file="{}/{}{}{:02d}{:02d}vmin.min.gz".format(self.magdir,self.obs,self.year,self.mon,self.day)
        processed_file="{}/{}{}{:02d}{:02d}xyz.min.gz".format(self.xyzdir,self.obs,self.year,self.mon,self.day)
        skips = 0
        with gzip.open(mag_file,'rt',encoding='utf-8', errors='ignore') as f:
            count = 0
            for line in f:
                if line.startswith('DATE'):
                    skips = count;
                    break
                count += 1
        if skips > 0:
            with gzip.open(mag_file, 'rt', encoding='utf-8', errors='ignore') as f:
                st = f.read()
            d = pd.read_fwf( io.StringIO(st), skiprows=skips)
            d.columns = ['DATE', 'TIME', 'DOY', 'H', 'D', 'Z', 'F']
            d['x'] = np.cos((d['D']*(math.pi/180)))*d['H']
            d['y'] = np.sin((d['D']*(math.pi/180)))*d['H']
            d['datetime'] = pd.to_datetime(d['DATE']+' '+d['TIME'])
            d.columns= ['DATE','TIME','DOY','h','d','z','f','x','y','datetime']
            d['iaga'] = self.obs
            dd = d.filter(['iaga','datetime','h','d','f','x','y','z'], axis=1)
            dd.to_csv(processed_file,index=False,compression='gzip')
        else:
            shutil.copyfile(mag_file, processed_file)

class AllFiles(luigi.Task):
    st = luigi.DateParameter(default=datetime.date.today())
    obs = luigi.ListParameter()
    magdir = luigi.Parameter()
    xyzdir = luigi.Parameter()
    task_complete = False

    def requires(self):
        obs = list(self.obs)
        # b = datetime.date(2017,10,28)
        b = self.st
        e = datetime.date.today()
        for d in daterange( b, e ):
            for o in obs:
                yield GetProcessedMag(d.year, d.month, d.day, o, self.magdir, self.xyzdir)
                # yield GetInterMag(d.year, d.month, d.day, o, self.magdir)

    def complete(self):
        return  self.task_complete

    def run(self):
        task_complete = True


if __name__ == '__main__':
    luigi.run()