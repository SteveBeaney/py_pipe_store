import luigi
from ftplib import FTP
import datetime
import subprocess
import time
import random

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)

class PrintFile(luigi.Task):
    year = luigi.IntParameter()
    mon = luigi.IntParameter()
    day = luigi.IntParameter()
    obs = luigi.Parameter()
    outdir = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        mag_file="{}/{}{}{:02d}{:02d}vmin.min.gz".format(self.outdir,self.obs,self.year,self.mon,self.day)
        return luigi.LocalTarget(mag_file)

    def run(self):
        ftp_file= "intermagnet/minute/variation/IAGA2002/{}/{:02d}/{}{}{:02d}{:02d}vmin.min".format(self.year,self.mon,self.obs,self.year,self.mon,self.day)
        mag_file="/home/steve/intermagnet/{}{}{:02d}{:02d}vmin.min".format(self.obs,self.year,self.mon,self.day)
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
        time.sleep(random.randint(0, 6))


    def get_mag_file(self, ftp, ftp_file, mag_file):
        with open(mag_file, 'wb') as fp:
            ftp.retrbinary('RETR ' + ftp_file, fp.write)


class AllFiles(luigi.Task):
    st = luigi.DateParameter(default=datetime.date.today())
    obs = luigi.ListParameter()
    outdir = luigi.Parameter()
    task_complete = False

    def requires(self):
        obs = list(self.obs)
        # b = datetime.date(2017,10,28)
        b = self.st
        e = datetime.date.today()
        for d in daterange( b, e ):
            for o in obs:
                yield PrintFile(d.year,d.month,d.day,o,self.outdir)

    def complete(self):
        return  self.task_complete

    def run(self):
        task_complete = True


if __name__ == '__main__':
    luigi.run()