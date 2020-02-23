import sqlalchemy as sa
import os
import gzip
import json
import luigi
import util
import inter
import datetime
from sqlalchemy import text
from ftplib import FTP
import subprocess


class GetInterMag(luigi.Task):
    year = luigi.IntParameter()
    mon = luigi.IntParameter()
    day = luigi.IntParameter()
    obs = luigi.Parameter()
    magdir = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        mag_file = "{}/{}{}{:02d}{:02d}vmin.min.gz".format(self.magdir, self.obs, self.year, self.mon, self.day)
        return luigi.LocalTarget(mag_file)

    def run(self):
        ftp_file = "intermagnet/minute/variation/IAGA2002/{}/{:02d}/{}{}{:02d}{:02d}vmin.min".format(self.year,
                                                                                                     self.mon, self.obs,
                                                                                                     self.year,
                                                                                                     self.mon, self.day)
        mag_file = "{}/{}{}{:02d}{:02d}vmin.min".format(self.magdir, self.obs, self.year, self.mon, self.day)
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
                l = ftp.retrlines('LIST ' + ftp_file + ".gz")
                self.get_mag_file(ftp, ftp_file + ".gz", mag_file + ".gz")
            except Exception as e:
                dd = datetime.date.today() - datetime.date(year=self.year, month=self.mon, day=self.day)
                if dd.days > 30:
                    with open(mag_file, 'w') as f:
                        f.write("missing\n")
                    subprocess.run(["gzip", mag_file])

    def get_mag_file(self, ftp, ftp_file, mag_file):
        with open(mag_file, 'wb') as fp:
            ftp.retrbinary('RETR ' + ftp_file, fp.write)


class Load_site(luigi.Task):
    d = luigi.DateParameter()
    obs = luigi.Parameter()
    magdir = luigi.Parameter()

    def requires(self):
        return [GetInterMag(self.d.year, self.d.month, self.d.day, self.obs.lower(), self.magdir)]

    def complete(self):
        sql = 'select count(*) ' \
              '    from mag.data d ' \
              '    join mag.observatories o ' \
              '        on o.id = d.observatory_id ' \
              '    where o.name = \'' + self.obs + '\' ' \
              '      and d.obs_date = \'' + self.d.strftime('%Y-%m-%d') + '\''
        t = text(sql)
        result = conn.execute(t)
        n = result.fetchone()[0]
        if n > 0:
            return True
        else:
            return False

    def run(self):
        mag_file = "{}/{}{}{:02d}{:02d}vmin.min.gz".format(self.magdir, self.obs.lower(), self.d.year, self.d.month, self.d.day)
        with gzip.open(mag_file,'rt',encoding='utf-8', errors='ignore') as f:
            for line in f:
                if 'missing' in line:
                    return
        dm = inter.Intermag_File(mag_file)
        sql = 'select id ' \
              '    from  mag.observatories o ' \
              '    where  o.name = \'' + self.obs + '\' '
        t = text(sql)
        result = conn.execute(t)
        row = result.fetchone()
        id = -1
        if row is not None:
            id = row[0]
        else:
            sql = "insert into mag.observatories( name, station_name, source_name,lat,long ) values (\'" + self.obs + "\', \'"+dm.get_station()+"\', \'"+ dm.get_source()+ "\' ,"+str(dm.get_lat())+" , "+str(dm.get_long())+ "    )"
            t = text(sql)
            conn.execute(t)
            sql = 'select id ' \
                  '    from  mag.observatories o ' \
                  '    where  o.name = \'' + self.obs + '\' '
            t = text(sql)
            result = conn.execute(t)
            id = row = result.fetchone()
            if row is not None:
                id = row[0]
        j = []
        df = dm.get_data()
        if len(df.index)>0 :
            for index, row in df.iterrows():
                j.append( {"dt": str(row['datetime']), "x": row['x'], "y": row['y'], "z": row['z']})
            t = text("insert into mag.data (observatory_id, obs_date, values) values ( "+str(id)+", '"+str(self.d)+"', '"+str(json.dumps(j))+"')")
            conn.execute(t)
        os.remove(mag_file)


class Load_mag_data(luigi.Task):
    st = luigi.DateParameter(default=datetime.date.today())
    obs = luigi.ListParameter()
    magdir = luigi.Parameter()
    task_complete = False

    def requires(self):
        obs = list(self.obs)
        b = self.st
        e = datetime.date.today()
        for d in util.daterange(b, e):
            for o in obs:
                yield Load_site(d, o, self.magdir)

    def complete(self):
        return self.task_complete

    def run(self):
        task_complete = True


engine = sa.create_engine('postgresql://' + os.environ['MAGL'] + ':' + os.environ['MAGP'] + '@localhost:5432/mag')   #-------------------------------------------------
conn = engine.connect()


if __name__ == '__main__':
    luigi.run()
