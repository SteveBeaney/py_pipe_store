import luigi


class PrintFile(luigi.Task):
    mag_file = luigi.IntParameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.mag_file)

    def run(self):
        with self.output().open('w') as f:
            f.write("done\n")


class AllFiles(luigi.Task):
    task_complete = False

    def requires(self):
        return [PrintFile("/home/steve/2001.txt"),PrintFile("/home/steve/2002.txt")]

    def complete(self):
        return  self.task_complete

    def run(self):
        print('All done')
        task_complete = True


if __name__ == '__main__':
    luigi.run()