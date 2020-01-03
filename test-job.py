import luigi

class TaskB(luigi.Task):
    task_complete = False
    def run(self):
        print( "running task b")
        self.task_complete = True

    def complete(self):
        return  self.task_complete


class TaskA(luigi.Task):
    task_complete = False
    def requires(self):
        return TaskB()

    def run(self):
        print( "running task a")
        self.task_complete = True

    def complete(self):
        return  self.task_complete


if __name__ == '__main__':
    luigi.run()