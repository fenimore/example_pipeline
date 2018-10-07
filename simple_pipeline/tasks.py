import os
from datetime import datetime
import luigi
from luigi import Task, IntParameter, DateParameter, LocalTarget


class MonthTask(Task):
    date = DateParameter(default=datetime.today().date())

    def output(self):
        return LocalTarget("filesystem/m-{}".format(self.date.strftime("%m")))

    def run(self):
        open(self.output().path, 'a').close()

class DayTask(Task):
    date = DateParameter(default=datetime.today().date())

    def output(self):
        return LocalTarget("filesystem/d-{}".format(self.date.strftime("%d")))

    def run(self):
        open(self.output().path, 'a').close()

class YearTask(Task):
    date = DateParameter(default=datetime.today().date())

    def output(self):
        return LocalTarget("filesystem/y-{}".format(self.date.strftime("%Y")))

    def run(self):
        open(self.output().path, 'a').close()

class DateTask(Task):
    date = DateParameter(default=datetime.today().date())

    def requires(self):
        return {
            "day": DayTask(date=self.date),
            "month": MonthTask(date=self.date),
            "year": YearTask(date=self.date),
        }

    def output(self):
        return LocalTarget("filesystem/date-{}".format(self.date.strftime("%Y%m%d")))

    def run(self):
        open(self.output().path, 'a').close()



if __name__ == '__main__':
     luigi.build([DateTask()], workers=5, local_scheduler=True)
