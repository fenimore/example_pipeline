import os
from datetime import datetime, timedelta
from functools import reduce
from itertools import groupby
from collections import namedtuple

from dag.constants import (
    SEASON,
    ZODIAC,
    WORKWEEK,
    SIGNS,
    _horoscope,
)

import holidays
import luigi
from luigi import Task, Parameter, IntParameter, DateParameter, LocalTarget


class SeasonTask(Task):
    """
       Northern Hemisphere seasons, not based on the
       equinox but rather a 3 month cut off
       Because the that, this job is only run once a month
       TODO: make it use a date and use real seasons
    """
    month = IntParameter()

    def output(self):
        return LocalTarget(
            "filesystem/SEASON_{}".format(
                self.month
            )
        )

    def run(self):
        with open(self.output().path, 'a') as f:
            f.write(SEASON[self.month])


class WorkDayTask(Task):
    'WorkDayTask is a simple daily task to check if a given day is a workday'
    date = DateParameter()

    def output(self):
        return LocalTarget(
            "filesystem/WORKDAY_{}".format(
                self.date.strftime("%Y%m%d")
            )
        )

    def run(self):
        us_holidays = holidays.US()
        is_holiday = True if self.date in us_holidays else False
        is_workday = WORKWEEK[self.date.weekday()]
        with open(self.output().path, 'a') as f:
            if not is_workday:
                f.write("weekend")
            elif is_holiday:
                f.write("holiday")
            else:
                f.write("work")


class ZodiacTask(Task):
    year: int = IntParameter()

    def output(self):
        return LocalTarget("filesystem/ZODIAC_{}".format(self.year))

    def run(self):
        zodiac = ZODIAC[(self.year - 2000) % 12]
        with open(self.output().path, 'a') as f:
            f.write(zodiac)

class HoroscopeTask(Task):
    date: datetime.date = DateParameter()

    def output(self):
        return LocalTarget(
            "filesystem/HOROSCOPE_{}".format(
                self.date.strftime("%Y%m%d")
            )
        )

    def run(self):
        with open(self.output().path, 'a') as f:
            f.write(_horoscope(self.date.day, self.date.month))


class DayAggTask(Task):
    date = DateParameter(default=datetime.today().date())

    def requires(self):
        return {
            "workday": WorkDayTask(
                date=self.date,
            ),
            "season": SeasonTask(
                month=self.date.month,
            ),
            "horoscope": HoroscopeTask(
                date=self.date,
            ),
            "zodiac": ZodiacTask(
                year=self.date.year,
            ),
        }

    def output(self):
        return LocalTarget(
            "filesystem/DATEAGG-{}".format(
                self.date.strftime("%Y%m%d")
            )
        )

    def run(self):
        with open(self.requires()["workday"].output().path, "r") as f:
            work_day = f.read().strip()
            assert work_day
        with open(self.requires()["season"].output().path, "r") as f:
            season = f.read().strip()
            assert season
        with open(self.requires()["zodiac"].output().path, "r") as f:
            zodiac = f.read().strip()
            assert zodiac
        with open(self.requires()["horoscope"].output().path, "r") as f:
            horoscope = f.read().strip()
            assert horoscope
        with open(self.output().path, 'a') as f:
            f.write("\n".join(
                [
                    zodiac,
                    season,
                    work_day,
                    horoscope
                ])
            )


class DaysBack_90(Task):
    'This job transforms the date aggregates into 90 day "logs"'
    date = DateParameter(default=datetime.today().date())

    def requires(self):
        for day_back in range(0, 90):
            retro = self.date - timedelta(days=day_back)
            yield DayAggTask(date=retro)

    def output(self):
        return LocalTarget(
            "filesystem/retrospective-{}.log".format(
                self.date.strftime("%Y%m%d")
            )
        )
    def run(self):
        zodiacs = {z: 0 for z in ZODIAC.values()}
        seasons = {s: 0 for s in SEASON.values()}
        horoscopes = {s: 0 for s in SIGNS}
        logs = []
        for date_task in self.requires():
            with open(date_task.output().path, "r") as f:
                date_info = f.read().strip()
                zodi, seas, work, horo = date_info.split("\n")
                logs.append(",".join([zodi, seas, work, horo]))
        with open(self.output().path, 'a') as f:
            f.write("\n".join(logs))

class HoroscopeReportTask(Task):
    'This is a Map Reduce job'
    date = DateParameter(default=datetime.today().date())

    def requires(self):
        return DaysBack_90(date=self.date)

    def output(self):
        return LocalTarget(
            "filesystem/horoscope_report-{}.tsv".format(
                self.date.strftime("%Y%m%d")
            )
        )
    def run(self):
        logs = []
        with open(self.requires().output().path, "r") as f:
            logs = f.read().strip().split("\n")

        def map_log(row):
            zodi, seas, work, horo = row.split(",")
            is_workday = work == "work"
            is_weekend = work == "weekend"
            is_holiday = work == "holiday"
            return (
                horo,  # key
                (
                    1 if is_workday else 0,
                    1 if is_holiday else 0,
                    1 if is_weekend else 0,
                    1,  # count
                ),
            )
        mapped_logs = map(map_log , logs)
        Row = namedtuple(
            "Row", ["work", "holiday", "weekend", "total"]
        )
        reduced_logs = reduce_by_key(
            lambda l, r: Row(
                work=l[0] +r[0],
                holiday=l[1] + r[1],
                weekend=l[2] + r[2],
                total=l[3] + r[3],
            ),
            mapped_logs
        )
        tsv = ["sign\tworking_days\tholidays\tweekends\ttotal_days"]
        for row in reduced_logs:
            tsv.append(
                "{}\t{}\t{}\t{}\t{}".format(
                    row[0],
                    row[1].work, row[1].holiday,
                    row[1].weekend, row[1].total,
                )
            )
        with open(self.output().path, 'a') as f:
            f.write("\n".join(tsv))


def reduce_by_key(func, iterable):
    """
    Reduce by key
    adapted from
    https://gist.github.com/Juanlu001/562d1ec55be970403442
    """
    grouped_iters = groupby(
        # you have to sort when using groupby anyway
        sorted(iterable, key=lambda r: r[0]),
        lambda r: r[0]  # tell groupby what to group by
    )
    applied_func = map(
        lambda row: (
            # grouped_pairs are (key, <iterable>)
            #   and we have to reduce that^ iterable
            row[0],  # The key
            reduce(  # Reduce the _grouper objects
                func,
                [grouped[1] for grouped in row[1]],
            )
        ),
        grouped_iters
    )
    return applied_func



if __name__ == '__main__':
     luigi.build(
         [HoroscopeReportTask()], workers=10, local_scheduler=True
     )
