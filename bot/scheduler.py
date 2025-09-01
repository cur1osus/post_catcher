import asyncio
import datetime
import functools
import logging
import random
import re
import warnings
from collections.abc import Callable, Hashable

logger = logging.getLogger("schedule")


class ScheduleError(Exception):
    """Base schedule exception."""


class ScheduleValueError(ScheduleError):
    """Base schedule value error."""


class IntervalError(ScheduleValueError):
    """An improper interval was used."""


class CancelJob:
    """Can be returned from a job to unschedule itself."""


class Scheduler:
    def __init__(self) -> None:
        self.jobs: list[Job] = []

    async def run_pending(self, *args, **kwargs):
        jobs = [asyncio.create_task(job.run()) for job in self.jobs if job.should_run]
        if not jobs:
            return [], []
        done, pending = await asyncio.wait(jobs, *args, **kwargs)
        return done, pending

    async def run_all(self, delay_seconds: int = 0, *args, **kwargs):
        if delay_seconds:
            warnings.warn(
                "The `delay_seconds` parameter is deprecated.",
                DeprecationWarning,
                stacklevel=2,
            )
        jobs = [asyncio.create_task(self._run_job(job)) for job in self.jobs[:]]
        if not jobs:
            return [], []
        done, pending = await asyncio.wait(jobs, *args, **kwargs)
        return done, pending

    def get_jobs(self, tag: None | Hashable = None) -> list["Job"]:
        if tag is None:
            return self.jobs[:]
        return [job for job in self.jobs if tag in job.tags]

    def clear(self, tag: None | Hashable = None) -> None:
        if tag is None:
            logger.info("Deleting *all* jobs")
            del self.jobs[:]
        else:
            logger.info('Deleting all jobs tagged "%s"', tag)
            self.jobs[:] = (job for job in self.jobs if tag not in job.tags)

    def cancel_job(self, job: "Job") -> None:
        try:
            logger.info('Cancelling job "%s"', str(job))
            self.jobs.remove(job)
        except ValueError:
            logger.info('Cancelling not-scheduled job "%s"', str(job))

    def every(self, interval: int = 1) -> "Job":
        job = Job(interval, self)
        return job

    async def _run_job(self, job: "Job"):
        ret = await job.run()

    @property
    def get_next_run(self, tag: None | Hashable = None) -> None | datetime.datetime:
        if not self.jobs:
            return None
        jobs_filtered = self.get_jobs(tag)
        if not jobs_filtered:
            return None
        return min(jobs_filtered).next_run

    @property
    def idle_seconds(self) -> None | float:
        if not self.get_next_run:
            return None
        return (self.get_next_run - datetime.datetime.now()).total_seconds()


class Job:
    def __init__(self, interval: int, scheduler: None | Scheduler = None):
        self.interval: int = interval
        self.latest: None | int = None
        self.job_func: None | functools.partial = None
        self.unit: None | str = None
        self.at_time: None | datetime.time = None
        self.at_time_zone = None
        self.last_run: None | datetime.datetime = None
        self.next_run: None | datetime.datetime = None
        self.start_day: None | str = None
        self.cancel_after: None | datetime.datetime = None
        self.tags: set = set()
        self.scheduler: None | Scheduler = scheduler

    def __lt__(self, other):
        return self.next_run < other.next_run

    def __str__(self) -> str:
        if hasattr(self.job_func, "__name__"):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)
        return ("Job(interval={}, unit={}, do={}, args={}, kwargs={})").format(
            self.interval,
            self.unit,
            job_func_name,
            "()" if self.job_func is None else self.job_func.args,
            "{}" if self.job_func is None else self.job_func.keywords,
        )

    def __repr__(self):
        def format_time(t):
            return t.strftime("%Y-%m-%d %H:%M:%S") if t else "[never]"

        def is_repr(j):
            return not isinstance(j, Job)

        timestats = "(last run: %s, next run: %s)" % (
            format_time(self.last_run),
            format_time(self.next_run),
        )
        if hasattr(self.job_func, "__name__"):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)
        if self.job_func is not None:
            args = [repr(x) if is_repr(x) else str(x) for x in self.job_func.args]
            kwargs = ["%s=%s" % (k, repr(v)) for k, v in self.job_func.keywords.items()]
            call_repr = job_func_name + "(" + ", ".join(args + kwargs) + ")"
        else:
            call_repr = "[None]"
        if self.at_time is not None:
            return "Every %s %s at %s do %s %s" % (
                self.interval,
                self.unit[:-1] if self.interval == 1 else self.unit,
                self.at_time,
                call_repr,
                timestats,
            )
        fmt = (
            "Every %(interval)s "
            + ("to %(latest)s " if self.latest is not None else "")
            + "%(unit)s do %(call_repr)s %(timestats)s"
        )
        return fmt % dict(
            interval=self.interval,
            latest=self.latest,
            unit=(self.unit[:-1] if self.interval == 1 else self.unit),
            call_repr=call_repr,
            timestats=timestats,
        )

    @property
    def second(self):
        assert self.interval == 1, "Use seconds instead of second"
        return self.seconds

    @property
    def seconds(self):
        self.unit = "seconds"
        return self

    @property
    def minute(self):
        assert self.interval == 1, "Use minutes instead of minute"
        return self.minutes

    @property
    def minutes(self):
        self.unit = "minutes"
        return self

    @property
    def hour(self):
        assert self.interval == 1, "Use hours instead of hour"
        return self.hours

    @property
    def hours(self):
        self.unit = "hours"
        return self

    @property
    def day(self):
        assert self.interval == 1, "Use days instead of day"
        return self.days

    @property
    def days(self):
        self.unit = "days"
        return self

    @property
    def week(self):
        assert self.interval == 1, "Use weeks instead of week"
        return self.weeks

    @property
    def weeks(self):
        self.unit = "weeks"
        return self

    @property
    def monday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .monday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "monday"
        return self.weeks

    @property
    def tuesday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .tuesday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "tuesday"
        return self.weeks

    @property
    def wednesday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .wednesday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "wednesday"
        return self.weeks

    @property
    def thursday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .thursday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "thursday"
        return self.weeks

    @property
    def friday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .friday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "friday"
        return self.weeks

    @property
    def saturday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .saturday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "saturday"
        return self.weeks

    @property
    def sunday(self):
        if self.interval != 1:
            raise IntervalError(
                "Scheduling .sunday() jobs is only allowed for weekly jobs."
            )
        self.start_day = "sunday"
        return self.weeks

    def tag(self, *tags: Hashable):
        if not all(isinstance(tag, Hashable) for tag in tags):
            raise TypeError("Tags must be hashable")
        self.tags.update(tags)
        return self

    def at(self, time_str: str, tz: None | str = None):
        if self.unit not in ("days", "hours", "minutes") and not self.start_day:
            raise ScheduleValueError(
                "Invalid unit (valid units are `days`, `hours`, and `minutes`)"
            )
        if tz is not None:
            import pytz

            if isinstance(tz, str):
                self.at_time_zone = pytz.timezone(tz)
            elif isinstance(tz, pytz.BaseTzInfo):
                self.at_time_zone = tz
            else:
                raise ScheduleValueError(
                    "Timezone must be string or pytz.timezone object"
                )
        if not isinstance(time_str, str):
            raise TypeError("at() should be passed a string")
        if self.unit == "days" or self.start_day:
            if not re.match(r"^[0-2]\d:[0-5]\d(:[0-5]\d)?$", time_str):
                raise ScheduleValueError(
                    "Invalid time format for a daily job (valid format is HH:MM(:SS)?)"
                )
        if self.unit == "hours":
            if not re.match(r"^([0-5]\d)?:[0-5]\d$", time_str):
                raise ScheduleValueError(
                    "Invalid time format for an hourly job (valid format is (MM)?:SS)"
                )
        if self.unit == "minutes":
            if not re.match(r"^:[0-5]\d$", time_str):
                raise ScheduleValueError(
                    "Invalid time format for a minutely job (valid format is :SS)"
                )
        time_values = time_str.split(":")
        hour: str | int
        minute: str | int
        second: str | int
        if len(time_values) == 3:
            hour, minute, second = time_values
        elif len(time_values) == 2 and self.unit == "minutes":
            hour = 0
            minute = 0
            _, second = time_values
        elif len(time_values) == 2 and self.unit == "hours" and len(time_values[0]):
            hour = 0
            minute, second = time_values
        else:
            hour, minute = time_values
            second = 0
        if self.unit == "days" or self.start_day:
            hour = int(hour)
            if not (0 <= hour <= 23):
                raise ScheduleValueError(
                    "Invalid number of hours ({} is not between 0 and 23)"
                )
        elif self.unit == "hours":
            hour = 0
        elif self.unit == "minutes":
            hour = 0
            minute = 0
        hour = int(hour)
        minute = int(minute)
        second = int(second)
        self.at_time = datetime.time(hour, minute, second)
        return self

    def to(self, latest):
        self.latest = latest
        return self

    def until(
        self, until_time: datetime.datetime | datetime.timedelta | datetime.time | str
    ):
        if isinstance(until_time, datetime.datetime):
            self.cancel_after = until_time
        elif isinstance(until_time, datetime.timedelta):
            self.cancel_after = datetime.datetime.now() + until_time
        elif isinstance(until_time, datetime.time):
            self.cancel_after = datetime.datetime.combine(
                datetime.datetime.now(), until_time
            )
        elif isinstance(until_time, str):
            cancel_after = self._decode_datetimestr(
                until_time,
                [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y-%m-%d",
                    "%H:%M:%S",
                    "%H:%M",
                ],
            )
            if cancel_after is None:
                raise ScheduleValueError("Invalid string format for until()")
            if "-" not in until_time:
                now = datetime.datetime.now()
                cancel_after = cancel_after.replace(
                    year=now.year, month=now.month, day=now.day
                )
            self.cancel_after = cancel_after
        else:
            raise TypeError(
                "until() takes a string, datetime.datetime, datetime.timedelta, datetime.time parameter"
            )
        if self.cancel_after < datetime.datetime.now():
            raise ScheduleValueError(
                "Cannot schedule a job to run until a time in the past"
            )
        return self

    def do(self, job_func: Callable, *args, **kwargs):
        self.job_func = functools.partial(job_func, *args, **kwargs)
        functools.update_wrapper(self.job_func, job_func)
        self._schedule_next_run()
        if self.scheduler is None:
            raise ScheduleError(
                "Unable to a add job to schedule. Job is not associated with an scheduler"
            )
        self.scheduler.jobs.append(self)
        return self

    @property
    def should_run(self) -> bool:
        assert self.next_run is not None, "must run _schedule_next_run before"
        return datetime.datetime.now() >= self.next_run

    async def run(self):
        if self._is_overdue(datetime.datetime.now()):
            logger.info("Cancelling job %s", self)
            return CancelJob
        logger.info("Running job %s", self)
        ret = await self.job_func()
        if isinstance(ret, CancelJob) or ret is CancelJob:
            self.scheduler.cancel_job(self)
            return ret
        self.last_run = datetime.datetime.now()
        self._schedule_next_run()
        if self._is_overdue(self.next_run):
            logger.info("Cancelling job %s", self)
            return CancelJob
        return ret

    def _schedule_next_run(self) -> None:
        if self.unit not in ("seconds", "minutes", "hours", "days", "weeks"):
            raise ScheduleValueError(
                "Invalid unit (valid units are `seconds`, `minutes`, `hours`, `days`, and `weeks`)",
            )
        if self.latest is not None:
            if not (self.latest >= self.interval):
                raise ScheduleError("`latest` is greater than `interval`")
            interval = random.randint(self.interval, self.latest)
        else:
            interval = self.interval
        now = datetime.datetime.now(self.at_time_zone)
        next_run = now
        if self.start_day is not None:
            if self.unit != "weeks":
                raise ScheduleValueError("`unit` should be 'weeks'")
            next_run = _move_to_next_weekday(next_run, self.start_day)
        if self.at_time is not None:
            next_run = self._move_to_at_time(next_run)
        period = datetime.timedelta(**{self.unit: interval})
        if interval != 1:
            next_run += period
        while next_run <= now:
            next_run += period
        next_run = self._correct_utc_offset(
            next_run, fixate_time=(self.at_time is not None)
        )
        if self.at_time_zone is not None:
            next_run = next_run.astimezone()
            next_run = next_run.replace(tzinfo=None)
        self.next_run = next_run

    def _move_to_at_time(self, moment: datetime.datetime) -> datetime.datetime:
        if self.at_time is None:
            return moment
        kwargs = {"second": self.at_time.second, "microsecond": 0}
        if self.unit == "days" or self.start_day is not None:
            kwargs["hour"] = self.at_time.hour
        if self.unit in ["days", "hours"] or self.start_day is not None:
            kwargs["minute"] = self.at_time.minute
        moment = moment.replace(**kwargs)
        moment = self._correct_utc_offset(moment, fixate_time=True)
        return moment

    def _correct_utc_offset(
        self, moment: datetime.datetime, fixate_time: bool
    ) -> datetime.datetime:
        if self.at_time_zone is None:
            return moment
        offset_before_normalize = moment.utcoffset()
        moment = self.at_time_zone.normalize(moment)
        offset_after_normalize = moment.utcoffset()
        if offset_before_normalize == offset_after_normalize:
            return moment
        if not fixate_time:
            return moment
        offset_diff = offset_after_normalize - offset_before_normalize
        moment -= offset_diff
        re_normalized_offset = self.at_time_zone.normalize(moment).utcoffset()
        if re_normalized_offset != offset_after_normalize:
            moment += offset_diff
        return moment

    def _is_overdue(self, when: datetime.datetime):
        return self.cancel_after is not None and when > self.cancel_after

    def _decode_datetimestr(
        self, datetime_str: str, formats: list[str]
    ) -> None | datetime.datetime:
        for f in formats:
            try:
                return datetime.datetime.strptime(datetime_str, f)
            except ValueError:
                pass
        return None


default_scheduler = Scheduler()
jobs = default_scheduler.jobs


def every(interval: int = 1) -> Job:
    return default_scheduler.every(interval)


async def run_pending() -> None:
    await default_scheduler.run_pending()


async def run_all(delay_seconds: int = 0) -> None:
    await default_scheduler.run_all(delay_seconds=delay_seconds)


def get_jobs(tag: None | Hashable = None) -> list[Job]:
    return default_scheduler.get_jobs(tag)


def clear(tag: None | Hashable = None) -> None:
    default_scheduler.clear(tag)


def cancel_job(job: Job) -> None:
    default_scheduler.cancel_job(job)


def next_run(tag: None | Hashable = None) -> None | datetime.datetime:
    return default_scheduler.get_next_run(tag)


def idle_seconds() -> None | float:
    return default_scheduler.idle_seconds


def repeat(job, *args, **kwargs):
    def _schedule_decorator(decorated_function):
        job.do(decorated_function, *args, **kwargs)
        return decorated_function

    return _schedule_decorator


def _move_to_next_weekday(moment: datetime.datetime, weekday: str):
    weekday_index = _weekday_index(weekday)
    days_ahead = weekday_index - moment.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return moment + datetime.timedelta(days=days_ahead)


def _weekday_index(day: str) -> int:
    weekdays = (
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    )
    if day not in weekdays:
        raise ScheduleValueError(f"Invalid start day (valid start days are {weekdays})")
    return weekdays.index(day)
