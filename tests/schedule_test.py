from context import schedule

"""Unit tests for schedule.py"""
import datetime
import functools
import mock
import unittest

# Silence "missing docstring", "method could be a function",
# "class already defined", and "too many public methods" messages:
# pylint: disable-msg=R0201,C0111,E0102,R0904,R0901


def make_mock_job(name=None):
    job = mock.Mock()
    job.__name__ = name or 'job'
    return job


class mock_datetime(object):
    """
    Monkey-patch datetime for predictable results
    """
    def __init__(self, year, month, day, hour, minute):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute

    def __enter__(self):
        class MockDate(datetime.datetime):
            @classmethod
            def today(cls):
                return cls(self.year, self.month, self.day)

            @classmethod
            def now(cls):
                return cls(self.year, self.month, self.day,
                           self.hour, self.minute)
        self.original_datetime = datetime.datetime
        datetime.datetime = MockDate

    def __exit__(self, *args, **kwargs):
        datetime.datetime = self.original_datetime


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        schedule.clear()

    def test_time_units(self):
        assert schedule.every().seconds.unit == 'seconds'
        assert schedule.every().minutes.unit == 'minutes'
        assert schedule.every().hours.unit == 'hours'
        assert schedule.every().days.unit == 'days'
        assert schedule.every().weeks.unit == 'weeks'

    def test_singular_time_units_match_plural_units(self):
        assert schedule.every().second.unit == schedule.every().seconds.unit
        assert schedule.every().minute.unit == schedule.every().minutes.unit
        assert schedule.every().hour.unit == schedule.every().hours.unit
        assert schedule.every().day.unit == schedule.every().days.unit
        assert schedule.every().week.unit == schedule.every().weeks.unit

    def test_at_time(self):
        mock_job = make_mock_job()
        assert schedule.every().day.at('10:30').do(mock_job).next_run.hour == 10
        assert schedule.every().day.at('10:30').do(mock_job).next_run.minute == 30

    def test_next_run_time(self):
        with mock_datetime(2010, 1, 6, 12, 15):
            mock_job = make_mock_job()
            assert schedule.next_run() is None
            assert schedule.every().minute.do(mock_job).next_run.minute == 16
            assert schedule.every(5).minutes.do(mock_job).next_run.minute == 20
            assert schedule.every().hour.do(mock_job).next_run.hour == 13
            assert schedule.every().day.do(mock_job).next_run.day == 7
            assert schedule.every().day.at('09:00').do(mock_job).next_run.day == 7
            assert schedule.every().day.at('12:30').do(mock_job).next_run.day == 6
            assert schedule.every().week.do(mock_job).next_run.day == 13

    def test_run_all(self):
        mock_job = make_mock_job()
        schedule.every().minute.do(mock_job)
        schedule.every().hour.do(mock_job)
        schedule.every().day.at('11:00').do(mock_job)
        schedule.run_all()
        assert mock_job.call_count == 3

    def test_job_func_args_are_passed_on(self):
        mock_job = make_mock_job()
        schedule.every().second.do(mock_job, 1, 2, 'three', foo=23, bar={})
        schedule.run_all()
        mock_job.assert_called_once_with(1, 2, 'three', foo=23, bar={})

    def test_to_string(self):
        def job_fun():
            pass
        s = str(schedule.every().minute.do(job_fun, 'foo', bar=23))
        assert 'job_fun' in s
        assert 'foo' in s
        assert 'bar=23' in s

    def test_to_string_lambda_job_func(self):
        assert len(str(schedule.every().minute.do(lambda: 1))) > 1
        assert len(str(schedule.every().day.at('10:30').do(lambda: 1))) > 1

    def test_run_pending(self):
        """Check that run_pending() runs pending jobs.
        We do this by overriding datetime.datetime with mock objects
        that represent increasing system times.
        Please note that it is *intended behavior that run_pending() does not
        run missed jobs*. For example, if you've registered a job that
        should run schedule.every minute and you only call run_pending() in one hour
        increments then your job won't be run 60 times in between but
        only once.
        """
        mock_job = make_mock_job()

        with mock_datetime(2010, 1, 6, 12, 15):
            schedule.every().minute.do(mock_job)
            schedule.every().hour.do(mock_job)
            schedule.every().day.do(mock_job)
            schedule.run_pending()
            assert mock_job.call_count == 0

        with mock_datetime(2010, 1, 6, 12, 16):
            schedule.run_pending()
            assert mock_job.call_count == 1

        with mock_datetime(2010, 1, 6, 13, 16):
            mock_job.reset_mock()
            schedule.run_pending()
            assert mock_job.call_count == 2

        with mock_datetime(2010, 1, 7, 13, 16):
            mock_job.reset_mock()
            schedule.run_pending()
            assert mock_job.call_count == 3

    def test_run_every_n_days_at_specific_time(self):
        mock_job = make_mock_job()
        with mock_datetime(2010, 1, 6, 11, 29):
            schedule.every(2).days.at('11:30').do(mock_job)
            schedule.run_pending()
            assert mock_job.call_count == 0

        with mock_datetime(2010, 1, 6, 11, 31):
            schedule.run_pending()
            assert mock_job.call_count == 0

        with mock_datetime(2010, 1, 7, 11, 31):
            schedule.run_pending()

        with mock_datetime(2010, 1, 8, 11, 29):
            schedule.run_pending()

        with mock_datetime(2010, 1, 8, 11, 31):
            schedule.run_pending()
            assert mock_job.call_count == 1

        with mock_datetime(2010, 1, 10, 11, 31):
            schedule.run_pending()
            assert mock_job.call_count == 2

    def test_next_run_property(self):
        original_datetime = datetime.datetime
        with mock_datetime(2010, 1, 6, 13, 16):
            hourly_job = make_mock_job('hourly')
            daily_job = make_mock_job('daily')
            schedule.every().day.do(daily_job)
            schedule.every().hour.do(hourly_job)
            assert len(schedule.jobs) == 2
            # Make sure the hourly job is first
            assert schedule.next_run() == original_datetime(2010, 1, 6, 14, 16)
            assert schedule.idle_seconds() == 60 * 60

    def test_cancel_jobs(self):
        def stop_job():
            return schedule.CancelJob

        schedule.every().second.do(stop_job)
        schedule.every().second.do(stop_job)
        schedule.every().second.do(stop_job)
        assert len(schedule.jobs) == 3

    def test_tag_type_enforcement(self):
        job1 = schedule.every().second.do(make_mock_job(name='job1'))
        self.assertRaises(TypeError, job1.tag, {})
        self.assertRaises(TypeError, job1.tag, 1, 'a', [])
        job1.tag(0, 'a', True)
        assert len(job1.tags) == 3

    def test_clear_by_tag(self):
        schedule.every().second.do(make_mock_job(name='job1')).tag('tag1')
        schedule.every().second.do(make_mock_job(name='job2')).tag('tag1', 'tag2')
        schedule.every().second.do(make_mock_job(name='job3')).tag('tag3', 'tag3',
                                                          'tag3', 'tag2')
        assert len(schedule.jobs) == 3
        schedule.run_all()
        assert len(schedule.jobs) == 3
        schedule.clear('tag3')
        assert len(schedule.jobs) == 2
        schedule.clear('tag1')
        assert len(schedule.jobs) == 0
        schedule.every().second.do(make_mock_job(name='job1'))
        schedule.every().second.do(make_mock_job(name='job2'))
        schedule.every().second.do(make_mock_job(name='job3'))
        schedule.clear()
        assert len(schedule.jobs) == 0




if __name__ == "__main__":
    unittest.main()