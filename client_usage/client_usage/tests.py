import datetime
from io import StringIO
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.management import call_command
from django.http import HttpResponse
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from rest_framework.authtoken.models import Token

from client_usage.classify import classify
from client_usage import middleware
from client_usage.middleware import NOTICE_HEADER, OUTDATED_STATUS, is_outdated
from client_usage.models import ClientFingerprint, ClientUsage, Signups
from client_usage.router import ClientUsageRouter


class TestClassify(SimpleTestCase):

    def test_clients(self):
        cases = {
            ('3.6.0', 'python-requests/2.32.3'): ('ONE', '3.6.0'),
            ('', 'ONE/3.7.0 python-requests/2.32.3'): ('ONE', '3.7.0'),
            ('', 'python-requests/2.32.3'): ('python-requests', '2.32.3'),
            ('', 'curl/8.5.0'): ('curl', '8.5.0'),
            ('', 'Python-urllib/3.12'): ('python-urllib', '3.12'),
            ('', 'Mozilla/5.0 (X11; Linux x86_64) Firefox/131.0'): ('browser', ''),
            ('', 'SomethingElse/1.0'): ('other', ''),
            ('', ''): ('none', ''),
        }
        for (one, agent), expected in cases.items():
            with self.subTest(agent=agent, one=one):
                headers = {'User-Agent': agent}
                if one:
                    headers['ONE-API-Version'] = one
                self.assertEqual(expected, classify(headers))

    def test_version_is_sanitised(self):
        self.assertEqual(('ONE', '3.6.0'), classify({'ONE-API-Version': '3.6.0; drop table'}))
        self.assertEqual(32, len(classify({'User-Agent': 'curl/' + '1' * 100})[1]))


@override_settings(CLIENT_USAGE_FLUSH_SECONDS=0,
                   CLIENT_USAGE_SHARED_ACCOUNTS=('shared',),
                   CLIENT_USAGE_NOTICES={'shared': 'Please register your own account.'})
class TestRecording(TestCase):

    def setUp(self):
        # Discard, not flush: in a shared test process the buffer holds other tests' requests.
        middleware._counts.clear()
        middleware._fingerprints.clear()
        self.user = get_user_model().objects.create_user(username='ada', password='x')
        self.shared = get_user_model().objects.create_user(username='shared', password='x')
        self.url = reverse('subject-list')

    def _get(self, user, ip='192.0.2.1', **headers):
        token, _ = Token.objects.get_or_create(user=user)
        return self.client.get(self.url, HTTP_AUTHORIZATION=f'Token {token.key}',
                               REMOTE_ADDR=ip, **headers)

    def test_token_requests_are_counted_per_client_version(self):
        for _ in range(2):
            self._get(self.user, HTTP_ONE_API_VERSION='3.6.0', HTTP_USER_AGENT='python-requests/2')
        self._get(self.user, HTTP_USER_AGENT='curl/8.5.0')
        rows = {(r.client, r.version): r.requests
                for r in ClientUsage.objects.filter(user_id=self.user.pk)}
        self.assertEqual({('ONE', '3.6.0'): 2, ('curl', '8.5.0'): 1}, rows)
        self.assertFalse(ClientFingerprint.objects.exists(), 'only shared accounts are hashed')

    def test_anonymous_requests_are_not_counted(self):
        self.client.get(self.url)
        self.assertFalse(ClientUsage.objects.exists())

    def test_notice_goes_to_listed_accounts_only(self):
        self.assertIn('register your own', self._get(self.shared).headers[NOTICE_HEADER])
        self.assertNotIn(NOTICE_HEADER, self._get(self.user).headers)

    def test_notices_join_any_already_given(self):
        """ONE raises a notice as a warning only if the header starts with '!!!'."""
        for notice, expected in (('Please register.', 'Page capped. Please register.'),
                                 ('!!!Please register.', '!!!Please register. Page capped.')):
            with self.subTest(notice), override_settings(CLIENT_USAGE_NOTICES={'shared': notice}):
                view = mock.Mock(return_value=HttpResponse(headers={NOTICE_HEADER: 'Page capped.'}))
                request = RequestFactory().get(self.url)
                request.user = self.shared
                response = middleware.ClientUsageMiddleware(view)(request)
                self.assertEqual(expected, response[NOTICE_HEADER])

    def test_shared_account_callers_are_counted_then_compacted(self):
        for ip in ('192.0.2.1', '192.0.2.1', '192.0.2.2'):
            self._get(self.shared, ip=ip, HTTP_ONE_API_VERSION='3.6.0')
        self.assertEqual(2, ClientFingerprint.objects.count())

        # Once the day has ended, purge reduces the fingerprints to a count and deletes them.
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        ClientFingerprint.objects.update(date=yesterday)
        ClientUsage.objects.update(date=yesterday)
        call_command('client_usage', 'purge', stdout=StringIO())
        self.assertFalse(ClientFingerprint.objects.exists())
        row = ClientUsage.objects.get(user_id=self.shared.pk)
        self.assertEqual((3, 2), (row.requests, row.distinct_clients))

        out = StringIO()
        call_command('client_usage', 'report', stdout=out)
        self.assertIn('Distinct callers per day of shared account shared', out.getvalue())

    def test_purge_expires_old_records(self):
        self._get(self.user)
        ClientUsage.objects.update(date=datetime.date.today() - datetime.timedelta(days=500))
        call_command('client_usage', 'purge', '--keep-days', '400', stdout=StringIO())
        self.assertFalse(ClientUsage.objects.exists())


class TestOutdated(SimpleTestCase):

    def test_outdated_releases(self):
        cases = {
            ('', 'python-requests/2.32.3'): True,  # ONE before 3.6.0, or another script
            ('3.6.0', 'python-requests/2.32.3'): True,
            ('3.6.1', 'python-requests/2.32.3'): False,
            ('3.10.0', 'python-requests/2.32.3'): False,  # compared as numbers, not text
            ('', 'ONE/3.5.2 python-requests/2.32.3'): True,
            ('unreadable', 'python-requests/2.32.3'): False,
            ('', 'curl/8.5.0'): False,
            ('', 'Mozilla/5.0 (X11; Linux x86_64) Firefox/131.0'): False,
        }
        for (one, agent), expected in cases.items():
            with self.subTest(agent=agent, one=one):
                headers = {'User-Agent': agent}
                if one:
                    headers['ONE-API-Version'] = one
                self.assertEqual(expected, is_outdated(headers))
        with override_settings(CLIENT_USAGE_MIN_ONE_VERSION='3.7'):
            self.assertTrue(is_outdated({'ONE-API-Version': '3.6.2'}))
            self.assertFalse(is_outdated({'ONE-API-Version': '3.7.0'}))


OLD = {'HTTP_USER_AGENT': 'python-requests/2.32.3'}
NEW = {'HTTP_USER_AGENT': 'python-requests/2.32.3', 'HTTP_ONE_API_VERSION': '3.6.1'}


@override_settings(CLIENT_USAGE_FLUSH_SECONDS=0,
                   CLIENT_USAGE_OUTDATED_RATE=1,
                   CLIENT_USAGE_OUTDATED_NOTICES={'shared': 'Register your own account.'})
class TestOutdatedClients(TestCase):

    def setUp(self):
        middleware._counts.clear()
        middleware._fingerprints.clear()
        cache.clear()  # holds the cooldowns, and isn't emptied between tests
        self.user = get_user_model().objects.create_user(username='ada', password='x')
        self.shared = get_user_model().objects.create_user(username='shared', password='x')
        self.url = reverse('subject-list')

    def _auth(self, user):
        token, _ = Token.objects.get_or_create(user=user)
        return {'HTTP_AUTHORIZATION': f'Token {token.key}'}

    def _get(self, user, **headers):
        return self.client.get(self.url, **self._auth(user), **headers)

    def _view_ran(self, method, path, **headers):
        """Pass a request straight through the middleware; return whether the view ran."""
        view = mock.Mock(return_value=HttpResponse())
        request = RequestFactory().generic(method, path, **headers)
        middleware.ClientUsageMiddleware(view)(request)
        return view.called

    def test_outdated_clients_are_told_to_update(self):
        response = self._get(self.user, **OLD)
        self.assertEqual(OUTDATED_STATUS, response.status_code)
        self.assertIn('pip install -U ONE-api', response.json()['detail'])
        self.assertEqual(200, self._get(self.user, **NEW).status_code)
        # A refused request still counts as a request by that client
        self.assertTrue(ClientUsage.objects.filter(
            user_id=self.user.pk, client='python-requests').exists())

    def test_account_message(self):
        response = self._get(self.shared, **OLD)
        self.assertEqual(OUTDATED_STATUS, response.status_code)
        self.assertEqual('Register your own account.', response.json()['detail'])

    def test_cooldown(self):
        """Each caller is refused at most once per CLIENT_USAGE_OUTDATED_COOLDOWN."""
        self.assertEqual(OUTDATED_STATUS, self._get(self.user, **OLD).status_code)
        self.assertEqual(200, self._get(self.user, **OLD).status_code)
        # Another caller of the same account, and another account, each have their own
        self.assertEqual(OUTDATED_STATUS,
                         self._get(self.user, REMOTE_ADDR='192.0.2.9', **OLD).status_code)
        self.assertEqual(OUTDATED_STATUS, self._get(self.shared, **OLD).status_code)
        cache.clear()  # as when the cooldown expires
        self.assertEqual(OUTDATED_STATUS, self._get(self.user, **OLD).status_code)

    def test_rate(self):
        with override_settings(CLIENT_USAGE_OUTDATED_RATE=0.1, CLIENT_USAGE_OUTDATED_COOLDOWN=0), \
                mock.patch('client_usage.middleware.random.random', side_effect=(0.05, 0.5)):
            self.assertEqual(OUTDATED_STATUS, self._get(self.user, **OLD).status_code)
            self.assertEqual(200, self._get(self.user, **OLD).status_code)
        with override_settings(CLIENT_USAGE_OUTDATED_RATE=0):
            self.assertEqual(200, self._get(self.user, **OLD).status_code)

    def test_refused_requests_do_no_work(self):
        self.assertFalse(self._view_ran('GET', '/subjects', **self._auth(self.user), **OLD))
        self.assertTrue(self._view_ran('GET', '/subjects', **self._auth(self.user), **NEW))

    def test_exempt_requests(self):
        """Logins, writes, invalid tokens and the cache tables are never refused."""
        auth = self._auth(self.user)
        cases = {
            'anonymous': ('GET', '/subjects', OLD),
            'login': ('POST', '/auth-token', OLD),
            'write': ('POST', '/subjects', {**auth, **OLD}),
            'invalid token': ('GET', '/subjects', {'HTTP_AUTHORIZATION': 'Token nope', **OLD}),
            'cache info': ('GET', '/cache/info/', {**auth, **OLD}),
            'cache tables': ('GET', '/cache.zip', {**auth, **OLD}),
        }
        for case, (method, path, headers) in cases.items():
            with self.subTest(case):
                self.assertTrue(self._view_ran(method, path, **headers))


@override_settings(CLIENT_USAGE_SHARED_ACCOUNTS=('shared',))
class TestSignups(TestCase):

    def setUp(self):
        self.today = datetime.date.today()
        User = get_user_model()
        self.days_ago = lambda n: datetime.datetime.combine(
            self.today - datetime.timedelta(days=n), datetime.time(12))
        User.objects.create_user('a', date_joined=self.days_ago(2), is_public_user=True)
        User.objects.create_user('b', date_joined=self.days_ago(2), is_public_user=True)
        User.objects.create_user('c', date_joined=self.days_ago(2))
        User.objects.create_user('d', date_joined=self.days_ago(0), is_public_user=True)
        # Recreated at every data release, so must not count as a signup.
        User.objects.create_user('shared', date_joined=self.days_ago(1), is_public_user=True)

    def _counts(self, days_ago):
        day = self.today - datetime.timedelta(days=days_ago)
        return {r.public: r.accounts for r in Signups.objects.filter(date=day)}

    def test_ended_days_are_recorded_once_with_zeros(self):
        call_command('client_usage', 'purge', stdout=StringIO())
        self.assertEqual({True: 2, False: 1}, self._counts(2))
        self.assertEqual({True: 0, False: 0}, self._counts(1), 'shared accounts not counted')
        self.assertEqual({}, self._counts(0), 'today has not ended')
        # The first run starts from the earliest account.
        self.assertEqual(self.today - datetime.timedelta(days=2),
                         Signups.objects.order_by('date').first().date)

        # Deleting an account later does not rewrite a recorded day.
        get_user_model().objects.filter(username='a').delete()
        call_command('client_usage', 'purge', stdout=StringIO())
        self.assertEqual({True: 2, False: 1}, self._counts(2))

    def test_only_new_days_are_added(self):
        call_command('client_usage', 'purge', stdout=StringIO())
        n = Signups.objects.count()
        call_command('client_usage', 'purge', stdout=StringIO())
        self.assertEqual(n, Signups.objects.count())

        out = StringIO()
        call_command('client_usage', 'report', '--days', '7', stdout=out)
        self.assertIn('Signups (ended days only): 2 public, 1 other', out.getvalue())


class TestRouter(SimpleTestCase):

    def test_dedicated_database_holds_only_usage(self):
        router = ClientUsageRouter()
        with override_settings(CLIENT_USAGE_DATABASE='usage'):
            self.assertTrue(router.allow_migrate('usage', 'client_usage'))
            self.assertFalse(router.allow_migrate('default', 'client_usage'))
            self.assertFalse(router.allow_migrate('usage', 'subjects'))
            self.assertIsNone(router.allow_migrate('default', 'subjects'))
            self.assertEqual('usage', router.db_for_write(ClientUsage))

    def test_default_database_needs_no_routing(self):
        router = ClientUsageRouter()
        self.assertTrue(router.allow_migrate('default', 'client_usage'))
        self.assertIsNone(router.allow_migrate('default', 'subjects'))
