"""Offline tests for substack_surfacer. All network calls are faked; run with: pytest"""

import csv
import json
import os
import sys
from datetime import datetime, timezone

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import substack_surfacer as ss  # noqa: E402


# --- helpers -----------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text='', headers=None):
        self.status_code = status_code
        self._json = json_data
        self.text = text if text else (json.dumps(json_data) if json_data is not None else '')
        self.headers = headers or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def archive_post(post_id, date, likes=5, **extra):
    post = {
        'id': post_id,
        'title': f'Post {post_id}',
        'subtitle': 'sub',
        'post_date': date,
        'reaction_count': likes,
        'wordcount': 1000,
        'audience': 'everyone',
        'canonical_url': f'https://example.substack.com/p/post-{post_id}',
        'publishedBylines': [{'name': 'Ada'}],
    }
    post.update(extra)
    return post


class FakeSubstack:
    """Routes fake GETs: archive pages (by cursor) and homepages (for subscriber counts)."""

    def __init__(self, archives=None, homepages=None, failures=None):
        self.archives = archives or {}    # base url -> list of pages (each a list of posts)
        self.homepages = homepages or {}  # base url -> html
        self.failures = failures or {}    # base url -> status code
        self.calls = []

    def __call__(self, url, params=None, headers=None, **kwargs):
        self.calls.append((url, dict(params or {})))
        base = url.split('/api/')[0].rstrip('/')
        if base in self.failures:
            return FakeResponse(self.failures[base])
        if url.endswith('/api/v1/archive'):
            pages = self.archives.get(base, [[]])
            index = int((params or {}).get('cursor', 0))
            next_cursor = str(index + 1) if index + 1 < len(pages) else None
            return FakeResponse(200, {'posts': pages[index], 'nextCursor': next_cursor})
        return FakeResponse(200, text=self.homepages.get(base, ''))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(ss.time, 'sleep', lambda seconds: None)


# --- parsing helpers ---------------------------------------------------------

@pytest.mark.parametrize('text, expected', [
    ('2026-06-26', utc(2026, 6, 26)),
    ('2026-06-26 12:30:00', utc(2026, 6, 26, 12, 30)),
    ('2026-06-26T12:30:00.000Z', utc(2026, 6, 26, 12, 30)),
    ('2026/06/26', utc(2026, 6, 26)),
])
def test_parse_datetime(text, expected):
    assert ss.parse_datetime(text) == expected


def test_parse_datetime_invalid():
    assert ss.parse_datetime('not a date') is None
    with pytest.raises(ValueError):
        ss.parse_datetime('not a date', raise_on_error=True)


@pytest.mark.parametrize('count, expected', [
    ('1,234', 1234), ('3K+', 3000), ('116K+', 116000), ('1M+', 1_000_000),
    ('47', 47), ('UNKNOWN', None), ('Rate limited', None), ('', None), (None, None),
])
def test_parse_subscriber_count(count, expected):
    assert ss.parse_subscriber_count(count) == expected


@pytest.mark.parametrize('likes, count, expected', [
    (30, '3K+', 1.0), (5, '1,000', 0.5), (5, '0', 'UNKNOWN'),
    (None, '1,000', 'UNKNOWN'), ('UNKNOWN', '1,000', 'UNKNOWN'), (5, 'Error: boom', 'UNKNOWN'),
])
def test_calculate_engagement_rate(likes, count, expected):
    assert ss.calculate_engagement_rate(likes, count) == expected


@pytest.mark.parametrize('url, expected', [
    ('https://foo.substack.com/', 'https://foo.substack.com'),
    ('https://substack.com/@tuckerwalsh', 'https://tuckerwalsh.substack.com'),
    ('https://www.example.com/p/some-post', 'https://www.example.com'),
])
def test_normalize_substack_url(url, expected):
    assert ss.normalize_substack_url(url) == expected


@pytest.mark.parametrize('url, expected', [
    ('https://foo.substack.com', 'foo'),
    ('https://www.dwarkesh.com', 'www.dwarkesh.com'),
])
def test_extract_newsletter_name_from_url(url, expected):
    assert ss.extract_newsletter_name_from_url(url) == expected


def test_load_urls_from_file(tmp_path):
    path = tmp_path / 'urls.txt'
    path.write_text('# comment\nhttps://a.substack.com\tbigwigs\nhttps://b.substack.com\n\nnot-a-url\n')
    assert ss.load_urls_from_file(str(path)) == [
        {'url': 'https://a.substack.com', 'category': 'bigwigs'},
        {'url': 'https://b.substack.com', 'category': ''},
    ]


# --- profile -----------------------------------------------------------------

def profile_html(subscriptions):
    data = json.dumps({'profile': {'subscriptions': subscriptions}})
    return f'<html><script id="preloads-json" type="application/json">{data}</script></html>'


def test_get_newsletters_from_profile(monkeypatch):
    html = profile_html([
        {'publication': {'subdomain': 'foo', 'custom_domain': None}},
        {'publication': {'subdomain': 'bar', 'custom_domain': 'www.bar.com'}},
        {'publication': None},
    ])
    monkeypatch.setattr(ss.requests, 'get', lambda *a, **k: FakeResponse(200, text=html))
    assert ss.get_newsletters_from_profile('https://substack.com/@x') == [
        'https://foo.substack.com', 'https://www.bar.com']


def test_get_newsletters_from_profile_unrecognised_page(monkeypatch):
    monkeypatch.setattr(ss.requests, 'get', lambda *a, **k: FakeResponse(200, text='<html></html>'))
    assert ss.get_newsletters_from_profile('https://substack.com/@x') == []


# --- subscriber count ----------------------------------------------------------

@pytest.mark.parametrize('html, expected', [
    ('{"freeSubscriberCount":"12,000"}', '12,000'),
    (r'{\"freeSubscriberCount\":\"12,000\"}', '12,000'),
    ('{"freeSubscriberCount":null,"freeSubscriberCountOrderOfMagnitude":"3K+"}', '3K+'),
    ('{"freeSubscriberCount":null}', 'UNKNOWN'),
])
def test_get_free_subscriber_count(monkeypatch, html, expected):
    monkeypatch.setattr(ss, 'http_get', lambda *a, **k: FakeResponse(200, text=html))
    assert ss.get_free_subscriber_count('https://foo.substack.com') == expected


def test_get_free_subscriber_count_failure_is_unknown(monkeypatch):
    monkeypatch.setattr(ss, 'http_get', lambda *a, **k: FakeResponse(404))
    assert ss.get_free_subscriber_count('https://foo.substack.com') == 'UNKNOWN'


# --- retries -----------------------------------------------------------------

def sequence(*outcomes):
    """A callable that raises/returns each outcome in turn, counting calls."""
    state = {'calls': 0}

    def func():
        outcome = outcomes[min(state['calls'], len(outcomes) - 1)]
        state['calls'] += 1
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, FakeResponse):
            outcome.raise_for_status()
        return outcome
    return func, state


def test_retry_recovers_from_429_using_retry_after(monkeypatch):
    waits = []
    monkeypatch.setattr(ss.time, 'sleep', waits.append)
    func, state = sequence(FakeResponse(429, headers={'Retry-After': '7'}), 'ok')
    assert ss.retry_with_backoff(func) == 'ok'
    assert waits == [7]


def test_retry_gives_up_on_persistent_429():
    func, state = sequence(FakeResponse(429))
    assert ss.retry_with_backoff(func, max_retries=3) == 'Rate limited'
    assert state['calls'] == 3


def test_retry_recovers_from_5xx_and_timeouts():
    func, state = sequence(FakeResponse(503), requests.Timeout('slow'), 'ok')
    assert ss.retry_with_backoff(func) == 'ok'


def test_retry_gives_up_on_persistent_5xx():
    func, state = sequence(FakeResponse(503))
    assert ss.retry_with_backoff(func).startswith('Error:')
    assert state['calls'] == ss.TRANSIENT_MAX_RETRIES


def test_retry_does_not_retry_404():
    func, state = sequence(FakeResponse(404), 'ok')
    assert ss.retry_with_backoff(func).startswith('Error:')
    assert state['calls'] == 1


# --- archive + filtering -----------------------------------------------------------

BASE = 'https://example.substack.com'
HOME = '{"freeSubscriberCountOrderOfMagnitude":"1K+"}'


def test_fetch_archive_posts_follows_cursor_and_stops_at_from_date(monkeypatch):
    fake = FakeSubstack(archives={BASE: [
        [archive_post(1, '2026-11-10T00:00:00Z'), archive_post(2, '2026-09-01T00:00:00Z')],
        [archive_post(3, '2026-07-01T00:00:00Z'), archive_post(4, '2026-05-01T00:00:00Z')],
        [archive_post(5, '2026-01-01T00:00:00Z')],  # never requested
    ]})
    monkeypatch.setattr(ss, 'http_get', fake)
    posts = ss.fetch_archive_posts(BASE, utc(2026, 6, 26))
    assert [p['id'] for p in posts] == [1, 2, 3, 4]
    assert len(fake.calls) == 2


def test_fetch_archive_posts_raises_on_failure(monkeypatch):
    monkeypatch.setattr(ss, 'http_get', FakeSubstack(failures={BASE: 404}))
    with pytest.raises(RuntimeError):
        ss.fetch_archive_posts(BASE, utc(2026, 6, 26))


def test_fetch_newsletter_posts_filters_window_with_exclusive_end(monkeypatch):
    fake = FakeSubstack(
        archives={BASE: [[
            archive_post(1, '2026-11-15T00:00:00Z'),        # == exclusive end -> excluded
            archive_post(2, '2026-11-14T23:59:59Z', likes=30),
            archive_post(3, '2026-06-26T00:00:00Z', audience='only_paid'),
            archive_post(4, '2026-06-25T23:59:59Z'),        # before window
        ]]},
        homepages={BASE: HOME},
    )
    monkeypatch.setattr(ss, 'http_get', fake)
    posts = ss.fetch_newsletter_posts(BASE, utc(2026, 6, 26), utc(2026, 11, 15), category='tm')
    assert [p['post_id'] for p in posts] == [2, 3]
    first = posts[0]
    assert first['free_subscriber_count'] == '1K+'
    assert first['likes_per_100_free_subscribers'] == 3.0
    assert first['author'] == 'Ada'
    assert first['category'] == 'tm'
    assert first['post_date'] == '2026-11-14 23:59:59'
    assert [p['is_paid'] for p in posts] == [False, True]


def test_subscriber_count_only_fetched_when_there_are_posts(monkeypatch):
    fake = FakeSubstack(archives={BASE: [[archive_post(1, '2026-01-01T00:00:00Z')]]},
                        homepages={BASE: HOME})
    monkeypatch.setattr(ss, 'http_get', fake)
    assert ss.fetch_newsletter_posts(BASE, utc(2026, 6, 26), utc(2026, 11, 15)) == []
    assert all(url.endswith('/api/v1/archive') for url, _ in fake.calls)


def test_max_posts_keeps_newest_without_failing(monkeypatch):
    posts = [archive_post(i, f'2026-07-{20 - i:02d}T00:00:00Z') for i in range(1, 6)]
    monkeypatch.setattr(ss, 'http_get', FakeSubstack(archives={BASE: [posts]}, homepages={BASE: HOME}))
    result = ss.fetch_newsletter_posts(BASE, utc(2026, 6, 26), utc(2026, 11, 15), max_posts=2)
    assert [p['post_id'] for p in result] == [1, 2]


def test_missing_byline_falls_back_to_newsletter_name(monkeypatch):
    post = archive_post(1, '2026-07-01T00:00:00Z', publishedBylines=[])
    monkeypatch.setattr(ss, 'http_get', FakeSubstack(archives={BASE: [[post]]}, homepages={BASE: HOME}))
    [result] = ss.fetch_newsletter_posts(BASE, utc(2026, 6, 26), utc(2026, 11, 15))
    assert result['author'] == 'example'


# --- CSV writing ---------------------------------------------------------------

def read_csv(path):
    with open(path, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def test_save_posts_to_csv_round_trip(tmp_path):
    row = {name: 'x' for name in ['newsletter_name', 'newsletter_url', 'category', 'free_subscriber_count',
                                  'likes', 'likes_per_100_free_subscribers', 'post_url', 'author',
                                  'post_title', 'post_subtitle', 'post_date', 'word_count', 'is_paid', 'post_id']}
    out = tmp_path / 'sub' / 'out.csv'
    ss.save_posts_to_csv([row, row], str(out))
    assert len(read_csv(out)) == 2
    ss.save_posts_to_csv([], str(out))
    assert read_csv(out) == []
    assert out.exists()


def test_save_posts_to_csv_is_atomic_on_crash(tmp_path, monkeypatch):
    out = tmp_path / 'out.csv'
    ss.save_posts_to_csv([], str(out))
    original = out.read_text()

    def crash(self, row):
        raise KeyboardInterrupt('simulated crash')
    monkeypatch.setattr(csv.DictWriter, 'writerow', crash)
    with pytest.raises(KeyboardInterrupt):
        ss.save_posts_to_csv([{'post_id': 1}], str(out))
    assert out.read_text() == original
    assert os.listdir(tmp_path) == ['out.csv']

    with pytest.raises(KeyboardInterrupt):
        ss.save_posts_to_csv([{'post_id': 1}], str(tmp_path / 'new.csv'))
    assert not (tmp_path / 'new.csv').exists()


# --- end to end (main) -----------------------------------------------------------

A, B, DEAD = 'https://a.substack.com', 'https://b.substack.com', 'https://dead.example.com'


def run_main(monkeypatch, tmp_path, fake, *args):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ss, 'http_get', fake)
    (tmp_path / 'urls.txt').write_text(f'{A}\ttm\n{B}\n{DEAD}\n')
    monkeypatch.setattr(sys, 'argv', ['substack_surfacer.py', '--urls', 'urls.txt', *args])
    ss.main()


def make_fake():
    return FakeSubstack(
        archives={
            A: [[archive_post(1, '2026-11-14T10:00:00Z'), archive_post(2, '2026-07-01T00:00:00Z')]],
            B: [[archive_post(3, '2026-01-01T00:00:00Z')]],
        },
        homepages={A: HOME, B: HOME},
        failures={DEAD: 404},
    )


def test_main_end_to_end_and_resume(monkeypatch, tmp_path, capsys):
    fake = make_fake()
    run_main(monkeypatch, tmp_path, fake, '--from', '2026-06-26', '--to', '2026-11-14', '--workers', '3')

    window = tmp_path / 'substacks' / '2026-06-26_to_2026-11-14'
    assert sorted(os.listdir(window)) == ['a.csv', 'b.csv']   # failed newsletter gets no file
    assert [r['post_id'] for r in read_csv(window / 'a.csv')] == ['1', '2']
    assert read_csv(window / 'b.csv') == []
    combined = read_csv(tmp_path / 'combined.csv')
    assert [r['post_id'] for r in combined] == ['1', '2']
    assert combined[0]['category'] == 'tm'
    assert 'Failed newsletters (re-run to retry): 1' in capsys.readouterr().out

    # Resume: finished newsletters are skipped, the failed one is retried
    fake.calls.clear()
    run_main(monkeypatch, tmp_path, fake, '--from', '2026-06-26', '--to', '2026-11-14')
    requested = {url.split('/api/')[0] for url, _ in fake.calls}
    assert requested == {DEAD}
    assert [r['post_id'] for r in read_csv(tmp_path / 'combined.csv')] == ['1', '2']


def test_main_different_window_does_not_reuse_results(monkeypatch, tmp_path):
    fake = make_fake()
    run_main(monkeypatch, tmp_path, fake, '--from', '2026-06-26', '--to', '2026-11-14')
    fake.calls.clear()
    run_main(monkeypatch, tmp_path, fake, '--from', '2026-07-02', '--to', '2026-11-14')
    assert {url.split('/api/')[0] for url, _ in fake.calls} >= {A, B}
    assert [r['post_id'] for r in read_csv(tmp_path / 'combined.csv')] == ['1']
    assert sorted(os.listdir(tmp_path / 'substacks')) == ['2026-06-26_to_2026-11-14', '2026-07-02_to_2026-11-14']


def test_main_to_with_time_is_exact_cutoff(monkeypatch, tmp_path):
    run_main(monkeypatch, tmp_path, make_fake(), '--from', '2026-06-26', '--to', '2026-11-14 09:00:00')
    assert [r['post_id'] for r in read_csv(tmp_path / 'combined.csv')] == ['2']
