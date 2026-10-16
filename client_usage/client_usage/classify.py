"""Name the client software behind a request.

Only the product and its version are kept, never the raw User-Agent: the full string is detailed
enough to help single out an individual, and the product is all a usage report needs.
"""
import re

#: Longest version string stored; anything longer is truncated.
MAX_VERSION_LENGTH = 32

# (pattern matched against the start of the User-Agent, client name). The first group, if any, is
# the version. Order matters: ONE's own agent must win over the requests library it runs on.
USER_AGENTS = (
    (r'ONE/(\S+)', 'ONE'),
    (r'python-requests/(\S+)', 'python-requests'),
    (r'python-httpx/(\S+)', 'python-httpx'),
    (r'Python-urllib/(\S+)', 'python-urllib'),
    (r'Python/\S+ aiohttp/(\S+)', 'aiohttp'),
    (r'curl/(\S+)', 'curl'),
    (r'Wget/(\S+)', 'wget'),
    (r'MATLAB (\S+)', 'matlab'),
    (r'node-fetch(?:/(\S+))?', 'node-fetch'),
    (r'axios/(\S+)', 'axios'),
    (r'Go-http-client/(\S+)', 'go'),
    (r'Java/(\S+)', 'java'),
    (r'.*\br-curl/(\S+)', 'r-curl'),
    (r'Mozilla/', 'browser'),
)
_USER_AGENTS = tuple((re.compile(p), name) for p, name in USER_AGENTS)
_VERSION = re.compile(r'\d+(?:\.\d+)*')


def classify(headers):
    """Return (client, version) for a request's headers; version may be ''.

    ONE 3.6.0 sends its version in a header but identifies itself to the server only as the
    requests library, so earlier ONE releases are indistinguishable from other scripts using
    requests and are recorded as 'python-requests'.
    """
    if version := headers.get('ONE-API-Version', ''):
        match = _VERSION.match(version.strip())
        return 'ONE', match.group(0)[:MAX_VERSION_LENGTH] if match else ''
    agent = headers.get('User-Agent', '')
    for pattern, name in _USER_AGENTS:
        if match := pattern.match(agent):
            version = (match.group(1) or '') if match.groups() else ''
            return name, version[:MAX_VERSION_LENGTH]
    return ('other', '') if agent else ('none', '')
