"""Real-stack check: run after docker compose up -d --build."""
import json
import queue
import threading
import time
import urllib.error
import urllib.request

BASE = 'http://127.0.0.1:15173'
notifications = queue.Queue()
ready = threading.Event()

def listen():
    try:
        with urllib.request.urlopen(BASE + '/api/reports/events', timeout=45) as response:
            for line in response:
                line = line.decode().strip()
                if line == 'event:ready':
                    ready.set()
                if line.startswith('data:'):
                    notifications.put(line[5:].strip())
    except Exception as error:
        notifications.put(error)

threading.Thread(target=listen, daemon=True).start()
assert ready.wait(15), 'SSE connection did not open'

def post(text):
    request = urllib.request.Request(BASE + '/api/reports', data=json.dumps({'text': text}).encode(),
                                     headers={'Content-Type': 'application/json'}, method='POST')
    return urllib.request.urlopen(request, timeout=20)

try:
    post('   ')
    raise AssertionError('Blank input should be rejected')
except urllib.error.HTTPError as error:
    assert error.code == 400, error.code

with post('Hello from the event lab') as response:
    assert response.status == 202
    report_id = json.load(response)['id']

deadline = time.monotonic() + 45
while time.monotonic() < deadline:
    with urllib.request.urlopen(BASE + '/api/reports', timeout=10) as response:
        report = next(row for row in json.load(response) if row['id'] == report_id)
    if report['status'] == 'COMPLETED':
        assert report['wordCount'] == 5, report
        assert report['characterCount'] == 24, report
        break
    time.sleep(0.5)
else:
    raise AssertionError('Report did not complete; inspect backend and worker logs')

# A request publication also emits a notification. Require two notifications
# for this ID, proving that completion emitted another one.
seen = 0
while time.monotonic() < deadline and seen < 2:
    try:
        value = notifications.get(timeout=1)
    except queue.Empty:
        continue
    if isinstance(value, Exception):
        raise value
    if value == report_id:
        seen += 1
assert seen >= 2, 'Missing completion SSE notification'
print(f'PASS: input validation, HTTP 202, Kafka round trip, saved counts, SSE: {report_id}')
