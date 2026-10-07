"""JDK 17+ and Python standard library only; all outputs stay under out/tests/."""
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / 'out' / 'tests' / 'classes'
RUN = ROOT / 'out' / 'tests' / str(time.time_ns())
BUILD.mkdir(parents=True, exist_ok=True)
RUN.mkdir(parents=True)


def run(*args, timeout=60, success=True):
    result = subprocess.run(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding='utf-8', errors='replace', timeout=timeout)
    if (result.returncode == 0) != success:
        raise AssertionError(f'{args}: exit={result.returncode}\n{result.stdout}')
    return result.stdout


def java(main, *args, jvm_args=(), **kwargs):
    return run('java', *jvm_args, '-cp', str(BUILD), main, *map(str, args), **kwargs)


run('javac', '--release', '17', '-encoding', 'UTF-8', '-d', str(BUILD),
    *map(str, (ROOT / 'src').glob('*.java')), str(ROOT / 'tests' / 'RegressionTest.java'))
print(java('RegressionTest', RUN / 'regression').strip())

for main in ('Server', 'ClientMain'):
    for invalid in (('--requests', '0'), ('--port', '65536'), ('--port', '0')):
        java(main, '--host', '127.0.0.1', '--port', '5000', *invalid, success=False)
java('Verify', '--requests', success=False)
print('Invalid command-line arguments: PASS')


def integration(name, requests, interval_args, jvm_args=()):
    directory = RUN / name
    directory.mkdir()
    with socket.socket() as port_socket:
        port_socket.bind(('127.0.0.1', 0))
        port = port_socket.getsockname()[1]
    with (directory / 'server-console.txt').open('w', encoding='utf-8') as output:
        server = subprocess.Popen(['java', *jvm_args, '-cp', str(BUILD), 'Server', '--host', '127.0.0.1',
                                   '--port', str(port), '--requests', str(requests), '--log-dir', str(directory)],
                                  cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        try:
            client_output = java('ClientMain', '--host', '127.0.0.1', '--port', port, '--requests', requests,
                                 '--log-dir', directory, *interval_args, jvm_args=jvm_args)
            server.wait(timeout=20)
            if server.returncode != 0:
                raise AssertionError((directory / 'server-console.txt').read_text(encoding='utf-8'))
            verify = java('Verify', '--log-dir', directory, '--requests', requests, jvm_args=jvm_args)
            if 'Final seat integrity = PASS' not in verify:
                raise AssertionError(verify + client_output)
            print(f'{name}: 30 x {requests} requests, TCP shutdown and integrity PASS')
            return directory
        finally:
            if server.poll() is None:
                server.kill()
                server.wait(timeout=5)


integration('default-interval', 3, ())
reference = integration('stress', 200, ('--min-interval-ms', '1', '--max-interval-ms', '3'))
locale_directory = integration('german-locale', 200, ('--min-interval-ms', '1', '--max-interval-ms', '3'),
                               ('-Duser.language=de', '-Duser.country=DE'))
server_text = (locale_directory / 'Server.txt').read_text(encoding='utf-8')
report_text = (locale_directory / 'VerifyResult.txt').read_text(encoding='utf-8')
throughput = re.search(r'throughput=(\d+\.\d+) ', server_text).group(1)
if f': {throughput} req/s' not in report_text:
    raise AssertionError('Locale-dependent throughput parsing')
if not re.search(r'\(waited \d+\.\d{3}s\)', server_text):
    raise AssertionError('Locale-dependent NOTIFY wait time')
weighted_response = responded_total = 0
for client_file in locale_directory.glob('Client*.txt'):
    summary = client_file.read_text(encoding='utf-8').splitlines()[-1]
    average = float(re.search(r'avg_resp_ms=(\d+\.\d+)', summary).group(1))
    responded = int(re.search(r'responded=(\d+)', summary).group(1))
    weighted_response += average * responded
    responded_total += responded
reported_average = float(re.search(r'avg response time\s+: (\d+\.\d+) ms', report_text).group(1))
if abs(reported_average - weighted_response / responded_total) > 0.051:
    raise AssertionError('Locale-dependent response time parsing')
print('Decimal metrics preserved under German JVM locale: PASS')

mutations = {
    'missing-final-held': ('Client1.txt', lambda s: re.sub(r'final_held=\[[^]]*\]', '', s)),
    'missing-metrics': ('Server.txt', lambda s: '\n'.join(line for line in s.splitlines() if 'Metrics:' not in line)),
    'missing-final-report': ('Server.txt', lambda s: '\n'.join(line for line in s.splitlines() if 'Final seat map' not in line)),
    'deadlock': ('Server.txt', lambda s: s.replace('deadlock=0', 'deadlock=1')),
    'missing-notify-accounting': ('Client1.txt', lambda s: re.sub(r'notified=\d+', '', s)),
    'invalid-held-seat': ('Client1.txt', lambda s: re.sub(r'final_held=\[[^]]*\]', 'final_held=[101]', s)),
    'missing-server-bye': ('Server.txt', lambda s: '\n'.join(line for line in s.splitlines() if 'Graceful shutdown.' not in line)),
    'fractional-double-booking': ('Server.txt', lambda s: s.replace('double_booking=0', 'double_booking=0.5')),
    'decimal-integer-counter': ('Server.txt', lambda s: s.replace('double_booking=0', 'double_booking=0.0')),
    'malformed-double-booking': ('Server.txt', lambda s: s.replace('double_booking=0', 'double_booking=0INVALID')),
    'overflow-counter': ('Server.txt', lambda s: s.replace('double_booking=0', 'double_booking=9223372036854775808')),
    'duplicate-counter': ('Server.txt', lambda s: s.replace('double_booking=0', 'double_booking=0 double_booking=0')),
    'fractional-client-quota': ('Client1.txt', lambda s: s.replace('sent=200 ', 'sent=200.5 ').replace('responded=200 ', 'responded=200.5 ')),
    'wrapped-client-quota': ('Client1.txt', lambda s: s.replace('sent=200 ', 'sent=4294967496 ').replace('responded=200 ', 'responded=4294967496 ')),
    'malformed-last-counter': ('Client1.txt', lambda s: s.replace('protocol_errors=0.', 'protocol_errors=0INVALID.')),
    'comma-decimal-metric': ('Server.txt', lambda s: re.sub(r'throughput=(\d+)\.(\d+)', r'throughput=\1,\2', s)),
    'malformed-decimal-metric': ('Server.txt', lambda s: re.sub(r'throughput=\S+', 'throughput=10INVALID', s)),
    'invalid-owner-suffix': ('Server.txt', lambda s: re.sub(r'(\d+=(?:EMPTY|Client\d+))(?=\s|$)', r'\1INVALID', s, count=1)),
    'negative-seat-number': ('Server.txt', lambda s: s.replace('Final seat map [1-10]: 1=', 'Final seat map [1-10]: -1=')),
    'overflow-seat-number': ('Server.txt', lambda s: s.replace('Final seat map [1-10]: 1=', 'Final seat map [1-10]: 999999999999999999=')),
    'invalid-final-held-suffix': ('Client1.txt', lambda s: re.sub(r'(final_held=\[[^]]*\])', r'\1INVALID', s)),
}
for name, (filename, change) in mutations.items():
    directory = RUN / ('verify-' + name)
    directory.mkdir()
    for source in reference.glob('*.txt'):
        shutil.copyfile(source, directory / source.name)
    target = directory / filename
    target.write_text(change(target.read_text(encoding='utf-8')), encoding='utf-8')
    result = java('Verify', '--log-dir', directory, '--requests', '200', success=False)
    if 'Final seat integrity = FAIL' not in result:
        raise AssertionError(name + ': missing FAIL report\n' + result)
print(f'Corrupted/incomplete log cases correctly rejected: {len(mutations)}')
print('All tests passed. Logs:', RUN.relative_to(ROOT))
