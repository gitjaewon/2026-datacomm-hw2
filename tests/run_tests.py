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


def java(main, *args, **kwargs):
    return run('java', '-cp', str(BUILD), main, *map(str, args), **kwargs)


run('javac', '--release', '17', '-encoding', 'UTF-8', '-d', str(BUILD),
    *map(str, (ROOT / 'src').glob('*.java')), str(ROOT / 'tests' / 'RegressionTest.java'))
print(java('RegressionTest', RUN / 'regression').strip())

for main in ('Server', 'ClientMain'):
    for invalid in (('--requests', '0'), ('--port', '65536'), ('--port', '0')):
        java(main, '--host', '127.0.0.1', '--port', '5000', *invalid, success=False)
java('Verify', '--requests', success=False)
print('Invalid command-line arguments: PASS')


def integration(name, requests, interval_args):
    directory = RUN / name
    directory.mkdir()
    with socket.socket() as port_socket:
        port_socket.bind(('127.0.0.1', 0))
        port = port_socket.getsockname()[1]
    with (directory / 'server-console.txt').open('w', encoding='utf-8') as output:
        server = subprocess.Popen(['java', '-cp', str(BUILD), 'Server', '--host', '127.0.0.1',
                                   '--port', str(port), '--requests', str(requests), '--log-dir', str(directory)],
                                  cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        try:
            client_output = java('ClientMain', '--host', '127.0.0.1', '--port', port, '--requests', requests,
                                 '--log-dir', directory, *interval_args)
            server.wait(timeout=20)
            if server.returncode != 0:
                raise AssertionError((directory / 'server-console.txt').read_text(encoding='utf-8'))
            verify = java('Verify', '--log-dir', directory, '--requests', requests)
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

mutations = {
    'missing-final-held': ('Client1.txt', lambda s: re.sub(r'final_held=\[[^]]*\]', '', s)),
    'missing-metrics': ('Server.txt', lambda s: '\n'.join(line for line in s.splitlines() if 'Metrics:' not in line)),
    'missing-final-report': ('Server.txt', lambda s: '\n'.join(line for line in s.splitlines() if 'Final seat map' not in line)),
    'deadlock': ('Server.txt', lambda s: s.replace('deadlock=0', 'deadlock=1')),
    'missing-notify-accounting': ('Client1.txt', lambda s: re.sub(r'notified=\d+', '', s)),
    'invalid-held-seat': ('Client1.txt', lambda s: re.sub(r'final_held=\[[^]]*\]', 'final_held=[101]', s)),
    'missing-server-bye': ('Server.txt', lambda s: '\n'.join(line for line in s.splitlines() if 'Graceful shutdown.' not in line)),
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
