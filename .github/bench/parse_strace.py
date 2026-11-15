#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Summarize a per-call strace log (strace -f -tt -T -y -e trace=fsync,fdatasync,msync).

Prints, per syscall: count, total, median, p90, max; then every fsync/fdatasync with its file and duration,
slowest first, tagged TEMP (written under the build's tmp dir, deleted after v3 conversion) or FINAL (under v3/).
Usage: python3 parse_strace.py <strace-log>
"""
import re
import statistics
import sys
from collections import defaultdict

LINE = re.compile(r'^(?P<pid>\d+)\s+(?P<ts>\S+)\s+(?P<rest>.*)$')
CALL = re.compile(r'^(?P<name>fsync|fdatasync|msync)\((?P<args>.*)$')
RESUMED = re.compile(r'^<\.\.\. (?P<name>fsync|fdatasync|msync) resumed>(?P<args>.*)$')
DURATION = re.compile(r'<(?P<dur>\d+\.\d+)>\s*$')
RESULT = re.compile(r'\)\s+=\s+(?P<ret>-?\d+)')
PATH = re.compile(r'^\d+<(?P<path>[^>]*)>')


def classify(path):
  if '/v3/' in path or path.endswith('/v3'):
    return 'FINAL'
  if '/tmp-' in path:
    return 'TEMP'
  return 'OTHER'


def short(path):
  # Drop the random per-iteration directory prefix, keep the meaningful tail.
  parts = path.split('/')
  for marker in ('v3', ):
    if marker in parts:
      return '/'.join(parts[parts.index(marker):])
  return '/'.join(parts[-2:])


def main(log_path):
  pending = {}  # pid -> (name, args-so-far)
  calls = []    # (name, path or '', seconds, ret)
  with open(log_path) as f:
    for raw in f:
      m = LINE.match(raw.rstrip('\n'))
      if not m:
        continue
      pid, rest = m.group('pid'), m.group('rest')
      if rest.endswith('<unfinished ...>'):
        c = CALL.match(rest)
        if c:
          pending[pid] = (c.group('name'), c.group('args'))
        continue
      r = RESUMED.match(rest)
      if r:
        name, head = pending.pop(pid, (r.group('name'), ''))
        args = head.replace('<unfinished ...>', '') + r.group('args')
      else:
        c = CALL.match(rest)
        if not c:
          continue
        name, args = c.group('name'), c.group('args')
      d = DURATION.search(args)
      if not d:
        continue
      ret = RESULT.search(args)
      p = PATH.match(args.strip())
      calls.append((name, p.group('path') if p else '', float(d.group('dur')), int(ret.group('ret')) if ret else 0))

  by_name = defaultdict(list)
  for name, _, secs, _ in calls:
    by_name[name].append(secs)
  print(f'{"syscall":10} {"calls":>6} {"total_s":>9} {"median_ms":>10} {"p90_ms":>9} {"max_ms":>9}')
  for name, secs in sorted(by_name.items()):
    secs_sorted = sorted(secs)
    p90 = secs_sorted[min(len(secs_sorted) - 1, int(0.9 * len(secs_sorted)))]
    print(f'{name:10} {len(secs):6d} {sum(secs):9.3f} {statistics.median(secs) * 1000:10.2f} {p90 * 1000:9.2f} '
          f'{max(secs) * 1000:9.2f}')
  errors = [c for c in calls if c[3] != 0]
  print(f'failed calls: {len(errors)}')

  syncs = [c for c in calls if c[0] in ('fsync', 'fdatasync')]
  by_kind = defaultdict(float)
  for _, path, secs, _ in syncs:
    by_kind[classify(path)] += secs
  print('fsync time by file kind: ' + ', '.join(f'{k}={v:.3f}s' for k, v in sorted(by_kind.items())))
  print()
  print(f'{"ms":>9}  kind   file')
  for name, path, secs, _ in sorted(syncs, key=lambda c: -c[2]):
    print(f'{secs * 1000:9.2f}  {classify(path):5}  {short(path) or "(no path)"}')


if __name__ == '__main__':
  main(sys.argv[1])
