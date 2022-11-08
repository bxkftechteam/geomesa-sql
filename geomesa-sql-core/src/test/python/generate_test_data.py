#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate and write test data to src/test/resources directory"""

import os
import csv
import uuid

columns = [
    'I:int',
    'L:long',
    'F:double',
    'D:double',
    'B:boolean',
    'S:string',
    'U:string',
    'TS:timestamp'
]

rows = []
for k in range(0, 1000):
    hour = k // 3600
    minute = (k - hour * 3600) // 60
    second = (k - hour * 3600) % 60
    ts_str = "2022-05-01 %02d:%02d:%02d" % (hour, minute, second)
    uuid_str = '00000000-0000-0000-0000-00000000%04d' % k
    row = [
        k,                      # I:int
        10000000000 + k,        # L:long
        "%.1f" % (k * 0.1),     # F:float
        "%.2f" % (k * 0.01),    # D:double
        ("true" if k % 2 == 0 else "false"), # B:boolean
        "str%04d" % k,          # S:string
        str(uuid.UUID(uuid_str)), # U:string
        ts_str,                   # TS:timestamp
    ]
    rows.append(row)

test_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
resources_path = os.path.join(test_path, "resources")
with open(os.path.join(resources_path, 'test_data.csv'), 'w+') as f:
    writer = csv.writer(f)
    writer.writerow(['__FID__:string'] + columns)
    for row in rows:
        writer.writerow(['#%d' % (row[0])] + row)

with open(os.path.join(resources_path, 'test_data_geomesa.csv'), 'w+') as f:
    writer = csv.writer(f)
    writer.writerow([c.split(':')[0] for c in columns])
    for row in rows:
        row[-1] = row[-1].replace(' ', 'T') + 'Z'
        writer.writerow(row)
