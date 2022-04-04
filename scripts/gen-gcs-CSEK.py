#!/usr/bin/env python3

import base64
import os

print(base64.b64encode(os.urandom(32)).decode())
