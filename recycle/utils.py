import logging
import re
import subprocess
from time import time
import uuid

from faker import Faker

default_faker = Faker()
hide_passwords = re.compile(r":([^:@]{2})[^:@]+([^:@]{2})@")


def fake_like_gen(value, faker=default_faker):
    if isinstance(value, dict):
        ls = {k: fake_like_gen(v, faker=faker) for k, v in value.items()}
        return lambda: {k: v() for k, v in ls.items()}

    if isinstance(value, list):
        ls = [fake_like_gen(v, faker=faker) for v in value]
        return lambda: [v() for v in ls]

    if value == "phone":
        return faker.phone_number

    if value == "email":
        return lambda: f"{uuid.uuid4().hex}-{faker.email()}"

    if value == "firstname":
        return faker.first_name

    if value == "lastname":
        return faker.last_name

    if value == "str":
        return faker.word

    if value is None:
        return lambda: None

    raise NotImplementedError


def shell(cmd, log_formatter=None, json=False):
    if not isinstance(cmd, list):
        cmd = cmd.split()

    command_name = str(cmd[0])
    logging.info(re.sub(hide_passwords, ":\\1****\\2@", " ".join(cmd)))

    start = time()
    logging.info(f"shell starting {command_name}")

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=512
    )

    ret = []

    logger = logging.getLogger(command_name)
    if json:
        output, error = process.communicate()
        ret = output.decode("ascii").replace("\n", "").replace("\t", "")
        logger.info(log_formatter(ret) if log_formatter is not None else ret)
    else:
        for line in process.stdout:
            out = line.decode().rstrip("\n")
            ret.append(out)
            logger.info(log_formatter(out) if log_formatter is not None else out)

    assert process.wait() == 0, f"shell {command_name} failed"
    logging.info(f"shell terminated {command_name} {round((time() - start) * 1000)}ms")

    return ret
