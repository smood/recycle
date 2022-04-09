import base64
from contextlib import contextmanager
import os
import tempfile

from datacycle.utils import shell

CSEK_ENV = "GOOGLE_STORAGE_CSEK"
SA_ENV = "GOOGLE_APPLICATION_CREDENTIALS"


@contextmanager
def service_account(bytes_content, decode64=False):
    fd, tmp_fpath = tempfile.mkstemp()
    os.close(fd)

    try:
        if SA_ENV in os.environ:
            with open(tmp_fpath, "wb") as f:
                if decode64:
                    bytes_content = base64.b64decode(bytes_content)
                else:
                    bytes_content = bytes_content.encode()

                f.write(bytes_content)

            yield tmp_fpath
        else:
            yield None
    finally:
        os.remove(tmp_fpath)


def push(uri, folder):
    with service_account(os.environ.get(SA_ENV)) as sa:

        cmd = "gsutil -m"

        if CSEK_ENV in os.environ:
            cmd += f" -o GSUtil:encryption_key={os.environ[CSEK_ENV]}"

        if sa is not None:
            cmd += f" -o Credentials:gs_service_key_file={sa}"

        cmd += f" cp -r {folder} {uri}"
        shell(cmd)


def pull(uri, folder):
    with service_account(os.environ.get(SA_ENV)) as sa:

        cmd = "gsutil -m"

        if CSEK_ENV in os.environ:
            cmd += f" -o GSUtil:decryption_key1={os.environ[CSEK_ENV]}"

        if sa is not None:
            cmd += f" -o Credentials:gs_service_key_file={sa}"

        cmd += f" cp -r {uri} {folder}"
        shell(cmd)


def list(uri):
    with service_account(os.environ.get(SA_ENV)) as sa:

        cmd = "gsutil"

        if sa is not None:
            cmd += f" -o Credentials:gs_service_key_file={sa}"

        cmd += f" ls -r {uri}"
        return [f for f in shell(cmd) if len(f) > 0]
