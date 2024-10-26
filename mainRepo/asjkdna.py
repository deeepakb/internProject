from __future__ import print_function
from distutils.spawn import find_executable
from os.path import dirname, join as joinpath, realpath
from subprocess import PIPE, Popen
from sys import stderr

from pytest import mark

current_dir = dirname(realpath(__file__))
shells = list(filter(find_executable, ("sh", "bash", "ksh", "dash", "zsh")))


@mark.parametrize("shell", shells)
def test_ecfunc(shell):
    if not find_executable(shell):
        return

    cmd = [shell, joinpath(current_dir, "ecfunc_test.sh")]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if isinstance(out, (bytes, bytearray)):
        out = out.decode("utf-8")
    if isinstance(err, (bytes, bytearray)):
        err = err.decode("utf-8")
    if err:
        print(err, file=stderr)
    assert not err
    assert out == "SUCCESS\n"
    assert proc.returncode == 0
