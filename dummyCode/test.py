# Copyright(c) Amazon.com, Inc. or its affiliates, 2020. All rights reserved.
# Install the downloaded external dependencies for python3. All installed dependencies
# are listed in installed-requirements, matching the requirements file. If this file
# already exists and all dependencies are matched, then skip the installation.
import logging
import optparse
import os
import shutil
import subprocess
import sys
import urllib.request
LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
DEPS_DIR = os.path.join(os.path.dirname(__file__), "deps")
ENV_DIR = os.path.join(os.path.dirname(__file__), "env")
REQS_PATH = os.path.join(DEPS_DIR, "requirements.txt")
# Copy all installed dependencies from requirements onto a new file.
INSTALLED_REQS_PATH = os.path.join(DEPS_DIR, "installed-requirements.txt")
# Running pip in a subprocess is more reliable than using the pip module directly from within this
# python script. (https://pip.pypa.io/en/stable/user_guide/?highlight=_internal#using-pip-from-your-program)asdas
def exec_pip3_install(args, **popen_kwargs):
  '''Executes "pip3 install" with the provided command line arguments and default install
     options.
  '''
  # we explicityl allow binary packages here, so previously downloaded wheels are used.
  base_cmd = [os.path.join(ENV_DIR, "bin", "python3"), os.path.join(ENV_DIR, "bin", "pip3"),
              "install", "--no-index", "--find-links",
      "file://{}/".format(urllib.request.pathname2url(os.path.abspath(DEPS_DIR)))]
  exec_cmd(base_cmd + args, **popen_kwargs)
def exec_cmd(args, **kwargs):
  '''Runs a command and waits for it to finish. An exception is raised if the command is
     returns a non-zero status.
     'args' and 'kwargs' under the same format as subprocess.Popen().
  '''
  process = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
      **kwargs)
  if process.returncode != 0:
    raise Exception(
      "Command returned non-zero status\nCommand: {}\nOutput: {}"
      .format(args, process.stdout.decode())
    )
def install_deps():
  if reqs_are_installed(REQS_PATH, INSTALLED_REQS_PATH):
    LOG.debug("Skipping deps - found installed requirements")
    return True
  else:
    try:
      LOG.info("Installing packages into python3")
      # override system-default packages present after setting up virtual env.
      # setuptools-scm in newer versions does not play well with dateutil.
      exec_pip3_install(["setuptools == 50.3.0", "setuptools-scm == 4.1.2"])
      # TODO(rpsilva): Remove thriftpy2 patch once a new dist version is supported.
      exec_pip3_install(["-r", REQS_PATH, "thriftpy2==0.4.12-p1"])
      mark_reqs_installed(REQS_PATH, INSTALLED_REQS_PATH)
    except Exception as e:
      LOG.error("Error installing packages: {}".format(e))
      return False
  LOG.info("Python3 setup complete")
  return True
def mark_reqs_installed(reqs_path, installed_reqs_path):
  '''Mark that the requirements from the given file are installed by copying it into the
  dependency directory.'''
  shutil.copyfile(reqs_path, installed_reqs_path)
def reqs_are_installed(reqs_path, installed_reqs_path):
  if not os.path.exists(installed_reqs_path):
    return False
  installed_reqs_file = open(installed_reqs_path)
  try:
    reqs_file = open(reqs_path)
    try:
      if reqs_file.read() == installed_reqs_file.read():
        return True
      else:
        LOG.debug("Python3 requirements upgrade needed")
        return False
    finally:
      reqs_file.close()
  finally:
    installed_reqs_file.close()
if __name__ == "__main__":
  parser = optparse.OptionParser()
  parser.add_option("-l", "--log-level", default="INFO",
      choices=("DEBUG", "INFO", "ERROR"))
  options, args = parser.parse_args()
  logging.basicConfig(level=getattr(logging, options.log_level))
  if not install_deps():
    sys.exit(1)
