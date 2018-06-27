# 0MQ examples in python

## Environment

The target python version for these examples is `3.6`. Thus please install

``` bash
sudo apt install python3.6-venv
```

which also should install `python3.6` in that case. Then, create a virtual environment by running

``` bash
python3.6 -m venv './venv'
```

Activate the environment and if required bring it up to date

``` bash
source ./venv/bin/activate
python -m pip install --upgrade pip
```

Then, finally install the requirements i.e. `pyzmq` and `pycodestyle`. The latter one is optional and only for linting hints for the `vscode` editor.

``` bash
pip install --requirement ./requirements.txt
pip freeze
pycodestyle==2.4.0
pyzmq==17.0.0
```
