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

Activate the working environment and if required bring it up to date

``` bash
source ./venv/bin/activate
python -m pip install --upgrade pip
```

Then, finally install the requirements i.e. `pyzmq` and `pycodestyle`. The latter package is optional and is only required for linting hints in IDE's like the `vscode` editor.

``` bash
pip install --requirement ./requirements.txt
pip freeze
pycodestyle==2.4.0
pyzmq==17.0.0
tornado==5.0.2
```

Furthermore, `tornado` asynchronous networking library is used for some code examples.

If you prefer to observe the communication on wire, say port `tcp://*:5555`, than `ngrep` and `tcpdump` can be used for this

``` bash
sudo ngrep -d lo '' 'port 5555'
```

To capture the communication into the `pcap` file, please use

``` bash
sudo tcpdump -i lo -n "port 5555" -w zmq-listening.pcap
```
