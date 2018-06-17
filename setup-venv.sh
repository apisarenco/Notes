#!/bin/bash
if [ -d .venv ] ; then
	echo 1 > /dev/null
else
	sudo apt-get -y install python3 python3-pip python3-dev virtualenv
	virtualenv -p python3 .venv
fi

.venv/bin/pip install -r requirements.txt

