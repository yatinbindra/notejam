#!/bin/sh
python2 manage.py syncdb --noinput &&python2 manage.py migrate && python2 manage.py runserver 0.0.0.0:8000