#! /usr/bin/env bash

env_name=".venv"

pip3 install --upgrade pip
pip3 install virtualenv==20.25.1

virtualenv -p $(which python3) $env_name