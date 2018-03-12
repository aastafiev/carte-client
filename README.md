<b>Description:</b><br>
The carte-client trying to run a job from Pentaho Carte Cluster on Carte Master via Carte API (https://help.pentaho.com/Documentation/8.0/Developer_Center/REST_API).<br>
If job run status not equal to 'Running' it trying to restart it. Default number of attemts - 5.

Used for scheduling Pentaho Data Integration jobs on Carte Cluster

<b>Unix:</b><br>
- export PYTHONPATH="$PYTHONPATH:/path_to_myapp/myapp/myapp/"

<b>Windows:</b><br>
- set PYTHONPATH=%PYTHONPATH%;D:\LocalData\au00681\PycharmProjects\odometer_service

<b>Update requirements:</b><br>
- add the package name into `requirements.in`
- run `pip-compile --upgrade`
- run `pip-compile --output-file requirements.txt requirements.in`
for creating `requirements.txt`
- run `pip-sync`

<b>Install requirements:</b><br>
- pip install -r requirements.txt