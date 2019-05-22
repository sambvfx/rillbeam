## Direct Runner

To run various experiments using the direct runner:
```bash
source venv/bin/activate
python rillbeam/experiments.py
```

# Flink Runner

To send work to flink, first read [this issue](https://issues.apache.org/jira/browse/BEAM-7379)

Alternately, if homebrew is not your style, try the docker flink services:
```bash
docker pull flink:1.7
cd docker
docker-compose up
```

Then:

```bash
source venv/bin/activate
python -m rillbeam.experiments --runner=PortableRunner --job_endpoint=localhost:8099 --setup_file ./setup.py
```

Note: if using a single node flink cluster, you must increase the number of 
task slots per manager in `conf/flink-conf.yaml` to run these examples:

```yaml
# The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.

taskmanager.numberOfTaskSlots: 8
```
