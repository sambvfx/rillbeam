To run the docker flink services:
```bash
docker pull flink:1.7
cd docker
docker-compose up
```

To run various experiments:
```bash
sourve venv/bin/activate
python rillbeam/experiments.py
```