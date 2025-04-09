docker-compose -f ../test/docker/docker-compose.yaml down
docker volume rm docker_me-jaeger-data
docker volume rm docker_me-kafka-data
docker volume rm docker_me-zookeeper-data
docker volume rm docker_me-redis-node-1-data
docker volume rm docker_me-redis-node-2-data
docker volume rm docker_me-redis-node-3-data