.PHONY: up down submit test

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

submit:
	docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		--executor-memory 2G \
		--executor-cores 2 \
		--total-executor-cores 4 \
		/opt/spark-apps/indegree_job.py \
		--input /data/raw/web-BerkStan.txt \
		--output /data/output/top50_indegree

test:
	pytest tests/