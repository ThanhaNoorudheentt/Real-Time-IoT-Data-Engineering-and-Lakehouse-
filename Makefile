.PHONY: init-env update-env up down clean ps logs topics sim sim-fast consume test accept stage1 ready-stage4 stage4-up stage4-down stage4-reconcile metrics benchmark iceberg-compact iceberg-expire

init-env:             ## create a private .env with unique local passwords
	python3 scripts/init_env.py

update-env:           ## safely add missing generated settings to an existing .env
	python3 scripts/init_env.py --update

up:            ## start the stack
	docker compose up -d --build

down:          ## stop everything (keeps data volumes)
	docker compose down

clean:         ## stop and delete all data volumes (fresh start)
	docker compose down -v

ps:
	docker compose ps

logs:          ## follow bridge logs
	docker compose logs -f --tail=50 bridge

topics:        ## list Kafka topics
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

sim:           ## live simulator -> MQTT, 5 homes, real time (Ctrl-C to stop)
	python3 simulator/simulator.py --sink mqtt --homes 5 --realtime --duration 0

sim-fast:      ## 10 simulated minutes into a local file, no broker needed
	python3 simulator/simulator.py --sink file --homes 3 --duration 600 --no-sleep

consume:       ## peek at 5 messages on iot.raw
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:9092 --topic iot.raw --from-beginning --max-messages 5

test:          ## unit tests (no Docker needed)
	python3 -m pytest -q tests

accept:        ## Stage 1 acceptance: every published message accounted for in Kafka (stack must be up)
	python3 scripts/stage1_acceptance.py

stage1: test accept   ## everything Stage 1 requires

stage4-up:          ## refresh Gold tables and publish alerts continuously
	docker compose --profile stage4 up -d --build gold alerts

stage4-down:        ## stop the Stage 4 refresh and alert jobs
	docker compose --profile stage4 stop gold alerts

stage4-reconcile:   ## reconcile Gold tables and alerts against Silver
	docker compose exec spark spark-submit /opt/spark-apps/reconcile_gold.py

ready-stage4:         ## validate the project prerequisites for Stage 4
	python3 scripts/stage4_readiness.py

metrics:             ## collect Kafka lag and Silver health metrics
	python3 scripts/collect_metrics.py

benchmark:           ## measure repeatable simulator-to-file throughput at 1/3/5 homes
	python3 scripts/benchmark.py

iceberg-compact:     ## compact Iceberg data files without expiring snapshots
	docker compose exec spark spark-submit /opt/spark-apps/maintenance.py --action rewrite

iceberg-expire:      ## explicitly expire old snapshots (requires SNAPSHOT_EXPIRE_BEFORE)
	test -n "$(SNAPSHOT_EXPIRE_BEFORE)" || (echo "Set SNAPSHOT_EXPIRE_BEFORE to an ISO timestamp in the past"; exit 2)
	docker compose exec spark spark-submit /opt/spark-apps/maintenance.py --action expire --expire-before "$(SNAPSHOT_EXPIRE_BEFORE)" --confirm-expire
