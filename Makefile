.PHONY: up down clean ps logs topics sim sim-fast consume test accept stage1 stage4-up stage4-down stage4-reconcile

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
