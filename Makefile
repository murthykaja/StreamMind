.PHONY: up down clean report logs

up:
	docker compose up --build

down:
	docker compose down

clean:
	docker compose down -v

report:
	docker compose exec -T api python -m app.report | cat

logs:
	docker compose logs -f detector agent
