.PHONY: run test demo demo-dispatch reset compose-up compose-down

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000

test:
	python -m pytest

demo:
	python demo/run_demo.py --base-url http://127.0.0.1:8000

demo-dispatch:
	python demo/run_dispatch_demo.py --base-url http://127.0.0.1:8000

reset:
	curl -s -X POST http://127.0.0.1:8000/admin/reset

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
