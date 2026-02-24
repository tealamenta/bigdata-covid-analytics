.PHONY: help up down logs clean test fix-permissions init

help:
	@echo "BIG DATA COVID ANALYTICS"
	@echo ""
	@echo "Commands:"
	@echo "  make init            - Initialize project (permissions + setup)"
	@echo "  make fix-permissions - Fix all file permissions"
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make logs            - View logs"
	@echo "  make clean           - Clean temporary files"
	@echo "  make test            - Run tests"

up:
	@echo " Starting services..."
	@export AIRFLOW_UID=$$(id -u) && docker-compose up -d
	@echo " Services started!"
	@echo "Airflow: http://localhost:8080 (admin/admin)"
	@echo "Kibana: http://localhost:5601"
	@echo "Spark: http://localhost:8081"

down:
	@docker-compose down

logs:
	@docker-compose logs -f

clean:
	@find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@echo " Cleaned!"

test:
	@echo "Running tests..."
	@pytest tests/ -v

fix-permissions:
	@echo " Fixing permissions..."
	@bash scripts/setup/fix_permissions.sh

init: fix-permissions
	@echo " Initializing project..."
	@echo " Project ready!"
