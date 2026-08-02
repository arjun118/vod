include .env
export

MIGRATE=docker compose run --rm migrate \
	-path /migrations \
	-database "$(DATABASE_URL)"

migrate-create:
	migrate create -seq -ext=.sql -dir=./migrations $(NAME)

migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

migrate-version:
	$(MIGRATE) version

migrate-force:
	$(MIGRATE) force $(VERSION)


show:
	@echo $(DATABASE_URL)
