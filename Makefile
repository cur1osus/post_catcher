.PHONY: build
build:
	uv run -m bot


.PHONY: sync_models
sync_models:
	cp ../post_catcher/bot/db/models.py ../post_manager/bot/db/models.py
