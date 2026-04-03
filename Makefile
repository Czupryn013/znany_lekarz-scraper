.PHONY: web cli

web:
	cd src && python -m uvicorn web_app.app:app --host 0.0.0.0 --port 8000 --reload

cli:
	@echo "Usage: make cli CMD=\"<command> [args]\""
	@echo "Example: make cli CMD=\"discover --help\""
	cd src && python zl_scraper/cli.py $(CMD)
