---
description: Guidelines for building extremely modular scraping and enrichment pipelines
---

# Modularity
- break down every task into one purpose functions
- functions should be as pure as possible
- aim for thin orchestration layers that connect the pure functions together
- split functions into files based on their purpose
- provide a one-line """ description at the top of each func

# Pipeline flow
- every step should be idempotent
- workflows shouldn't pass state but pull "not worked data" from source
- steps can pass minimal data
- each workflow/task should be able to be re-run from a given checkpoint

# Logs
- log at the start and end of each step
- log any errors with as much context as possible
- log any retries with the reason for the retry

# Terminal
- activate venv only once at the start of the terminal session
- dont activate for each command
- use `uv` for installing packages