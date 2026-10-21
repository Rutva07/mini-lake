.PHONY: install install-all test demo clean-data

install:
	python3 -m pip install -e '.[dev]'

install-all:
	python3 -m pip install -e '.[all,dev]'

test:
	python3 -m pytest -q

demo:
	python3 -m minilake generate-demo --output data/raw/events.csv --rows 50000
	python3 -m minilake run --input data/raw/events.csv --output data/lake
	python3 -m minilake query --dataset data/lake --event-type purchase --limit 5
	python3 -m minilake benchmark --dataset data/lake --event-type purchase

clean-data:
	python3 -m minilake clean --path data/lake

