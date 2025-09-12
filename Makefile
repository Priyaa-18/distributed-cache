test-unit:
	python3 -m pytest tests/test_consistent_hash.py -v

test:
	python3 -m pytest tests/ -v

install:
	pip install -r requirements.txt

run-demo:
	python3 src/demo.py