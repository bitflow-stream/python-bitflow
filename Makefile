init:
	pip install -r requirements.txt

test:
	cd tests ; py.test .

jenkins-test:
	cd tests ; py.test --junitxml test-report.xml --cov-report xml:coverage-report.xml --cov=bitflow .

install:
	./setup.py install

.PHONY: init test