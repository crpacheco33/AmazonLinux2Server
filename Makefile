# Application

app.serve.local:
	TEST_DATABASE_URI="mongodb+srv://visibly:Bxr2PWLtacpPq3v.tzdJ@cluster0.n739w.mongodb.net" PYTHONPATH=server/ python -m server.main main.py

app.serve.test:
	TEST_DATABASE_URI="mongodb+srv://visibly:Jv7TJsRcMCdWBm2@cluster0.isb04.mongodb.net" PYTHONPATH=server/ python -m server.main main.py

# Builds

ecr.pull.local:
	aws --profile barcelona-iam-pm-user ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 369963911650.dkr.ecr.us-west-1.amazonaws.com
	docker pull 369963911650.dkr.ecr.us-west-1.amazonaws.com/local-ecr-visibly-server:brand-analytics

ecr.push.barcelona:
	# mv .python-version python-version
	aws --profile barcelona-iam-pm-user ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 369963911650.dkr.ecr.us-west-1.amazonaws.com
	docker build -t ecr-visibly-server .
	docker tag ecr-visibly-server:latest 369963911650.dkr.ecr.us-west-1.amazonaws.com/ecr-visibly-server:latest
	docker push 369963911650.dkr.ecr.us-west-1.amazonaws.com/ecr-visibly-server:latest
	# mv python-version .python-version

ecr.push.local:
	# mv .python-version python-version
	aws --profile barcelona-iam-pm-user ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 369963911650.dkr.ecr.us-west-1.amazonaws.com
	docker build -t local-ecr-visibly-server .
	docker tag local-ecr-visibly-server:latest 369963911650.dkr.ecr.us-west-1.amazonaws.com/local-ecr-visibly-server:latest
	docker push 369963911650.dkr.ecr.us-west-1.amazonaws.com/local-ecr-visibly-server:latest
	# mv python-version .python-version

ecr.push.local.tag:
	# mv .python-version python-version
	aws --profile barcelona-iam-pm-user ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 369963911650.dkr.ecr.us-west-1.amazonaws.com
	docker build -t local-ecr-visibly-server:${tag} .
	docker tag local-ecr-visibly-server:${tag} 369963911650.dkr.ecr.us-west-1.amazonaws.com/local-ecr-visibly-server:${tag}
	docker push 369963911650.dkr.ecr.us-west-1.amazonaws.com/local-ecr-visibly-server:${tag}
	# mv python-version .python-version

tag.barcelona:
	git tag ecr-barcelona-0.1.`git rev-list HEAD | wc -l`.`git rev-parse --short HEAD`; git push --tags

tag.local:
	git tag ecr-local-0.1.`git rev-list HEAD | wc -l`.`git rev-parse --short HEAD`; git push --tags

tag.podgorica:
	git tag ecr-podgorica-0.1.`git rev-list HEAD | wc -l`.`git rev-parse --short HEAD`; git push --tags

tag.sandbox:
	git tag ecr-sandbox-0.1.`git rev-list HEAD | wc -l`.`git rev-parse --short HEAD`; git push --tags

# Elasticsearch

es:
	PYTHONPATH="$$PYTHONPATH:.server/" python server/services/data/es.py

es.data:
	PYTHONPATH=":$$PYTHONPATH:.server/" python server/services/data_service.py

# Requirements

requirements:
	poetry export -f requirements.txt --output ./server.requirements.txt --without-hashes
	poetry export -f requirements.txt --output ./test.requirements.txt --without-hashes --dev

# Tests

test:
	TEST_DATABASE_URI="mongodb+srv://visibly:Jv7TJsRcMCdWBm2@cluster0.isb04.mongodb.net" NO_CORS=1 PYTHONPATH="$$PWD/server:$$PWD/tests:$$PYTHONPATH" pytest -o log_cli=true -m "not cli"

testcli:
	TEST_DATABASE_URI="mongodb+srv://visibly:Jv7TJsRcMCdWBm2@cluster0.isb04.mongodb.net" NO_CORS=1 PYTHONPATH="$$PWD/server:$$PWD/tests:$$PYTHONPATH" pytest --capture=no --log-cli-level=ERROR -o log_cli=false -m cli

testclimark:
	TEST_DATABASE_URI="mongodb+srv://visibly:Jv7TJsRcMCdWBm2@cluster0.isb04.mongodb.net" NO_CORS=1 PYTHONPATH="$$PWD/server:$$PWD/tests:$$PYTHONPATH" pytest --capture=no --log-cli-level=ERROR -o log_cli=true -m "${m}"

testmark:
	TEST_DATABASE_URI="mongodb+srv://visibly:Jv7TJsRcMCdWBm2@cluster0.isb04.mongodb.net" NO_CORS=1 PYTHONPATH="$$PWD/server:$$PWD/tests:$$PYTHONPATH" pytest -o log_cli=true -m "${m}"
