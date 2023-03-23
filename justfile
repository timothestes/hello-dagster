# run the main script
run:
    python3 -m hello-dagster
dagit:
    dagster dev
install-deps:
    pip3 install -e ".[dev]"
test:
    pytest