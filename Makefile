MODULES := \
	components/queue \
	contrib/queue/bridge \
	contrib/queue/kafka \
	contrib/queue/rabbitmq \
	examples/queue-bridge

.PHONY: test

test:
	@set -e; \
	for m in $(MODULES); do \
		echo "==> go test ./... in $$m"; \
		( cd $$m && go test ./... ); \
	done
