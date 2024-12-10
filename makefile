.PHONY: test compile-tests docs no-warn

MocPath = $(shell mops toolchain bin moc)

test:
	mops test

check:
	find src -type f -name '*.mo' -print0 | xargs -0 $(MocPath) -r $(shell mops sources) -Werror -wasi-system-api

# docs: 
# 	$(MocPath)/mo-doc
# 	$(MocPath)/mo-doc --format plain

bench:
	mops bench

canister-tests:
	-dfx start --background --emulator
	zx -i ./z-scripts/canister-tests.mjs