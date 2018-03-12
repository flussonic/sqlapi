all:
	./rebar compile

clean:
	./rebar clean

test:
	mkdir -p logs
	ct_run -noshell -pa ebin -logdir logs/


.PHONY: test