#!/usr/bin/env escript
%%
%%! -pa ebin

-mode(compile).


main([]) ->
  {ok,_} = application:ensure_all_started(sqlapi),
  sqlapi:load_config(#{port => 4407, listener_name => test_sql, handler => sql_ets_api, trivial => true}),
  io:format("listening on port 4407. Connect as:\n"
    "  mysql -h 127.0.0.1 -P 4407 -u login -ppass ets\n"
    "To create ets table use following syntax:\n"
    "  SELECT create_ram_table('sessions','session_id:string','user_id:integer','last_seen_at:integer');\n"
    "Ensure that it is created:\n"
    "  show tables;"
    "\n"
    "Now run some queries:\n"
    "  insert into sessions (user_id,session_id,last_seen_at) values (1,'u1',1540123456);\n"
    "  select * from sessions;\n"
    "  update sessions set last_seen_at = 14123123123 where user_id=1;\n"
    "  select * from sessions;\n"
    "  delete from sessions where user_id=1;\n"
    "  select * from sessions;\n"
    "Now try to drop it:\n"
    "  SELECT drop_ram_table('sessions');\n"
    "Check that there is no 'sessions' table anymore:\n"
    "show tables;\n"
  ),
  receive _ -> ok end.

