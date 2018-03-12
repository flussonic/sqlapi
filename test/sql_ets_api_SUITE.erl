-module(sql_ets_api_SUITE).

-compile(export_all).

all() ->
  [{group, ets}].


groups() ->
  [{ets, [], [
    full_cycle
  ]}].



init_per_suite(Config) ->
  application:ensure_started(sqlapi),
  sqlapi:load_config(#{port => 4406, listener_name => test_sql, handler => sql_ets_api, trivial => true}),
  Config.

end_per_suite(Config) -> 
  sqlapi:load_config(#{handler => sql_ets_api, listener_name => test_sql, trivial => true}),
  Config.


full_cycle(_) ->
  {ok, Conn} = nanomysql:connect("mysql://login:pass@127.0.0.1:4406/ets?login=init_db"),
  {ok, _} = nanomysql:execute("SELECT create_ram_table('sessions','session_id:string','user_id:integer','last_seen_at:integer')", Conn),
  {ok ,_} = nanomysql:execute("insert into sessions (user_id,session_id,last_seen_at) values (1,'u1',1540123456)", Conn),
  [#{user_id := 1, session_id := <<"u1">>, last_seen_at := 1540123456}] = nanomysql:select("select * from sessions", Conn),

  {ok, _} = nanomysql:execute("update sessions set last_seen_at = 14123123123 where user_id=1", Conn),
  [#{user_id := 1, last_seen_at := 14123123123}] = nanomysql:select("select * from sessions", Conn),

  nanomysql:execute("delete from sessions where user_id=1", Conn),
  [] = nanomysql:select("select * from sessions", Conn),
  {ok, _} = nanomysql:execute("SELECT drop_ram_table('sessions')", Conn),
  {error, {1146,_}} = nanomysql:execute("select * from sessions", Conn),
  ok.
