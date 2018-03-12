-module(my_ranch_worker).
% MIT License
-author('Max Lapshin <max@maxidoors.ru>').

-export([start_server/4, stop_server/1, start_link/4, init_server/4]).

-export([start_trivial_server/4, init_trivial_listener/4, init_trivial_server/2]).

-export([handle_call/3, handle_info/2, terminate/2]).
-include("myproto.hrl").


start_server(Port, Name, Handler, Args) ->
  application:start(ranch),
  ranch:start_listener(Name, 10, ranch_tcp, [{port, Port},{backlog,4096},{max_connections,32768}], ?MODULE, [Handler, Args]).

stop_server(Name) ->
  ranch:stop_listener(Name).


start_link(ListenerPid, Socket, _Transport, [Handler, Args]) ->
  proc_lib:start_link(?MODULE, init_server, [ListenerPid, Socket, Handler, Args]).



% Do not use this for anything except tests or debug.
start_trivial_server(Port, Name, Handler, Args) ->
  proc_lib:start(?MODULE, init_trivial_listener, [Port, Name, Handler, Args]).

init_trivial_listener(Port, Name, Handler, Args) ->
  {ok, LSocket} = gen_tcp:listen(Port,[binary, {reuseaddr, true}]),
  register(Name, self()),
  {ok, ListenPort} = inet:port(LSocket),
  put('$my_listener_port', ListenPort),
  proc_lib:init_ack({ok, self()}),
  loop_trivial_listener(LSocket, Handler, Args).

loop_trivial_listener(LSocket, Handler, Args) ->
  {ok, Socket} = gen_tcp:accept(LSocket),
  inet:setopts(Socket, [binary, {active,false}, {packet,raw}]),
  case proc_lib:start(?MODULE, init_trivial_server, [Handler, Args]) of
    {ok, Pid} ->
      gen_tcp:controlling_process(Socket, Pid),
      Pid ! {run,Socket};
    _Else ->
      ok
  end,
  loop_trivial_listener(LSocket, Handler, Args).




-record(server, {
  handler,
  args,
  state,
  database,
  socket,
  my
}).

init_server(ListenerPid, Socket, Handler, Args) ->
  proc_lib:init_ack({ok, self()}),
  ranch:accept_ack(ListenerPid),
  init_server3(Socket, Handler, Args).

init_trivial_server(Handler, Args) ->
  proc_lib:init_ack({ok, self()}),
  receive 
    {run, Socket} -> init_server3(Socket, Handler, Args)
  after
    5000 -> exit(not_activated)
  end.


init_server3(Socket, Handler, Args) ->
  My0 = my_protocol:init([{socket, Socket},{parse_query,true}]),
  {ok, My1} = my_protocol:hello(42, My0),
  case my_protocol:next_packet(My1) of
    {ok, #request{info = #user{} = User}, My2} ->
      try authorize_and_connect(User, Handler, Args) of
        {ok, HandlerState, DB} ->
          {ok, My3} = my_protocol:ok(My2),
          inet:setopts(Socket, [{active,once}]),
          State = #server{handler = Handler, state = HandlerState, socket = Socket, my = My3, database = DB},
          catch gen_server:enter_loop(?MODULE, [], State);
        {error, Reason} ->
          my_protocol:error(1045, Reason, My2);
        {error, Code, Reason} ->
          my_protocol:error(Code, Reason, My2)
      catch
        _C:E ->
          ST = erlang:get_stacktrace(),
          sqlapi:notify(sql_error, [{module,?MODULE},{line,?LINE},{pid,self()},{application,sqlapi},
            {query, <<"login">>}, {error, E}, {stacktrace, iolist_to_binary(io_lib:format("~p",[ST]))}]),
          my_protocol:error(500, <<"Internal server error on auth">>, My2)
      end;
    {error, StartError} ->
      {stop, StartError}
  end.

authorize_and_connect(#user{} = User, Handler, Args) ->
  #user{name = Name, password = Password, server_hash = Hash, database = Database} = User,
  case Handler:authorize(Name, Password, Hash, Args) of
    {ok, HandlerState} when size(Database) > 0 ->
      case Handler:connect_db(Database, HandlerState) of
        {ok, HandlerState1} -> {ok, HandlerState1, Database};
        {error, Code, E} -> {error, Code, E};
        {error, E} -> {error, E}
      end;
    {ok, HandlerState} ->
      {ok, HandlerState, undefined};
    {error, Code, E} ->
      {error, Code, E};
    {error, E} ->
      {error, E}
  end.



handle_call(Call, _From, #server{} = Server) ->
  {stop, {unknown_call,Call}, Server}.


handle_info({tcp, Socket, Bin}, #server{my = My} = Server) ->
  My1 = my_protocol:buffer_bytes(Bin, My),
  inet:setopts(Socket, [{active, once}]),
  try handle_packets(Server#server{my = My1})
  catch
    Class:Error ->
      ST = erlang:get_stacktrace(),
      sqlapi:notify(sql_handler_error, [{module,?MODULE},{line,?LINE},{pid,self()},{application,sqlapi},
        {error,iolist_to_binary(io_lib:format("~p",[Error]))},
        {stacktrace,iolist_to_binary(io_lib:format("~p",[ST]))}]),
      {stop, {Class,Error}, Server}
  end;

handle_info({tcp_closed, _Socket}, #server{handler = Handler, state = HandlerState} = Server) ->
  Handler:terminate(tcp_closed, HandlerState),
  {stop, normal, Server};

handle_info({tcp_error, _, Error}, #server{handler = Handler, state = HandlerState} = Server) ->
  Handler:terminate({tcp_error, Error}, HandlerState),
  {stop, normal, Server}.


terminate(_,_) -> ok.



handle_packets(#server{my = My} = Server) ->
  case my_protocol:decode(My) of
    {ok, Query, My1} ->
      case handle_packet(Query, Server#server{my = My1}) of
        {ok, Server1} ->
          handle_packets(Server1);
        {stop, Reason, Server1} ->
          {stop, Reason, Server1}
      end;
    {more, My1} ->
      {noreply, Server#server{my = My1}}
  end.





handle_packet(#request{command=quit}, #server{handler=H,state=S} = Server) ->
  S1 = H:terminate(quit, S),
  {stop, normal, Server#server{state=S1}};

handle_packet(#request{} = Request, #server{handler=H,state=S,my=My1,database=DB} = Server) ->
  put('$current_request',Request),
  case default_reply(Request, Server) of
    {reply, Reply, Server1} ->
      {ok, My2} = my_protocol:send_or_reply(Reply, My1),
      erase('$current_request'),
      {ok, Server1#server{my=My2}};
    undefined when DB == undefined ->
      Reply = #response{status=?STATUS_ERR, error_code=1046, info = <<"No database selected">>},
      {ok, My2} = my_protocol:send_or_reply(Reply, My1),
      erase('$current_request'),
      {ok, Server#server{my=My2}};
    undefined ->
      {reply, Reply, S1} = sqlapi:execute(Request,H,S),
      {ok, My2} = my_protocol:send_or_reply(Reply, My1),
      erase('$current_request'),
      {ok, Server#server{state=S1,my=My2}}
  end.



% This is a hack for Flask
default_reply(#request{command = 'query', text = <<"SELECT CAST(", _/binary>> = Text}, State) ->
  case re:run(Text, "CAST\\('([^']+)' AS (\\w+)", [{capture,all_but_first,binary}]) of
    {match, [Value, Name]} ->
      Info = {
        [#column{name = Name, type=?TYPE_VARCHAR, length=200}],
        [[Value]]
      },
      {reply, #response{status=?STATUS_OK, info=Info}, State};
    nomatch ->
      {reply, #response{status=?STATUS_ERR, error_code=1065, info = <<"Unsupported CAST">>}, State}
  end;


default_reply(#request{info = #select{params=[#variable{name = <<"version_comment">>}]}}, #server{}=Srv) ->
  Version = version_from_handler(Srv),
  Info = {
    [#column{name = <<"@@version_comment">>, type=?TYPE_VARCHAR, length=20}],
    [[Version]]
  },
  {reply, #response{status=?STATUS_OK, info=Info}, Srv};

default_reply(#request{info = #select{params=[#variable{name = <<"global.max_allowed_packet">>}]}}, State) ->
  Info = {
    [#column{name = <<"@@global.max_allowed_packet">>, type=?TYPE_LONG, length=20}],
    [[4194304]]
  },
  {reply, #response{status=?STATUS_OK, info=Info}, State};

default_reply(#request{info = #select{params=[#variable{name = <<"tx_isolation">>, scope = local}]}}, State) ->
  Info = {
    [#column{name = <<"@@tx_isolation">>, type=?TYPE_VARCHAR}],
    [[<<"REPEATABLE-READ">>]]
  },
  {reply, #response{status=?STATUS_OK, info=Info}, State};


default_reply(#request{info = {use,Database}}, #server{handler=Handler,state=S}=Srv) ->
  case Handler:connect_db(Database, S) of
    {ok, S1} -> 
      {reply, #response {
        status=?STATUS_OK, info = <<"Changed to ", Database/binary>>, status_flags = 2
      }, Srv#server{state=S1,database=Database}};
    {error, Code, E} -> 
      {reply, #response{status=?STATUS_ERR, error_code = Code, info = E}, Srv}
  end;

default_reply(#request{command = init_db, info = Database}, Srv) ->
  default_reply(#request{info={use,Database}}, Srv);

default_reply(#request{info = #select{params = [#function{name = <<"DATABASE">>}]}}, #server{handler=H,state=S}=Srv) ->
  case H:database(S) of
    undefined ->
      {reply, #response{status=?STATUS_OK, info = {
        [#column{name = <<"database()">>, type = ?TYPE_NULL}],
        [[null]]
      }}, Srv};
    Database ->
      {reply, #response{status=?STATUS_OK, info = {
        [#column{name = <<"database()">>, type = ?TYPE_VAR_STRING, length = 20}],
        [[Database]]
      }}, Srv}
  end;


default_reply(#request{info = #select{tables = [#table{name = {<<"information_schema">>,<<"tables">>}}]}}, Server) ->
  default_reply(#request{info = #show{type = tables}}, Server);

default_reply(#request{info = #select{tables = [#table{name = {<<"information_schema">>,_}}]} = Select}, State) ->
  #select{params = Params} = Select,
  ReplyColumns = [#column{name = or_(Alias,Name), type = ?TYPE_VAR_STRING} || #key{name=Name,alias=Alias} <- Params],
  Response = {ReplyColumns, []},
  {reply, #response{status=?STATUS_OK, info = Response}, State};



default_reply(#request{info = #show{type = databases}}, #server{handler=Handler,state=S}=Srv) ->
  Databases = Handler:databases(S),
  ResponseFields = {
    [#column{name = <<"Database">>, type=?TYPE_VAR_STRING, length=20, schema = <<"information_schema">>, table = <<"SCHEMATA">>, org_table = <<"SCHEMATA">>, org_name = <<"SCHEMA_NAME">>}],
    [ [DB] || DB <- Databases]
  },
  Response = #response{status=?STATUS_OK, info = ResponseFields},
  {reply, Response, Srv};


default_reply(#request{info = #show{type = collation}}, State) ->
  ResponseFields = {
    [#column{name = <<"Collation">>, type=?TYPE_VAR_STRING, length=20},
    #column{name = <<"Charset">>, type=?TYPE_VAR_STRING, length=20},
    #column{name = <<"Id">>, type=?TYPE_LONG},
    #column{name = <<"Default">>, type=?TYPE_VAR_STRING, length=20},
    #column{name = <<"Compiled">>, type=?TYPE_VAR_STRING, length=20},
    #column{name = <<"Sortlen">>, type=?TYPE_LONG}
    ],
    [ 
      [<<"utf8_bin">>,<<"utf8">>,83,<<"">>,<<"Yes">>,1]
    ]
  },
  {reply, #response{status=?STATUS_OK, info = ResponseFields}, State};



default_reply(#request{info = #show{type = variables}}, #server{}=Srv) ->
  Version = version_from_handler(Srv),
  Timestamp = integer_to_binary(erlang:system_time(micro_seconds)),

  Variables = [
    {<<"sql_mode">>, <<"NO_ENGINE_SUBSTITUTION">>},
    {<<"auto_increment_increment">>, <<"1">>},
    {<<"character_set_client">>, <<"utf8">>},
    {<<"character_set_connection">>, <<"utf8">>},
    {<<"character_set_database">>, <<"utf8">>},
    {<<"character_set_results">>, <<"utf8">>},
    {<<"character_set_server">>, <<"utf8">>},
    {<<"character_set_system">>, <<"utf8">>},
    {<<"date_format">>, <<"%Y-%m-%d">>},
    {<<"datetime_format">>, <<"%Y-%m-%d %H:%i:%s">>},
    {<<"default_storage_engine">>, <<"MyISAM">>},
    {<<"timestamp">>, Timestamp},
    {<<"version">>, Version}
  ],

  ResponseFields = {

    [#column{name = <<"Variable_name">>, type=?TYPE_VAR_STRING, length=20, schema = <<"information_schema">>, table = <<"SCHEMATA">>, org_table = <<"SCHEMATA">>, org_name = <<"SCHEMA_NAME">>},
    #column{name = <<"Value">>, type=?TYPE_VAR_STRING, length=20, schema = <<"information_schema">>, table = <<"SCHEMATA">>, org_table = <<"SCHEMATA">>, org_name = <<"SCHEMA_NAME">>}],

    [tuple_to_list(V) || V <- Variables]

  },
  {reply, #response{status=?STATUS_OK, info = ResponseFields}, Srv};


default_reply(#request{info = #select{params = [#function{name = <<"DATABASE">>}]}}, State) ->
  ResponseFields = {
    [#column{name = <<"DATABASE()">>, type=?TYPE_VAR_STRING, length=102, flags = 0, decimals = 31}],
    []
  },
  Response = #response{status=?STATUS_OK, info = ResponseFields},
  {reply, Response, State};


default_reply(#request{info = #show{type = tables}}, #server{database=undefined}=Srv) ->
  {reply, #response{status=?STATUS_ERR, error_code=1046, info = <<"No database selected">>}, Srv};

default_reply(#request{info = #show{type = tables}}, #server{handler=Handler,state=S}=Srv) ->
  Tables = Handler:tables(S),
  DB = Handler:database(S),
  ResponseFields = {
    [#column{name = <<"Tables_in_", DB/binary>>, type=?TYPE_VAR_STRING, schema = DB, table = <<"TABLE_NAMES">>, 
      org_table = <<"TABLE_NAMES">>, org_name = <<"TABLE_NAME">>, flags = 256, length = 192, decimals = 0}],
    [ [Table] || Table <- Tables]
  },
  Response = #response{status=?STATUS_OK, info = ResponseFields},
  {reply, Response, Srv};

default_reply(#request{info = #describe{table = #table{name = Table}}}, Server) ->
  default_reply(#request{info = #show{type = fields, from = Table, full = false }}, Server);

default_reply(#request{info = #show{type = fields}}, #server{database=undefined}=Srv) ->
  {reply, #response{status=?STATUS_ERR, error_code=1046, info = <<"No database selected">>}, Srv};

default_reply(#request{info = #show{type = fields, from = Table, full = Full}}, #server{handler=Handler,state=S}=Srv) ->
  Fields = Handler:columns(Table, S),
  Header = [
    #column{name = <<"Field">>, org_name = <<"COLUMN_NAME">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 192, flags = 256},
    #column{name = <<"Type">>, org_name = <<"COLUMN_TYPE">>, type = ?TYPE_BLOB, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 589815, flags = 256},
    #column{name = <<"Null">>, org_name = <<"IS_NULLABLE">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 9, flags = 256},
    #column{name = <<"Key">>, org_name = <<"COLUMN_KEY">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 9, flags = 256},
    #column{name = <<"Default">>, org_name = <<"COLUMN_DEFAULT">>, type = ?TYPE_BLOB, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 589815, flags = 256},
    #column{name = <<"Extra">>, org_name = <<"EXTRA">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 90, flags = 256}
  ] ++ case Full of
    true -> [
      #column{name = <<"Collation">>, org_name = <<"COLLATION_NAME">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 96, flags = 256},
      #column{name = <<"Privileges">>, org_name = <<"PRIVILEGES">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 240, flags = 256},
      #column{name = <<"Comment">>, org_name = <<"COLUMN_COMMENT">>, type = ?TYPE_VAR_STRING, table = <<"COLUMNS">>, org_table = <<"COLUMNS">>, schema = <<"information_schema">>, length = 3072, flags = 256}
    ];
    false -> []
  end,
  Rows = lists:map(fun({Name,Type}) ->
    [atom_to_binary(Name,latin1),
    case Type of string -> <<"varchar(255)">>; boolean -> <<"tinyint(1)">>; _ -> <<"bigint(20)">> end,
    <<"YES">>,
    <<>>,
    undefined,
    <<>>
    ] ++ case Full of
      true -> [case Type of string -> <<"utf8_general_ci">>; _ -> undefined end, <<"select,insert,update,references">>,<<>>];
      false -> []
    end
  end, Fields),
  {reply, #response{status=?STATUS_OK, info = {Header, Rows}}, Srv};  




default_reply(#request{info = #show{type = index}}, #server{database=undefined}=Srv) ->
  {reply, #response{status=?STATUS_ERR, error_code=1046, info = <<"No database selected">>}, Srv};


default_reply(#request{info = #show{type = index}}, #server{}=Srv) ->
  Header = [
    #column{name = <<"Table">>, org_name = <<"Table">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 192, flags = 256},
    #column{name = <<"Non_unique">>, org_name = <<"Non_unique">>, type = ?TYPE_LONGLONG, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 1, flags = 256},
    #column{name = <<"Key_name">>, org_name = <<"Key_name">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 192, flags = 256},
    #column{name = <<"Seq_in_index">>, org_name = <<"Seq_in_index">>, type = ?TYPE_LONGLONG, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 1, flags = 256},
    #column{name = <<"Column_name">>, org_name = <<"Column_name">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 192, flags = 256},
    #column{name = <<"Collation">>, org_name = <<"Collation">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 3, flags = 256},
    #column{name = <<"Cardinality">>, org_name = <<"Cardinality">>, type = ?TYPE_LONGLONG, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 1, flags = 256},
    #column{name = <<"Sub_part">>, org_name = <<"Sub_part">>, type = ?TYPE_LONGLONG, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 1, flags = 256},
    #column{name = <<"Packed">>, org_name = <<"Packed">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 192, flags = 256},
    #column{name = <<"Null">>, org_name = <<"Null">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 9, flags = 256},
    #column{name = <<"Index_type">>, org_name = <<"Index_type">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 48, flags = 256},
    #column{name = <<"Comment">>, org_name = <<"Comment">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 48, flags = 256},
    #column{name = <<"Index_comment">>, org_name = <<"Index_comment">>, type = ?TYPE_VAR_STRING, table = <<"STATISTICS">>, org_table = <<"STATISTICS">>, schema = <<"information_schema">>, length = 3072, flags = 256}
  ],
  % Sorry, no indexes
  Rows = [],
  {reply, #response{status=?STATUS_OK, info = {Header, Rows}}, Srv};  




default_reply(#request{info = #show{type = create_table, from = Table}}, #server{handler=Handler,state=S}=Srv) ->
  Fields = Handler:columns(Table, S),
  CreateTable = iolist_to_binary([
    "CREATE TABLE `", Table, "` (\n",
      tl(lists:flatmap(fun({Name,Type}) ->
        [",", "`", atom_to_binary(Name,latin1), "` ", case Type of string -> "varchar(255)"; integer -> "bigint(20)"; boolean -> "tinyint(1)" end, "\n"]
      end, Fields)),
    ")"
  ]),
  Response = {
    [#column{name = <<"Table">>, type = ?TYPE_VAR_STRING, flags = 256, decimals = 31, length = 192},
    #column{name = <<"Create Table">>, type = ?TYPE_VAR_STRING, flags = 256, decimals = 31, length = 3072}],
    [
    [Table, CreateTable]
    ]
  },
  {reply, #response{status=?STATUS_OK, info = Response}, Srv};


default_reply(#request{command = field_list, info = Table}, #server{handler=Handler,state=S}=Srv) ->
  Fields = Handler:columns(Table, S),
  DB = Handler:database(S),  
  Reply = [#column{schema = DB, table = Table, org_table = Table, name = to_b(Field), org_name = to_b(Field), length = 20,
    type = case Type of string -> ?TYPE_VAR_STRING; integer -> ?TYPE_LONGLONG; boolean -> ?TYPE_TINY end} || {Field,Type} <- Fields],
  {reply, #response{status=?STATUS_OK, info = {Reply}}, Srv};    

default_reply(#request{command = ping}, State) ->
  {reply, #response{status = ?STATUS_OK, id = 1}, State};


% This line can be disabled to allow passing variable sets to underlying module
default_reply(#request{command = 'query', info = #system_set{}}, State) ->
  {reply, #response{status = ?STATUS_OK}, State};


default_reply(#request{info = #select{params=[#value{name = Name, value = undefined}]}}, State) ->
  Info = {
    [#column{name = Name, type=?TYPE_NULL}],
    []
  },
  {reply, #response{status=?STATUS_OK, info=Info}, State};


default_reply(#request{info = #select{params=[#value{name = Name, value = Value}]}}, State) ->
  Info = {
    [#column{name = Name, type=?TYPE_VARCHAR, length=200}],
    [[Value]]
  },
  {reply, #response{status=?STATUS_OK, info=Info}, State};


default_reply(#request{info = 'begin'}, State) ->
  {reply, #response{status = ?STATUS_OK}, State};

default_reply(#request{info = commit}, State) ->
  {reply, #response{status = ?STATUS_OK}, State};

default_reply(#request{info = rollback}, State) ->
  {reply, #response{status = ?STATUS_OK}, State};

default_reply(#request{info = #select{tables = [#table{name = {<<"information_schema">>,_}}]}}, State) ->
  {reply, #response{status = ?STATUS_OK}, State};

default_reply(_, _State) ->
  undefined.



version_from_handler(#server{handler=H,state=S}) ->
  try H:version(S)
  catch
    _:_ -> <<"5.6.0">>
  end.



to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1);
to_b(Bin) when is_binary(Bin) -> Bin.


or_(undefined,A) -> A;
or_(A,_) -> A.


